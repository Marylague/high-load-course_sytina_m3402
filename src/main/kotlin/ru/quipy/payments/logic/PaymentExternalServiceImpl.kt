package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    private val rateWindow = Duration.ofSeconds(1)
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = rateWindow
    )
    private val ongoingWindow = OngoingWindow(
        maxWinSize = properties.parallelRequests
    )

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val pending = AtomicInteger()
    private val waiting = AtomicInteger()

    private val outgoingCounter = meterRegistry.counter("payment.outgoing", "account", accountName)
    private val rejectedCounter = meterRegistry.counter("payment.rejected", "account", accountName)
    private val completedSuccess = meterRegistry.counter("payment.completed", "account", accountName, "result", "success")
    private val completedFail = meterRegistry.counter("payment.completed", "account", accountName, "result", "fail")

    init {
        meterRegistry.gauge("payment.pending", listOf(io.micrometer.core.instrument.Tag.of("account", accountName)), pending) { it.get().toDouble() }
        meterRegistry.gauge("payment.waiting", listOf(io.micrometer.core.instrument.Tag.of("account", accountName)), waiting) { it.get().toDouble() }
    }


    override fun canMeetDeadline(deadline: Long, queuedAhead: Int): Boolean {
        val avgMillis = requestAverageProcessingTime.toMillis()
        val rateWait = (waiting.get() + queuedAhead).toLong() * rateWindow.toMillis() / rateLimitPerSec
        val slotWait = (pending.get() + queuedAhead).toLong() / parallelRequests * avgMillis
        val expectedFinish = now() + maxOf(rateWait, slotWait) + avgMillis
        return expectedFinish <= deadline
    }

    private fun awaitRatePermit(deadline: Long): Boolean {
        waiting.incrementAndGet()
        try {
            while (!rateLimiter.tick()) {
                if (now() + requestAverageProcessingTime.toMillis() > deadline) return false
                Thread.sleep(10)
            }
            return true
        } finally {
            waiting.decrementAndGet()
        }
    }

    private fun reject(paymentId: UUID, reason: String, paymentStartedAt: Long) {
        rejectedCounter.increment()
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (!canMeetDeadline(deadline)) {
            reject(paymentId, "Cannot meet deadline", paymentStartedAt)
            return
        }
        pending.incrementAndGet()
        try {
            ongoingWindow.acquire()
        } catch (e: Exception) {
            pending.decrementAndGet()
            throw e
        }

        if (now() + requestAverageProcessingTime.toMillis() > deadline) {
            ongoingWindow.release()
            pending.decrementAndGet()
            reject(paymentId, "Deadline exceeded while waiting", paymentStartedAt)
            return
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var attempt = 0
            while (true) {
                if (!awaitRatePermit(deadline)) {
                    rejectedCounter.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded while waiting for rate limiter")
                    }
                    break
                }

                outgoingCounter.increment()

                val body = client.newCall(request).execute().use { response ->
                    try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                }

                val rateLimited = !body.result && body.message?.contains("Rate limit", ignoreCase = true) == true
                if (rateLimited && ++attempt < 3 && now() + requestAverageProcessingTime.toMillis() < deadline) {
                    Thread.sleep(100)
                    continue
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                (if (body.result) completedSuccess else completedFail).increment()

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                break
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    completedFail.increment()
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    completedFail.increment()
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
        finally {
            ongoingWindow.release()
            pending.decrementAndGet()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()