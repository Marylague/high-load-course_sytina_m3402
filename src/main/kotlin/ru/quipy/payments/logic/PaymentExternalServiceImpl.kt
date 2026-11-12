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
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit


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

        private const val MAX_ATTEMPTS = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val pending = AtomicInteger()
    private val waiting = AtomicInteger()

    // на входе ведро токенов, за секунду наливаем rateLimitPerSec токенов, сам бакет в 2 раза больше
    // из-за него копим неиспользованные разрешения и отдаем на всплеске, неблокирующий берем
    private val tokenIntervalMillis = (rateWindow.toMillis() / rateLimitPerSec.coerceAtLeast(1)).coerceAtLeast(1)
    private val tokenBucket = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        bucketMaxCapacity = rateLimitPerSec * 2, // берем *2 для запаса на одну пустую секунду
        // получается, что то, что не потратили останется, а след долив поднимет до двух норм и
        // это все мы отдадим на всплеск
        window = 1,
        timeUnit = TimeUnit.SECONDS,
    )

    private val outgoingCounter = meterRegistry.counter("payment.outgoing", "account", accountName)
    private val rejectedCounter = meterRegistry.counter("payment.rejected", "account", accountName)
    private val completedSuccess = meterRegistry.counter("payment.completed", "account", accountName, "result", "success")
    private val completedFail = meterRegistry.counter("payment.completed", "account", accountName, "result", "fail")

    init {
        meterRegistry.gauge("payment.pending", listOf(io.micrometer.core.instrument.Tag.of("account", accountName)), pending) { it.get().toDouble() }
        meterRegistry.gauge("payment.waiting", listOf(io.micrometer.core.instrument.Tag.of("account", accountName)), waiting) { it.get().toDouble() }
    }

    // если не успеем до дд - 429 и retry-after
    // если успели, но нет токена - 429 и повтор через интервал одного токена
    // в ином случае пускаем, все ок
    override fun tryAdmit(deadline: Long, queuedAhead: Int): Admission {
        val now = now()
        val finishAt = expectedFinishAt(queuedAhead, now)
        if (finishAt > deadline) {
            val retryAt = (finishAt - (deadline - now)).coerceAtLeast(now + tokenIntervalMillis)
            return Admission(accepted = false, retryAfterTimestamp = retryAt)
        }
        if (!tokenBucket.tick()) {
            return Admission(accepted = false, retryAfterTimestamp = now + tokenIntervalMillis)
        }
        return Admission(accepted = true)
    }

    private fun isTransientMessage(message: String?): Boolean =
        message != null && (message.contains("Rate limit", ignoreCase = true) ||
                message.contains("Temporary error", ignoreCase = true))

    private fun pauseBeforeRetry(header: String?): Long {
        val value = header?.trim()?.toLongOrNull() ?: return tokenIntervalMillis
        return if (value >= 1_000_000_000_000L) (value - now()).coerceAtLeast(0) else value * 1000
    }

    override fun canMeetDeadline(deadline: Long, queuedAhead: Int): Boolean {
        return expectedFinishAt(queuedAhead) <= deadline
    }

    private fun expectedFinishAt(queuedAhead: Int, now: Long = now()): Long {
        val avgMillis = requestAverageProcessingTime.toMillis()
        val rateWait = (waiting.get() + queuedAhead).toLong() * rateWindow.toMillis() / rateLimitPerSec
        val slotWait = (pending.get() + queuedAhead).toLong() / parallelRequests * avgMillis
        return now + maxOf(rateWait, slotWait) + avgMillis
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
            var success = false
            var reason: String? = null
            while (true) {
                if (!awaitRatePermit(deadline)) {
                    rejectedCounter.increment()
                    success = false
                    reason = "Deadline exceeded while waiting for rate limiter"
                    break
                }
                outgoingCounter.increment()

                var retriable: Boolean
                var retryDelay = 0L
                try {
                    val call = client.newCall(request)
                    call.timeout().timeout((deadline - now()).coerceAtLeast(1), TimeUnit.MILLISECONDS)
                    call.execute().use { response ->
                        val code = response.code
                        val rawBody = response.body?.string()
                        val parsed = try {
                            mapper.readValue(rawBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $code, reason: $rawBody")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                        success = parsed.result
                        reason = parsed.message
                        val retriableByBody = !success && code == 200 && isTransientMessage(parsed.message)
                        retriable = when {
                            success -> false
                            code == 429 || code >= 500 -> true
                            code in 400..499 -> false
                            else -> retriableByBody
                        }
                        retryDelay = when {
                            code == 429 -> pauseBeforeRetry(response.header("Retry-After"))
                            retriableByBody -> tokenIntervalMillis
                            else -> 0L
                        }
                    }
                } catch (e: java.io.IOException) {
                    logger.error("[$accountName] Payment IO error for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                    success = false
                    reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                    retriable = true
                }

                logger.warn("[$accountName] Payment attempt $attempt for txId: $transactionId, payment: $paymentId, succeeded: $success, message: $reason")

                if (retriable && !success && attempt < MAX_ATTEMPTS - 1 &&
                    now() + retryDelay + requestAverageProcessingTime.toMillis() <= deadline
                ) {
                    if (retryDelay > 0) Thread.sleep(retryDelay)
                    attempt++
                    continue
                }
                break
            }

            (if (success) completedSuccess else completedFail).increment()
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            completedFail.increment()
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
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