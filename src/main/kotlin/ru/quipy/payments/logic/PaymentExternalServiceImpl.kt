package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
    attemptTimeout: Duration = Duration.ofSeconds(1),
) : PaymentExternalSystemAdapter {

    private val rateWindow = Duration.ofSeconds(1)
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val httpExecutor = PaymentHttpExecutor(properties, attemptTimeout, meterRegistry)

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

    private val rejectedCounter = meterRegistry.counter("payment.rejected", "account", accountName)
    private val completedSuccess = meterRegistry.counter("payment.completed", "account", accountName, "result", "success")
    private val completedFail = meterRegistry.counter("payment.completed", "account", accountName, "result", "fail")

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

    override fun canMeetDeadline(deadline: Long, queuedAhead: Int): Boolean {
        return expectedFinishAt(queuedAhead) <= deadline
    }

    private fun expectedFinishAt(queuedAhead: Int, now: Long = now()): Long {
        val avgMillis = requestAverageProcessingTime.toMillis()
        val rateWait = (httpExecutor.waitingCount + queuedAhead).toLong() * rateWindow.toMillis() / rateLimitPerSec
        val slotWait = (httpExecutor.pendingCount + queuedAhead).toLong() / parallelRequests * avgMillis
        return now + maxOf(rateWait, slotWait) + avgMillis
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

            val result = httpExecutor.execute(request, deadline)
            val success = result.success
            val reason = result.reason

            (if (success) completedSuccess else completedFail).increment()
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            completedFail.increment()
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()