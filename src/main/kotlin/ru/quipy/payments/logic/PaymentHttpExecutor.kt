package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

data class PaymentHttpResult(val success: Boolean, val reason: String? = null)

class PaymentHttpExecutor(
    properties: PaymentAccountProperties,
    private val attemptTimeout: Duration,
    private val meterRegistry: MeterRegistry,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .followSslRedirects(false)
        .build(),
) {
    init {
        require(attemptTimeout.toMillis() > 0) { "Payment attempt timeout must be positive" }
        require(properties.rateLimitPerSec > 0 && properties.parallelRequests > 0)
    }

    private val mapper = ObjectMapper().registerKotlinModule()
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelWindow = Semaphore(properties.parallelRequests, true)
    private val estimatedAttemptMillis = minOf(properties.averageProcessingTime.toMillis(), attemptTimeout.toMillis()).coerceAtLeast(1)
    private val retryIntervalMillis = (1000L / properties.rateLimitPerSec).coerceAtLeast(1)
    private val accountName = properties.accountName
    private val pending = AtomicInteger()
    private val waiting = AtomicInteger()

    val pendingCount: Int get() = pending.get()
    val waitingCount: Int get() = waiting.get()

    private val outgoing = meterRegistry.counter("payment.outgoing", "account", accountName)
    private val retries = meterRegistry.counter("payment.retries", "account", accountName)
    private val timeouts = meterRegistry.counter("payment.timeouts", "account", accountName)
    private val durations = listOf("success", "failure", "timeout", "io_error").associateWith { outcome ->
        Timer.builder("payment.http.duration")
            .description("Duration of one provider HTTP attempt, including failed and timed out attempts")
            .tags("account", accountName, "outcome", outcome)
            .publishPercentileHistogram()
            .serviceLevelObjectives(*longArrayOf(50, 100, 250, 500, 1000, 2000, 5000).map(Duration::ofMillis).toTypedArray())
            .register(meterRegistry)
    }

    init {
        val tags = listOf(Tag.of("account", accountName))
        meterRegistry.gauge("payment.pending", tags, pending) { it.get().toDouble() }
        meterRegistry.gauge("payment.waiting", tags, waiting) { it.get().toDouble() }
        meterRegistry.gauge("payment.http.timeout.seconds", tags, attemptTimeout) { it.toMillis() / 1000.0 }
    }

    fun execute(request: Request, deadline: Long): PaymentHttpResult {
        pending.incrementAndGet()
        try {
            for (attempt in 0 until MAX_ATTEMPTS) {
                if (deadline - now() < estimatedAttemptMillis) return PaymentHttpResult(false, "Payment deadline exceeded")

                val acquired = parallelWindow.tryAcquire(
                    (deadline - now() - estimatedAttemptMillis).coerceAtLeast(0), TimeUnit.MILLISECONDS
                )
                if (!acquired) return PaymentHttpResult(false, "Deadline exceeded while waiting for parallel slot")

                val result: AttemptResult
                try {
                    if (!awaitRatePermit(deadline)) return PaymentHttpResult(false, "Deadline exceeded while waiting for rate limiter")
                    val remaining = deadline - now()
                    if (remaining < estimatedAttemptMillis) return PaymentHttpResult(false, "Payment deadline exceeded")

                    outgoing.increment()
                    if (attempt > 0) retries.increment()
                    result = performAttempt(request, minOf(attemptTimeout.toMillis(), remaining))
                } finally {
                    parallelWindow.release()
                }

                if (result.payment.success || !result.retriable || attempt == MAX_ATTEMPTS - 1) return result.payment
                if (result.retryDelayMillis > deadline - now() - estimatedAttemptMillis) return result.payment
                if (result.retryDelayMillis > 0) Thread.sleep(result.retryDelayMillis)
            }
            error("Attempt loop must return a result")
        } finally {
            pending.decrementAndGet()
        }
    }

    private fun awaitRatePermit(deadline: Long): Boolean {
        waiting.incrementAndGet()
        try {
            while (deadline - now() >= estimatedAttemptMillis) {
                if (Thread.currentThread().isInterrupted) throw InterruptedException("Payment interrupted")
                if (rateLimiter.tick()) return true
                Thread.sleep(10)
            }
            return false
        } finally {
            waiting.decrementAndGet()
        }
    }

    private fun performAttempt(request: Request, timeoutMillis: Long): AttemptResult {
        val sample = Timer.start(meterRegistry)
        var outcome = "io_error"
        try {
            val call = client.newCall(request)
            call.timeout().timeout(timeoutMillis, TimeUnit.MILLISECONDS)
            return call.execute().use { response ->
                val rawBody = response.body?.string()
                val body = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (_: Exception) {
                    null
                }
                val success = response.isSuccessful && body?.result == true
                outcome = if (success) "success" else "failure"
                val transientBody = response.code == 200 && body?.result == false &&
                    (body.message?.contains("Temporary error", ignoreCase = true) == true ||
                        body.message?.contains("Rate limit", ignoreCase = true) == true)
                val retriable = !success && (response.code == 429 || response.code in 500..599 || transientBody)
                val delay = when {
                    response.code == 429 -> retryDelay(response.header("Retry-After"))
                    transientBody -> retryIntervalMillis
                    else -> 0L
                }
                AttemptResult(
                    PaymentHttpResult(success, body?.message ?: if (success) null else "HTTP ${response.code}"),
                    retriable, delay
                )
            }
        } catch (e: IOException) {
            val timeout = e is InterruptedIOException && !Thread.currentThread().isInterrupted
            outcome = if (timeout) "timeout" else "io_error"
            if (timeout) timeouts.increment()
            if (Thread.currentThread().isInterrupted) throw InterruptedException("Payment interrupted")
            return AttemptResult(PaymentHttpResult(false, if (timeout) "Request timeout." else e.message), true, 0)
        } finally {
            sample.stop(durations.getValue(outcome))
        }
    }

    private fun retryDelay(header: String?): Long {
        val value = header?.trim() ?: return retryIntervalMillis
        value.toLongOrNull()?.let {
            return when {
                it < 0 -> retryIntervalMillis
                it >= 1_000_000_000_000L -> (it - now()).coerceAtLeast(0)
                it > Long.MAX_VALUE / 1000 -> Long.MAX_VALUE
                else -> it * 1000
            }
        }
        return try {
            (ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() - now()).coerceAtLeast(0)
        } catch (_: Exception) {
            retryIntervalMillis
        }
    }

    private data class AttemptResult(val payment: PaymentHttpResult, val retriable: Boolean, val retryDelayMillis: Long)

    companion object {
        private const val MAX_ATTEMPTS = 3
    }
}
