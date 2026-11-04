package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun canAccept(deadline: Long, queuedAhead: Int): Boolean =
        paymentAccounts.any { it.isEnabled() && it.canMeetDeadline(deadline, queuedAhead) }

    override fun tryAdmit(deadline: Long, queuedAhead: Int): Admission {
        var retryAt = Long.MAX_VALUE
        for (account in paymentAccounts) {
            if (!account.isEnabled()) continue
            val decision = account.tryAdmit(deadline, queuedAhead)
            if (decision.accepted) return decision
            if (decision.retryAfterTimestamp < retryAt) retryAt = decision.retryAfterTimestamp
        }
        if (retryAt == Long.MAX_VALUE) retryAt = System.currentTimeMillis() + 1_000
        return Admission(accepted = false, retryAfterTimestamp = retryAt)
    }
}
