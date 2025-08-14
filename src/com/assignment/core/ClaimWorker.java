package com.assignment.core;

import com.assignment.entity.Claim;
import com.assignment.entity.ClaimStatus;
import com.assignment.entity.ExternalCheckSimulator;
import com.assignment.main.Config;
import com.assignment.util.AuditLogger;

import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClaimWorker implements Runnable {
    private final ClaimProcessor processor;

    public ClaimWorker(ClaimProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void run() {
        while (processor.isIngesting() || !processor.getGlobalQueue().isEmpty()) {
            try {
                Claim claim = processor.takeClaim();
                if (claim == null) continue;

                ReentrantLock lock = processor.getPolicyLock(claim.policyNumber);
                if (lock == null) {

                    processor.enqueueClaim(claim);
                    continue;
                }

                boolean acquired = lock.tryLock(1, TimeUnit.SECONDS);
                if (!acquired) {

                    processor.enqueueClaim(claim);
                    continue;
                }

                try {

                    Deque<Claim> queue = processor.getPolicyQueue(claim.policyNumber);
                    if (queue == null || queue.peekFirst() != claim) {

                        processor.enqueueClaim(claim);
                        continue;
                    }


                    queue.pollFirst();


                    if (processor.getProcessedClaims().contains(claim.claimId)) {

                        AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                                ClaimStatus.RECEIVED, ClaimStatus.RECEIVED, claim.attempt);
                        continue;
                    }

                    claim.attempt++;
                    AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                            ClaimStatus.RECEIVED, ClaimStatus.PROCESSING, claim.attempt);

                    ExternalCheckSimulator.Result checkResult = runExternalCheckWithTimeout(claim);

                    if (checkResult == ExternalCheckSimulator.Result.SUCCESS) {
                        processor.markProcessed(claim, ClaimStatus.APPROVED);
                        AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                                ClaimStatus.PROCESSING, ClaimStatus.APPROVED, claim.attempt);
                        if (claim.isSuspicious()) {
                            processor.getThrottleMonitor().reportSuspicious(claim);
                        }
                    } else if (checkResult == ExternalCheckSimulator.Result.TRANSIENT_FAILURE) {
                        if (claim.attempt <= Config.RETRY_LIMIT) {
                            AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                                    ClaimStatus.PROCESSING, ClaimStatus.FAILED, claim.attempt);
                            processor.enqueueClaim(claim);
                        } else {
                            processor.markProcessed(claim, ClaimStatus.REJECTED);
                            AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                                    ClaimStatus.PROCESSING, ClaimStatus.REJECTED, claim.attempt);
                        }
                    } else {
                        processor.markProcessed(claim, ClaimStatus.REJECTED);
                        AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                                ClaimStatus.PROCESSING, ClaimStatus.REJECTED, claim.attempt);
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private ExternalCheckSimulator.Result runExternalCheckWithTimeout(Claim claim) throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            var future = exec.submit(() -> ExternalCheckSimulator.check(claim));
            return future.get(Config.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {

            return ExternalCheckSimulator.Result.TRANSIENT_FAILURE;
        } finally {
            exec.shutdownNow();
        }
    }
}