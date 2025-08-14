package com.assignment.core;

import com.assignment.entity.Claim;
import com.assignment.entity.ClaimStatus;
import com.assignment.entity.ExternalCheckSimulator;
import com.assignment.util.AuditLogger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClaimWorker implements Runnable {
    private final BlockingQueue<Claim> queue;
    private final Map<String, Lock> policyLocks;
    private final Set<String> processedClaims;
    private final SummaryReport report;

    public ClaimWorker(BlockingQueue<Claim> queue, Map<String, Lock> policyLocks, Set<String> processedClaims, SummaryReport report) {
        this.queue = queue;
        this.policyLocks = policyLocks;
        this.processedClaims = processedClaims;
        this.report = report;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Claim claim = queue.take();
                if (processedClaims.contains(claim.claimId)) continue;

                Lock lock = policyLocks.computeIfAbsent(claim.policyNumber, k -> new ReentrantLock());
                lock.lock();
                try {
                    processClaim(claim);
                } finally {
                    lock.unlock();
                }

            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void processClaim(Claim claim) {
        claim.attempt++;
        ClaimStatus oldStatus = ClaimStatus.RECEIVED;
        ClaimStatus newStatus = ClaimStatus.PROCESSING;
        AuditLogger.log(claim.claimId, Thread.currentThread().getName(), oldStatus, newStatus, claim.attempt);

        Result result = null;

        Callable<Result> task = () -> ExternalCheckSimulator.check(claim);
        FutureTask<Result> future = new FutureTask<>(task);
        new Thread(future).start();

        try {
            result = future.get(Config.TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            result = Result.TRANSIENT_FAILURE;
            future.cancel(true);
        }

        switch (result) {
            case SUCCESS -> {
                AuditLogger.log(claim.claimId, Thread.currentThread().getName(), newStatus, ClaimStatus.APPROVED, claim.attempt);
                processedClaims.add(claim.claimId);
                report.add(claim, ClaimStatus.APPROVED, claim.attempt);
            }
            case PERMANENT_FAILURE -> {
                AuditLogger.log(claim.claimId, Thread.currentThread().getName(), newStatus, ClaimStatus.REJECTED, claim.attempt);
                processedClaims.add(claim.claimId);
                report.add(claim, ClaimStatus.REJECTED, claim.attempt);
            }
            case TRANSIENT_FAILURE -> {
                if (claim.attempt < Config.RETRY_LIMIT) {
                    queue.offer(claim);
                } else {
                    AuditLogger.log(claim.claimId, Thread.currentThread().getName(), newStatus, ClaimStatus.ESCALATED, claim.attempt);
                    processedClaims.add(claim.claimId);
                    report.add(claim, ClaimStatus.ESCALATED, claim.attempt);
                }
            }
        }
    }
}