package com.assignment.core;



import com.assignment.entity.Claim;

import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThrottleMonitor implements Runnable {
    private final Deque<Instant> suspiciousTimestamps = new LinkedList<>();
    private final AtomicBoolean throttled = new AtomicBoolean(false);
    private final BlockingQueue<Claim> queue;

    public ThrottleMonitor(BlockingQueue<Claim> queue) {
        this.queue = queue;
    }

    public boolean isThrottled() {
        return throttled.get();
    }

    @Override
    public void run() {
        while (true) {
            try {
                for (Claim claim : queue) {
                    if (claim.isSuspicious()) {
                        suspiciousTimestamps.add(Instant.now());
                        System.out.println("Suspicious claim: " + claim.claimId);
                    }
                }

                suspiciousTimestamps.removeIf(ts -> ts.isBefore(Instant.now().minusSeconds(Config.SUSPICIOUS_WINDOW_SEC)));

                if (suspiciousTimestamps.size() > Config.SUSPICIOUS_THRESHOLD) {
                    throttled.set(true);
                    System.out.println("THROTTLING INTAKE...");
                    Thread.sleep(2000); // Pause
                    throttled.set(false);
                }

                Thread.sleep(1000); // Run every 1s
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
