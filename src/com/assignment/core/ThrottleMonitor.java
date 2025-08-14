package com.assignment.core;



import com.assignment.entity.Claim;
import com.assignment.main.Config;

import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThrottleMonitor implements Runnable {
    private final ClaimProcessor processor;
    public final Deque<Instant> suspiciousTimestamps = new LinkedList<>();
    private volatile boolean running = true;
    private volatile boolean paused = false;

    public ThrottleMonitor(ClaimProcessor processor) {
        this.processor = processor;
    }

    public synchronized void reportSuspicious(Claim claim) {
        Instant now = Instant.now();
        suspiciousTimestamps.addLast(now);


        System.out.printf("Suspicious claim detected: %s amount=%d type=%s%n",
                claim.claimId, claim.amount, claim.type);

        cleanupOld(now);

        if (suspiciousTimestamps.size() > Config.SUSPICIOUS_THRESHOLD && !paused) {
            paused = true;
            new Thread(() -> {
                try {
                    System.out.println("Throttling intake for 2 seconds due to suspicious claims.");
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}
                synchronized (this) {
                    paused = false;
                    suspiciousTimestamps.clear();
                    this.notifyAll();
                }
            }).start();
        }
    }

    private void cleanupOld(Instant now) {
        Instant windowStart = now.minusSeconds(Config.SUSPICIOUS_WINDOW_SEC);
        while (!suspiciousTimestamps.isEmpty() && suspiciousTimestamps.peekFirst().isBefore(windowStart)) {
            suspiciousTimestamps.pollFirst();
        }
    }

    public synchronized boolean isPaused() {
        return paused;
    }

    @Override
    public void run() {
        while (running) {
            synchronized (this) {
                if (paused) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() {
        running = false;
        synchronized (this) {
            this.notifyAll();
        }
    }
}
