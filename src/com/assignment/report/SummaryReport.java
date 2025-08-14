package com.assignment.report;



import com.assignment.entity.Claim;
import com.assignment.entity.ClaimStatus;

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SummaryReport {
    private final AtomicInteger approved = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();
    private final AtomicInteger escalated = new AtomicInteger();
    private final AtomicInteger suspicious = new AtomicInteger();
    private final AtomicLong totalPaid = new AtomicLong();
    private final ConcurrentHashMap<String, Integer> attempts = new ConcurrentHashMap<>();

    private final long startTime = System.nanoTime();

    public void add(Claim claim, ClaimStatus status, int attempt) {
        switch (status) {
            case APPROVED -> {
                approved.incrementAndGet();
                totalPaid.addAndGet(claim.amount);
            }
            case ESCALATED -> escalated.incrementAndGet();
            case REJECTED -> rejected.incrementAndGet();
        }

        if (claim.isSuspicious()) suspicious.incrementAndGet();
        attempts.put(claim.claimId, attempt);
    }

    public void generate(int uniqueClaims) {
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        try (PrintWriter pw = new PrintWriter("summary.txt")) {
            pw.println("Total unique claims processed: " + uniqueClaims);
            pw.println("Approved: " + approved);
            pw.println("Escalated: " + escalated);
            pw.println("Rejected: " + rejected);
            pw.println("Suspicious claims detected: " + suspicious);
            pw.println("Total amount paid: " + totalPaid);
            pw.println("Average attempts per claim: " +
                    attempts.values().stream().mapToInt(i -> i).average().orElse

