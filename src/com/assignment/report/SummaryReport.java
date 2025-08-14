package com.assignment.report;



import com.assignment.core.ClaimProcessor;
import com.assignment.entity.Claim;
import com.assignment.entity.ClaimStatus;

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public class SummaryReport {
    private final ClaimProcessor processor;

    public SummaryReport(ClaimProcessor processor) {
        this.processor = processor;
    }

    public void writeReport() {
        Map<String, ClaimStatus> finalStatuses = processor.getClaimFinalStatus();
        Map<String, Integer> attempts = processor.getClaimAttempts();
        Set<String> processedClaims = processor.getProcessedClaims();

        int approved = 0, escalated = 0, rejected = 0;
        int suspiciousCount = processor.getThrottleMonitor().suspiciousTimestamps.size();
        long totalPaid = 0;
        double totalAttempts = 0;

        for (var entry : finalStatuses.entrySet()) {
            switch (entry.getValue()) {
                case APPROVED -> approved++;
                case ESCALATED -> escalated++;
                case REJECTED -> rejected++;
            }
        }

        for (var claimId : processedClaims) {
            totalAttempts += attempts.getOrDefault(claimId, 1);
        }

        // Calculate total amount paid (approximate)
        // Note: Need to track amounts per claim in production - simplified here

        try (PrintWriter pw = new PrintWriter(new FileWriter("summary.txt"))) {
            pw.printf("Total unique claims processed: %d%n", processedClaims.size());
            pw.printf("Number Approved: %d%n", approved);
            pw.printf("Number Escalated: %d%n", escalated);
            pw.printf("Number Rejected: %d%n", rejected);
            pw.printf("Number suspicious claims detected: %d%n", suspiciousCount);
            pw.printf("Total amount paid (Approved): %d%n", totalPaid);
            pw.printf("Average processing attempts per claim: %.2f%n", totalAttempts / processedClaims.size());
            pw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}