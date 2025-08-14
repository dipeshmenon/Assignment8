package com.assignment.core;




import com.assignment.entity.Claim;
import com.assignment.entity.ClaimStatus;
import com.assignment.entity.ClaimType;
import com.assignment.entity.Priority;
import com.assignment.main.Config;
import com.assignment.report.SummaryReport;
import com.assignment.util.AuditLogger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClaimProcessor {
    private final int workerCount = Config.WORKER_COUNT;
    private final int backlogCapacity = Config.BACKLOG_CAPACITY;
    private final int retryLimit = Config.RETRY_LIMIT;

    private final BlockingQueue<Claim> globalQueue;
    private final Map<String, Deque<Claim>> policyQueues = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> policyLocks = new ConcurrentHashMap<>();
    private final Set<String> processedClaims = ConcurrentHashMap.newKeySet();
    private final Map<String, List<String>> claimHistory = new ConcurrentHashMap<>();
    private final Map<String, ClaimStatus> claimFinalStatus = new ConcurrentHashMap<>();
    private final Map<String, Integer> claimAttempts = new ConcurrentHashMap<>();
    private final ExecutorService workers;
    private final ThrottleMonitor throttleMonitor;

    private volatile boolean ingesting = true;

    private long startTime;

    public ClaimProcessor() {

        this.globalQueue = new PriorityBlockingQueue<>(backlogCapacity, (c1, c2) -> {
            if (c1.priority != c2.priority)
                return c1.priority == Priority.URGENT ? -1 : 1;
            return c1.timestamp.compareTo(c2.timestamp);
        });
        this.workers = Executors.newFixedThreadPool(workerCount);
        this.throttleMonitor = new ThrottleMonitor(this);
    }

    public void start() throws IOException {
        startTime = System.nanoTime();

        new Thread(throttleMonitor).start();


        Thread ingestionThread = new Thread(this::ingestClaims);
        ingestionThread.start();


        for (int i = 0; i < workerCount; i++) {
            workers.submit(new ClaimWorker(this));
        }

        try {
            ingestionThread.join();
            workers.shutdown();
            workers.awaitTermination(1, TimeUnit.HOURS);
            throttleMonitor.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }


        new SummaryReport(this).writeReport();

        long elapsed = System.nanoTime() - startTime;
        System.out.printf("Processing complete in %.2f seconds.%n", elapsed / 1e9);
    }

    private void ingestClaims() {
        try (BufferedReader br = Files.newBufferedReader(Paths.get("/Users/DIPESH.M/Documents/Assignment8/src/com/assignment/main/Claims Data - Sheet2.csv"),java.nio.charset.StandardCharsets.UTF_8)) {
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                waitForBacklogSpace();
                Claim claim = parseClaim(line);
                if (claim == null) continue;

                if (!processedClaims.contains(claim.claimId)) {
                    enqueueClaim(claim);
                } else {

                    AuditLogger.log(claim.claimId, Thread.currentThread().getName(),
                            ClaimStatus.RECEIVED, ClaimStatus.RECEIVED, 0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            ingesting = false;
        }
    }

    private void waitForBacklogSpace() throws InterruptedException {
        while (globalQueue.size() >= backlogCapacity || throttleMonitor.isPaused()) {
            Thread.sleep(50);
        }
    }

    private Claim parseClaim(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length != 6) return null;
            String claimId = parts[0].trim();
            String policyNumber = parts[1].trim();
            int amount = Integer.parseInt(parts[2].trim());
            ClaimType type = ClaimType.valueOf(parts[3].trim().toUpperCase());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime timestamp = LocalDateTime.parse(parts[4].trim(), formatter);

            Priority priority = Priority.valueOf(parts[5].trim().toUpperCase());

            Claim claim = new Claim(claimId, policyNumber, amount, type, timestamp, priority);
            return claim;
        } catch (Exception e) {
            System.err.println("Failed to parse line: " + line);
            return null;
        }
    }

    public void enqueueClaim(Claim claim) {

        policyQueues.computeIfAbsent(claim.policyNumber, k -> new ConcurrentLinkedDeque<>()).addLast(claim);

        policyLocks.computeIfAbsent(claim.policyNumber, k -> new ReentrantLock());


        try {
            globalQueue.put(claim);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Claim takeClaim() throws InterruptedException {
        return globalQueue.take();
    }

    public void markProcessed(Claim claim, ClaimStatus finalStatus) {
        processedClaims.add(claim.claimId);
        claimFinalStatus.put(claim.claimId, finalStatus);
        claimAttempts.put(claim.claimId, claim.attempt);
    }

    public ReentrantLock getPolicyLock(String policyNumber) {
        return policyLocks.get(policyNumber);
    }

    public Deque<Claim> getPolicyQueue(String policyNumber) {
        return policyQueues.get(policyNumber);
    }

    public boolean isIngesting() {
        return ingesting;
    }

    public void recordHistory(String claimId, String event) {
        claimHistory.computeIfAbsent(claimId, k -> new ArrayList<>()).add(event);
    }

    public Map<String, ClaimStatus> getClaimFinalStatus() {
        return claimFinalStatus;
    }

    public Map<String, Integer> getClaimAttempts() {
        return claimAttempts;
    }

    public Map<String, List<String>> getClaimHistory() {
        return claimHistory;
    }

    public Set<String> getProcessedClaims() {
        return processedClaims;
    }

    public BlockingQueue<Claim> getGlobalQueue() {
        return globalQueue;
    }

    public ThrottleMonitor getThrottleMonitor() {
        return throttleMonitor;
    }

    public void shutdown() {
        ingesting = false;
        workers.shutdownNow();
        throttleMonitor.stop();
    }
}