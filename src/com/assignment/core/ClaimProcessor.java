package com.assignment.core;




import com.assignment.entity.Claim;
import com.assignment.entity.ClaimType;
import com.assignment.entity.Priority;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public class ClaimProcessor {
    private final BlockingQueue<Claim> backlog = new PriorityBlockingQueue<>(Config.BACKLOG_CAPACITY, Comparator
            .comparing((Claim c) -> c.priority).reversed()
            .thenComparing(c -> c.timestamp));
    private final Map<String, Lock> policyLocks = new ConcurrentHashMap<>();
    private final Set<String> processedClaims = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newFixedThreadPool(Config.WORKER_COUNT);
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private final SummaryReport report = new SummaryReport();
    private final ThrottleMonitor throttleMonitor;

    public ClaimProcessor() {
        throttleMonitor = new ThrottleMonitor(backlog);
    }

    public void start() throws Exception {
        loadClaims("claims.csv");

        // Start throttle monitor
        new Thread(throttleMonitor).start();

        // Start worker threads
        for (int i = 0; i < Config.WORKER_COUNT; i++) {
            workers.submit(new ClaimWorker(backlog, policyLocks, processedClaims, report));
        }
    }

    private void loadClaims(String filename) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        br.readLine(); // skip header
        DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            Claim claim = new Claim(
                    parts[0],
                    parts[1],
                    Integer.parseInt(parts[2]),
                    ClaimType.valueOf(parts[3].toUpperCase()),
                    LocalDateTime.parse(parts[4], dtf),
                    parts[5].equalsIgnoreCase("URGENT") ? Priority.URGENT : Priority.NORMAL
            );
            waitIfThrottled();
            backlog.put(claim); // Blocks if full
        }
        br.close();
    }

    private void waitIfThrottled() throws InterruptedException {
        while (throttleMonitor.isThrottled()) {
            Thread.sleep(200); // Wait during pause
        }
    }

    public void shutdown() {
        isShuttingDown.set(true);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(60, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
        }

        report.generate(processedClaims.size());
        System.out.println("Summary written to summary.txt");
    }
}
