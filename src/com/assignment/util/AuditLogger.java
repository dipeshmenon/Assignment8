package com.assignment.util;


import com.assignment.entity.ClaimStatus;

import java.io.*;
import java.time.LocalDateTime;
import java.util.concurrent.*;

public class AuditLogger {
    private static final BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();
    private static final PrintWriter writer;

    static {
        try {
            writer = new PrintWriter(new FileWriter("audit.log", true));
            Thread loggerThread = new Thread(() -> {
                while (true) {
                    try {
                        String line = logQueue.take();
                        writer.println(line);
                        writer.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            loggerThread.setDaemon(true);
            loggerThread.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void log(String claimId, String threadName, ClaimStatus oldStatus, ClaimStatus newStatus, int attempt) {
        String log = String.format("%s | %s | %s | %s -> %s | attempt %d",
                LocalDateTime.now(), claimId, threadName, oldStatus, newStatus, attempt);
        logQueue.offer(log);
    }
}
