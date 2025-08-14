package com.assignment.main;


import java.io.FileInputStream;
import java.util.Properties;

public class Config {
    public static int WORKER_COUNT = 8;
    public static int BACKLOG_CAPACITY = 100;
    public static int TIMEOUT_MS = 3000;
    public static int RETRY_LIMIT = 3;
    public static int SUSPICIOUS_WINDOW_SEC = 30;
    public static int SUSPICIOUS_THRESHOLD = 5;

    public static void load(String path) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }

        WORKER_COUNT = Integer.parseInt(props.getProperty("worker.count", "8"));
        BACKLOG_CAPACITY = Integer.parseInt(props.getProperty("backlog.capacity", "100"));
        TIMEOUT_MS = Integer.parseInt(props.getProperty("timeout.ms", "3000"));
        RETRY_LIMIT = Integer.parseInt(props.getProperty("retry.limit", "3"));
        SUSPICIOUS_WINDOW_SEC = Integer.parseInt(props.getProperty("suspicious.window.seconds", "30"));
        SUSPICIOUS_THRESHOLD = Integer.parseInt(props.getProperty("suspicious.threshold", "5"));
    }
}
