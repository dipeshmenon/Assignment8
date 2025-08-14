package com.assignment.main;

import com.assignment.core.ClaimProcessor;
import com.assignment.main.Config;
import com.assignment.util.ShutdownHandler;

public class Main {
    public static void main(String[] args) {
        try {
            // Load config from file
            Config.load("/Users/DIPESH.M/Documents/Assignment8/src/com/assignment/main/config.properties");

            // Start claim processor
            ClaimProcessor processor = new ClaimProcessor();
            processor.start();

            // Register graceful shutdown hook
            ShutdownHandler.register(() -> {
                System.out.println("Shutdown initiated...");
                processor.shutdown();
            });

        } catch (Exception e) {
            System.err.println("Error during startup: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
