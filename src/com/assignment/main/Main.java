package com.assignment.main;

import com.assignment.core.ClaimProcessor;
import com.assignment.main.Config;
import com.assignment.util.ShutdownHandler;

public class Main {
    public static void main(String[] args) {
        try {

            Config.load("/Users/DIPESH.M/Documents/Assignment8/src/com/assignment/main/config.properties");


            ClaimProcessor processor = new ClaimProcessor();
            processor.start();


            ShutdownHandler.register(() -> {
                System.out.println("Shutting Down");
                processor.shutdown();
            });

        } catch (Exception e) {
            System.err.println("Error during startup: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
