package com.assignment.entity;

import java.util.Random;

public class ExternalCheckSimulator {
    private static final Random random = new Random();

    public enum Result {
        SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE
    }

    public static Result check(Claim claim) throws InterruptedException {
        Thread.sleep(500 + random.nextInt(1000)); // Simulate slowness
        int roll = random.nextInt(100);
        if (roll < 70) return Result.SUCCESS;
        else if (roll < 90) return Result.TRANSIENT_FAILURE;
        else return Result.PERMANENT_FAILURE;
    }
}