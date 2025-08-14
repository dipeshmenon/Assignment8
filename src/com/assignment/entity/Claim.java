package com.assignment.entity;


import java.time.LocalDateTime;
import java.time.LocalDateTime;

public class Claim {
    public String claimId;
    public String policyNumber;
    public int amount;
    public ClaimType type;
    public LocalDateTime timestamp;
    public Priority priority;
    public int attempt = 0;

    public Claim(String claimId, String policyNumber, int amount, ClaimType type, LocalDateTime timestamp, Priority priority) {
        this.claimId = claimId;
        this.policyNumber = policyNumber;
        this.amount = amount;
        this.type = type;
        this.timestamp = timestamp;
        this.priority = priority;
    }

    public boolean isSuspicious() {
        return type == ClaimType.ACCIDENT && amount >= 400000;
    }

    public boolean isUrgent() {
        return priority == Priority.URGENT;
    }

    @Override
    public String toString() {
        return claimId + " (" + policyNumber + ")";
    }
}
