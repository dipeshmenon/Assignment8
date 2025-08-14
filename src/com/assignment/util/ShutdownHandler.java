package com.assignment.util;


public class ShutdownHandler {
    public static void register(Runnable callback) {
        Runtime.getRuntime().addShutdownHook(new Thread(callback));
    }
}