/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * A unit test that helps identify the functionality and behavior of {@link CompletableFuture}.
 * @author Randall Hauch
 */
public class CompletionTest {

    @Test
    public void testCompletion() throws Exception  {
        CompletableFuture<Void> status = CompletableFuture.completedFuture(null).thenRun(()->System.out.println("Completed!"));
        status.get(); // wait
        System.out.println("All completed!");
        Thread.sleep(3000);
    }
    @Test
    public void testNormalCompletionWithEarlyTaskCompletion() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(taskWork(1, running), executor);
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(taskQuick(2, running), executor);
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(taskWork(3, running), executor);
        Thread.sleep(1000);
        CompletableFuture<Void> master = CompletableFuture.allOf(task1, task2, task3);

        Thread.sleep(2000);
        running.set(false);
        master.get(); // wait
        System.out.println("All completed!");
        Thread.sleep(3000);
    }

    @Test
    public void testNormalCompletion() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(taskWork(1, running), executor);
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(taskWork(2, running), executor);
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(taskWork(3, running), executor);
        CompletableFuture<Void> master = CompletableFuture.allOf(task1, task2, task3);

        Thread.sleep(2000);
        running.set(false);
        master.get(); // wait
        System.out.println("All completed!");
    }

    @Test
    public void testCancellation() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(taskWork(1, running), executor);
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(taskWork(2, running), executor);
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(taskWork(3, running), executor);
        CompletableFuture<Void> master = CompletableFuture.allOf(task1, task2, task3);

        Thread.sleep(3000);
        try {
            boolean result = master.cancel(true);
            System.out.println("Master cancelled = " + result);
        } catch (CancellationException e) {
            System.out.println("Master cancelled!");
        }
        Thread.sleep(3000);
        // running.set(false);
        // master.get(); // wait
        // System.out.println("All completed!");
    }

    public Runnable taskQuick(int number, AtomicBoolean running) {
        return () -> {
            System.out.println("Task " + number + " starting");
            running.set(false);
            System.out.println("Task " + number + " stopped!");
        };
    }

    public Runnable taskWork(int number, AtomicBoolean running) {
        return () -> {
            while (running.get()) {
                System.out.println("Task " + number + " starting");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    System.out.println("Task " + number + " interrupted!");
                }
                System.out.println("Task " + number);
            }
            System.out.println("Task " + number + " completed!");
        };
    }

}
