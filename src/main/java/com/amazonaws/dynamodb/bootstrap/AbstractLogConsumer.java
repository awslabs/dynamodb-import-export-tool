/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;

/**
 * The interface to use with the DynamoDBBootstrapWorker.java class to consume
 * logs and write the results somewhere.
 */
public abstract class AbstractLogConsumer {

    // only keep a reference to the thread pool because we need to be able to shut it down
    private ExecutorService threadPool;
    protected ExecutorCompletionService<Void> exec;

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(AbstractLogConsumer.class);



    protected AbstractLogConsumer(int numThreads) {
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        this.exec = new ExecutorCompletionService<Void>(threadPool, new ArrayBlockingQueue<Future<Void>>(numThreads));
    }

    protected AbstractLogConsumer(ExecutorCompletionService<Void> exec, ExecutorService threadPool) {
        this.threadPool = threadPool;
        this.exec = exec;
    }

    /**
     * Writes the result of a scan to another endpoint asynchronously. Will call
     * getWorker to determine what job to submit with the result.
     * 
     * @param result
     *            the SegmentedScanResult to asynchronously write to another
     *            endpoint.
     */
    public abstract Future<Void> writeResult(SegmentedScanResult result);

    /**
     * Shuts the thread pool down.
     * 
     * @param awaitTermination
     *            If true, this method waits for the threads in the pool to
     *            finish. If false, this thread pool shuts down without
     *            finishing their current tasks.
     */
    public void shutdown(boolean awaitTermination) {
        if (awaitTermination) {
            boolean interrupted = false;
            threadPool.shutdown();
            try {
                while (!threadPool.awaitTermination(BootstrapConstants.WAITING_PERIOD_FOR_THREAD_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Waiting for the threadpool to terminate...");
                }
            } catch (InterruptedException e) {
                interrupted = true;
                LOGGER.warn("Threadpool was interrupted when trying to shutdown: "
                        + e.getMessage());
            } finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        } else {
            threadPool.shutdownNow();
        }
    }
}
