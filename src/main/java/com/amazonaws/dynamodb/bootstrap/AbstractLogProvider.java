/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;

/**
 * Abstract class to send inputs from a source to a consumer.
 */
public abstract class AbstractLogProvider {

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(AbstractLogProvider.class);

    protected ExecutorService threadPool;

    /**
     * Begins to read log results and transfer them to the consumer who will
     * write the results.
     */
    public abstract void pipe(final AbstractLogConsumer consumer)
            throws ExecutionException, InterruptedException;

    /**
     * Shuts the thread pool down.
     * 
     * @param <awaitTermination>
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
                    LOGGER.info("Waiting for the threadpool to terminate...");
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
