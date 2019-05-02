/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap.constants;

import java.nio.charset.Charset;

/**
 * Constants for bootstrapping.
 */
public class BootstrapConstants {

    /**
     * Seconds to wait for thread to terminate before retrying or exiting.
     */
    public static final int WAITING_PERIOD_FOR_THREAD_TERMINATION_SECONDS = 10;

    /**
     * Number of bytes in a gigabyte.
     */
    public static final double GIGABYTE = Math.pow(1024, 3);

    /**
     * Max ThreadPool size for the ExecutorService to use.
     */
    public static final int DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE = Runtime
            .getRuntime().availableProcessors() * 128;

    /**
     * Core pool size of a default thread pool.
     */
    public static final int DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE = Runtime
            .getRuntime().availableProcessors() * 4;

    /**
     * Amount of time in milliseconds to keep the ExecutorService alive for
     * inactive threads before shutting down.
     */
    public static final long DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE = 60000L;

    /**
     * Max amount of items to be included in a batch to write items to DynamoDB.
     */
    public static final int MAX_BATCH_SIZE_WRITE_ITEM = 25;

    /**
     * Max amount of time to back off before retrying.
     */
    public static final long MAX_EXPONENTIAL_BACKOFF_TIME = 2048;

    /**
     * Max amount of retries before exiting or throwing exception.
     */
    public static final int MAX_RETRIES = 10;

    /**
     * Initial retry time for an exponential back-off call in milliseconds.
     */
    public static final long INITIAL_RETRY_TIME_MILLISECONDS = 128;

    /**
     * UTF8 Charset.
     */
    public static final Charset UTF8 = Charset.forName("UTF8");

    /**
     * Each DynamoDB List element has 1 byte overhead for type. Adding 1 byte to
     * account for it in item size
     */
    public static final int BASE_LOGICAL_SIZE_OF_NESTED_TYPES = 1;

    /**
     * The size of an empty document in a DynamoDB item.
     */
    public static final int LOGICAL_SIZE_OF_EMPTY_DOCUMENT = 3;

    /**
     * Max number of bytes in a DynamoDB number attribute.
     */
    public static final int MAX_NUMBER_OF_BYTES_FOR_NUMBER = 21;
    
    /**
     * Number of bytes for an item being read with strongly consistent reads
     */
    public static final int STRONGLY_CONSISTENT_READ_ITEM_SIZE = 4 * 1024;
    
    /**
     * Number of bytes for an item being read with eventually consistent reads
     */
    public static final int EVENTUALLY_CONSISTENT_READ_ITEM_SIZE = 2 * STRONGLY_CONSISTENT_READ_ITEM_SIZE;

    /**
     * Max scan result size
     */
    public static final int SCAN_LIMIT = 1000;

    /**
     * Max connection size limit
     */
    public static final int MAX_CONN_SIZE = 5000;
}
