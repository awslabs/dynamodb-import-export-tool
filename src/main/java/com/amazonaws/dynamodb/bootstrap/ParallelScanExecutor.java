/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.BitSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

/**
 * This class executes multiple scan requests on one segment of a table in
 * series, as a runnable. Instances meant to be used as tasks of the worker
 * thread pool for parallel scans.
 * 
 */
public class ParallelScanExecutor {
    private final BitSet finished;
    private final ScanSegmentWorker[] workers;
    private final ExecutorCompletionService<SegmentedScanResult> exec;

    public ParallelScanExecutor(Executor executor, int segments) {
        this.exec = new ExecutorCompletionService<SegmentedScanResult>(executor);
        this.finished = new BitSet(segments);
        this.finished.clear();
        this.workers = new ScanSegmentWorker[segments];
    }

    /**
     * Set the segment to finished
     */
    public void finishSegment(int segment) {
        synchronized (finished) {
            if (segment > finished.size()) {
                throw new IllegalArgumentException(
                        "Invalid segment passed to finishSegment");
            }
            finished.set(segment);
        }
    }

    /**
     * returns if the scan is finished
     */
    public boolean finished() {
        synchronized (finished) {
            return finished.cardinality() == workers.length;
        }
    }

    /**
     * This method gets a segmentedScanResult and submits the next scan request
     * for that segment, if there is one.
     * 
     * @return the next available ScanResult
     * @throws ExecutionException
     *             if one of the segment pages threw while executing
     * @throws InterruptedException
     *             if one of the segment pages was interrupted while executing.
     */
    public SegmentedScanResult grab() throws ExecutionException,
            InterruptedException {
        Future<SegmentedScanResult> ret = exec.take();

        int segment = ret.get().getSegment();
        ScanSegmentWorker sw = workers[segment];

        if (sw.hasNext()) {
            exec.submit(sw);
        } else {
            finishSegment(segment);
        }

        return ret.get();
    }

    /**
     * adds a worker to the ExecutorCompletionService
     */
    public void addWorker(ScanSegmentWorker ssw, int segment) {
        workers[segment] = ssw;
        exec.submit(ssw);
    }
}
