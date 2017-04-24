/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap.example;

import java.util.concurrent.ExecutionException;

import com.amazonaws.dynamodb.bootstrap.consumer.BlockingQueueConsumer;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.worker.DynamoDBBootstrapWorker;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

class TransferDataFromOneTableToBlockingQueue {
    public static void main(String[] args) {
        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(com.amazonaws.regions.Regions.US_WEST_1).build();
        try {
            // 100.0 read operations per second. 4 threads to scan the table.
            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(client, 100.0, "mySourceTable", 4);
            final BlockingQueueConsumer consumer = new BlockingQueueConsumer(8);
            worker.pipe(consumer);
        } catch (NullReadCapacityException e) {
            System.err.println("The DynamoDB source table returned a null read capacity.");
            System.exit(1);
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Encountered exception when executing transfer: " + e.getMessage());
            System.exit(1);
        }
    }
}
