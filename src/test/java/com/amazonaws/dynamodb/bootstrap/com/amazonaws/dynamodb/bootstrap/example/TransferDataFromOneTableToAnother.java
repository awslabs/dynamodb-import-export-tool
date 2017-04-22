package com.amazonaws.dynamodb.bootstrap.com.amazonaws.dynamodb.bootstrap.example;

import com.amazonaws.dynamodb.bootstrap.DynamoDBBootstrapWorker;
import com.amazonaws.dynamodb.bootstrap.DynamoDBConsumer;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

class TransferDataFromOneTableToAnother {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(com.amazonaws.regions.Regions.US_WEST_1).build();
        DynamoDBBootstrapWorker worker = null;
        try {
            // 100.0 read operations per second. 4 threads to scan the table.
            worker = new DynamoDBBootstrapWorker(client,
                    100.0, "mySourceTable", 4);
        } catch (NullReadCapacityException e) {
            System.err.println("The DynamoDB source table returned a null read capacity.");
            System.exit(1);
        }
        // 50.0 write operations per second. 8 threads to scan the table.
        DynamoDBConsumer consumer = new DynamoDBConsumer(client, "myDestinationTable", 50.0,
                Executors.newFixedThreadPool(8));
        try {
            worker.pipe(consumer);
        } catch (ExecutionException e) {
            System.err.println("Encountered exception when executing transfer: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e){
            System.err.println("Interrupted when executing transfer: " + e.getMessage());
            System.exit(1);
        }
    }
}
