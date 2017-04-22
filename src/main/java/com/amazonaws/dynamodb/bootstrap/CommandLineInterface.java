/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.dynamodb.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import lombok.extern.log4j.Log4j;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * The interface that parses the arguments, and begins to transfer data from one
 * DynamoDB table to another
 */
@Log4j
public class CommandLineInterface {

    public static final String ENCOUNTERED_EXCEPTION_WHEN_EXECUTING_TRANSFER = "Encountered exception when executing transfer.";

    static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(Region region, Optional<String> endpoint, String endpointPrefix) {
        return new AwsClientBuilder.EndpointConfiguration(endpoint.or("https://" + region.getServiceEndpoint(endpointPrefix)), region.getName());
    }

    private final AwsClientBuilder.EndpointConfiguration sourceEndpointConfiguration;
    private final String sourceTable;
    private final AwsClientBuilder.EndpointConfiguration destinationEndpointConfiguration;
    private final String destinationTable;
    private final double readThroughputRatio;
    private final double writeThroughputRatio;
    private final int maxWriteThreads;
    private final boolean isConsistentScan;
    private final boolean createDestinationTableIfMissing;
    private final boolean copyStreamSpecification;
    private final int sectionNumber;
    private final int totalSections;

    private CommandLineInterface(final CommandLineArgs params) {
        sourceEndpointConfiguration = createEndpointConfiguration(Region.getRegion(Regions.fromName(params.getSourceSigningRegion())),
                Optional.fromNullable(params.getSourceEndpoint()), AmazonDynamoDB.ENDPOINT_PREFIX);
        sourceTable = params.getSourceTable();
        destinationEndpointConfiguration = createEndpointConfiguration(Region.getRegion(Regions.fromName(params.getDestinationSigningRegion())),
                Optional.fromNullable(params.getDestinationEndpoint()), AmazonDynamoDB.ENDPOINT_PREFIX);
        destinationTable = params.getDestinationTable();
        readThroughputRatio = params.getReadThroughputRatio();
        writeThroughputRatio = params.getWriteThroughputRatio();
        maxWriteThreads = params.getMaxWriteThreads();
        isConsistentScan = params.isConsistentScan();
        createDestinationTableIfMissing = params.isCreateDestinationTableIfMissing();
        copyStreamSpecification = params.isCopyStreamSpecification();
        sectionNumber = params.getSection();
        totalSections = params.getTotalSections();
    }

    static List<GlobalSecondaryIndex> convertGlobalSecondaryIndexDescriptions(List<GlobalSecondaryIndexDescription> list) {
        final List<GlobalSecondaryIndex> result = new ArrayList<>(list.size());
        for (GlobalSecondaryIndexDescription description : list) {
            result.add(new GlobalSecondaryIndex()
                    .withIndexName(description.getIndexName())
                    .withKeySchema(description.getKeySchema())
                    .withProjection(description.getProjection())
                    .withProvisionedThroughput(getProvisionedThroughputFromDescription(description.getProvisionedThroughput())));
        }
        return result;
    }

    static List<LocalSecondaryIndex> convertLocalSecondaryIndexDescriptions(List<LocalSecondaryIndexDescription> list) {
        final List<LocalSecondaryIndex> result = new ArrayList<>(list.size());
        for (LocalSecondaryIndexDescription description : list) {
            result.add(new LocalSecondaryIndex()
                    .withIndexName(description.getIndexName())
                    .withKeySchema(description.getKeySchema())
                    .withProjection(description.getProjection()));
        }
        return result;
    }

    @VisibleForTesting
    static CreateTableRequest convertTableDescriptionToCreateTableRequest(TableDescription description,
                                                                          String newTableName,
                                                                          boolean copyStreamSpecification) {
        List<GlobalSecondaryIndexDescription> gsiDesc = description.getGlobalSecondaryIndexes();
        List<GlobalSecondaryIndex> gsi = gsiDesc == null ? null : convertGlobalSecondaryIndexDescriptions(gsiDesc);
        List<LocalSecondaryIndexDescription> lsiDesc = description.getLocalSecondaryIndexes();
        List<LocalSecondaryIndex> lsi = lsiDesc == null ? null : convertLocalSecondaryIndexDescriptions(lsiDesc);
        ProvisionedThroughput pt = getProvisionedThroughputFromDescription(description.getProvisionedThroughput());
        CreateTableRequest ctr = new CreateTableRequest()
                .withTableName(newTableName)
                .withProvisionedThroughput(pt)
                .withAttributeDefinitions(description.getAttributeDefinitions())
                .withKeySchema(description.getKeySchema())
                .withGlobalSecondaryIndexes(gsi)
                .withLocalSecondaryIndexes(lsi);
        if (copyStreamSpecification) {
            ctr.withStreamSpecification(description.getStreamSpecification());
        }
        return ctr;
    }

    private static ProvisionedThroughput getProvisionedThroughputFromDescription(ProvisionedThroughputDescription description) {
        return new ProvisionedThroughput(description.getReadCapacityUnits(), description.getWriteCapacityUnits());
    }

    private void bootstrapTable() throws InterruptedException, ExecutionException, SectionOutOfRangeException {
        final ClientConfiguration config = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);

        final DefaultAWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();
        final AmazonDynamoDB sourceClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(sourceEndpointConfiguration)
                .withCredentials(credentials)
                .withClientConfiguration(config)
                .build();
        final AmazonDynamoDB destinationClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(destinationEndpointConfiguration)
                .withCredentials(credentials)
                .withClientConfiguration(config)
                .build();

        final TableDescription readTableDescription = sourceClient.describeTable(sourceTable).getTable();
        try {
            destinationClient.describeTable(destinationTable).getTable();
        } catch(ResourceNotFoundException e) {
            if(!createDestinationTableIfMissing) {
                throw new IllegalArgumentException("Destination table " + destinationTable + " did not exist", e);
            }
            try {
                destinationClient.createTable(convertTableDescriptionToCreateTableRequest(readTableDescription,
                        destinationTable, copyStreamSpecification));
                destinationClient.waiters().tableExists().run(new WaiterParameters<>(new DescribeTableRequest(destinationTable)));
            } catch(WaiterUnrecoverableException | WaiterTimedOutException | AmazonServiceException ase) {
                throw new IllegalArgumentException("Unable to create destination table", ase);
            }
        }

        final TableDescription writeTableDescription = destinationClient.describeTable(destinationTable).getTable();

        final int numSegments;
        try {
            numSegments = DynamoDBBootstrapWorker.estimateNumberOfSegments(readTableDescription);
        } catch (NullReadCapacityException e) {
            throw new IllegalStateException("All tables should have a read capacity set", e);
        }

        final double readThroughput = calculateThroughput(readTableDescription, readThroughputRatio, true);
        final double writeThroughput = calculateThroughput(writeTableDescription, writeThroughputRatio, false);

        final ExecutorService sourceExec = getThreadPool(numSegments);
        final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(sourceClient, readThroughput, sourceTable,
                sourceExec, sectionNumber, totalSections, numSegments, isConsistentScan);

        final ExecutorService destinationExec = getThreadPool(maxWriteThreads);
        final DynamoDBConsumer consumer = new DynamoDBConsumer(destinationClient, destinationTable, writeThroughput, destinationExec);

        log.info("Starting transfer.");
        worker.pipe(consumer);
        log.info("Finished transfer.");
    }

    /**
     * Main class to begin transferring data from one DynamoDB table to another
     * DynamoDB table.
     * 
     * @param args
     */
    public static void main(String[] args) {
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);
        } catch (ParameterException e) {
            log.error(e);
            cmd.usage();
            System.exit(1);
        }

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            System.exit(0);
        }

        final CommandLineInterface cli = new CommandLineInterface(params);
        try {
            cli.bootstrapTable();
        } catch (InterruptedException e) {
            log.error("Interrupted when executing transfer.", e);
            System.exit(1);
        } catch (ExecutionException e) {
            log.error(ENCOUNTERED_EXCEPTION_WHEN_EXECUTING_TRANSFER, e);
            System.exit(1);
        } catch (SectionOutOfRangeException e) {
            log.error("Invalid section parameter", e);
            System.exit(1);
        } catch (Exception e) {
            log.error(ENCOUNTERED_EXCEPTION_WHEN_EXECUTING_TRANSFER, e);
            System.exit(1);
        }
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     */
    private static double calculateThroughput(
            TableDescription tableDescription, double throughputRatio,
            boolean read) {
        if (read) {
            return tableDescription.getProvisionedThroughput().getReadCapacityUnits() * throughputRatio;
        }
        return tableDescription.getProvisionedThroughput().getWriteCapacityUnits() * throughputRatio;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    private static ExecutorService getThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }
        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                maxWriteThreads, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(maxWriteThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

}
