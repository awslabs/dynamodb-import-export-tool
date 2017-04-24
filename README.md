# DynamoDB Import Export Tool
The DynamoDB Import Export Tool is designed to perform parallel scans on the source table,
store scan results in a queue, then consume the queue by writing the items asynchronously to a destination table.

## Requirements ##
* Maven
* JRE 1.7+
* Pre-existing source DynamoDB tables. The destination table is optional in the CLI; you can choose to create the
destination table if it does not exist.

## Running as an executable
1. Build the library with `mvn install`. This produces the target jar in the target/ directory.
The CLI's usage follows with required parameters marked by asterisks.

```bash
    --consistentScan
      Use this flag to use strongly consistent scan. If the flag is not used 
      it will default to eventually consistent scan
      Default: false
    --copyStreamSpecificationWhenCreating
      Use the source table stream specification for the destination table 
      during its creation.
      Default: false
    --createAllGsi
      Create all GSI in destination table
      Default: false
    --createAllLsi
      Create all LSI in destination table
      Default: false
    --createDestination
      Create destination table if it does not exist
      Default: false
    --destinationEndpoint
      Endpoint of the destination table
  * --destinationSigningRegion
      Signing region for the destination endpoint
  * --destinationTable
      Name of the destination table
    --help
      Display usage information
    --includeGsi
      Include the following GSI in the destination table
    --includeLsi
      Include the following LSI in the destination table
    --maxWriteThreads
      Number of max threads to write to destination table
      Default: 1024
  * --readThroughputRatio
      Percentage of total read throughput to scan the source table
      Default: 0.0
    --section
      Section number to scan when running multiple programs concurrently [0, 
      1... totalSections-1]
      Default: 0
    --sourceEndpoint
      Endpoint of the source table
  * --sourceSigningRegion
      Signing region for the source endpoint
  * --sourceTable
      Name of the source table
    --totalSections
      Total number of sections to divide the scan into
      Default: 1
  * --writeThroughputRatio
      Percentage of total write throughput to write the destination table
      Default: 0.0
```

2. An example command you can use on one EC2 host to copy from one table `foo` in `us-east-1` to a new table
called `bar` in `us-east-2` follows.

```bash
java -jar target/dynamodb-import-export-tool-1.1.0.jar \
--sourceRegion us-east-1 \
--sourceTable foo \
--destinationRegion us-east-2 \
--destinationTable bar \
--readThroughputRatio 1 \
--writeThroughputRatio 1
```

> **NOTE**: To split the replication process across multiple machines, simply use the totalSections & section
command line arguments, where each machine will run one section out of [0 ... totalSections-1].

## Using the API
Find some examples of how to use the Import-Export tool's API below.
The first demonstrates how to use the API to copy data from one DynamoDB table to another.
The second demonstrates how to enqueue the data in a DynamoDB table in a
`BlockingQueueConsumer` in memory.

### 1. Transfer Data from One DynamoDB Table to Another DynamoDB Table

The below example will read from "mySourceTable" at 100 reads per second, using four threads.
And it will write to "myDestinationTable" at 50 writes per second, using eight threads.
Both tables are located at "dynamodb.us-west-1.amazonaws.com".
To transfer to a different region, create two AmazonDynamoDBClients
with different endpoints to pass into the DynamoDBBootstrapWorker and the DynamoDBConsumer.

```java
import DynamoDBBootstrapWorker;
import com.amazonaws.dynamodb.bootstrap.consumer.DynamoDBConsumer;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

class TransferDataFromOneTableToAnother {
    public static void main(String[] args) {
        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(com.amazonaws.regions.Regions.US_WEST_1).build();
        try {
            // 100.0 read operations per second. 4 threads to scan the table.
            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(client,
                    100.0, "mySourceTable", 4);
            // 50.0 write operations per second. 8 threads to scan the table.
            final DynamoDBConsumer consumer = new DynamoDBConsumer(client, "myDestinationTable",
                    50.0, Executors.newFixedThreadPool(8));
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
```


### 2. Transfer Data From one DynamoDB Table to a Blocking Queue.

The below example will read from a DynamoDB table and export to an array blocking queue.
This is useful for when another application would like to consume
the DynamoDB entries but does not have a setup application for it.
They can just retrieve the queue (consumer.getQueue()) and then continually pop() from it
to then process the new entries.

```java
import com.amazonaws.dynamodb.bootstrap.consumer.BlockingQueueConsumer;
import DynamoDBBootstrapWorker;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.util.concurrent.ExecutionException;

class TransferDataFromOneTableToBlockingQueue {
    public static void main(String[] args) {
        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(com.amazonaws.regions.Regions.US_WEST_1).build();
        try {
            // 100.0 read operations per second. 4 threads to scan the table.
            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(client, 100.0,
                    "mySourceTable", 4);
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
```
