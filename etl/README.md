## ETL Example

This example Apex application illustrates how to create and run a pipeline that performs
many of the operations common to ETL applications. It uses the Apex integration module for
Apache Calcite integration thus reducing the amount of code to a bare minimum.

### Overview
The application reads CDR (Call Detail Record) data from a Kafka topic, filters it to select
records from one area code to another, enriches it by computing the cost of each call based
on the duration and writes the resulting data to an output file.

### Configuration
Various properties can be configured in the file `properties.xml` file including the Kafka
topic for input, the SQL used for the various transformations, the format of input and
output records, and the directory and file name for the output. Make sure
the Kafka broker and topic are correctly configured for your environment.

### Operation
To build the application, simply run `mvn clean package -DskipTests`. To deploy it, use
one of the methods documented at [Apex docs](http://apex.apache.org/docs.html). Populate
the topic with the content of the file `etl-input.csv`. After the application fires up
and processes the data from Kafka, you should see an output file in the HDFS directory
`/tmp/ETLOutput` with content similar to this:
```
------------------------------------------------------------------------------------
12/10/2017 05:25:30 -0700,Voice,111-123-4567,999-987-6543,$120.00

15/10/2017 03:15:00 -0700,Data,111-222-3333,999-187-7654,$369.00

------------------------------------------------------------------------------------
```

### Input and output record format
The input records look like this:
```
13/10/2017 11:45:30 +0000,1,v,111-123-4567,222-987-6543,120
```
The fields represent: timestamp, unique record id, type of call (`v` for voice, `d` for data),
origin and destination numbers and call duration in seconds.

The output record format is similar: timestamp, expanded type, origin and destination
numbers and cost in dollars and cents. Only calls originating in the 111 area code and
going to the 999 area code are selected.
