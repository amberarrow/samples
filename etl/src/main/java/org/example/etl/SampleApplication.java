/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.example.etl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.sql.SQLExecEnvironment;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.apex.malhar.sql.table.StreamEndpoint;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;


@ApplicationAnnotation(name = "ETLExample")
public class SampleApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    try {
        // without this, the application fails at launch with this error:
        //   java.sql.SQLException: No suitable driver found for jdbc:calcite
        //
        Class.forName("org.apache.calcite.jdbc.Driver");
    } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
    }

    // register custom user defined functions (UDFs)
    final SQLExecEnvironment env = SQLExecEnvironment.getEnvironment();
    env.registerFunction("CALLTYPE", this.getClass(), "callType");
    env.registerFunction("COST", this.getClass(), "cost");

    final Map<String, Class> fieldMap = new HashMap<String, Class>(6);
    fieldMap.put("tstamp",      Date.class);
    fieldMap.put("id",          Integer.class);
    fieldMap.put("type",        String.class);
    fieldMap.put("origin",      String.class);
    fieldMap.put("destination", String.class);
    fieldMap.put("duration",    Integer.class);

    // Add Kafka Input
    KafkaSinglePortInputOperator input = dag.addOperator("KafkaInput", KafkaSinglePortInputOperator.class);
    input.setInitialOffset("EARLIEST");

    // Add CSVParser
    CsvParser parser = dag.addOperator("CSVParser", CsvParser.class);
    dag.addStream("KafkaToParser", input.outputPort, parser.in);

    // Register CSV Parser output as input table for SQL input endpoint
    env.registerTable(conf.get("inSchemaName"), new StreamEndpoint(parser.out, fieldMap));

    // Register FileEndpoint as output table for SQL output endpoint
    env.registerTable(conf.get("outSchemaName"), new FileEndpoint(conf.get("outputDir"),
        conf.get("fileName"), new CSVMessageFormat(conf.get("outputSchema"))));

    // Convert query to operators and streams and add to DAG
    env.executeSQL(dag, conf.get("query"));
  }

  public static String callType(String s) {
      if ("v".equals(s)) return "Voice";
      if ("d".equals(s)) return "Data";
      throw new RuntimeException(String.format("Error: Bad call type: %s", s));
  }
  public static String cost(String origin, String dest, int duration) {
      // can make this calculation more elaborate by using different unit costs based on
      // time of day, distance between origin and destination, etc.
      //
      final double centsPerSec = 0.2;
      return String.format("$%.2f", duration * centsPerSec);
  }
}
