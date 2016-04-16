package com.example.myapexapp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class KafkaApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator in = dag.addOperator("in", new KafkaSinglePortInputOperator());
    in.setInitialPartitionCount(1);
    in.setTopics("test");
    in.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    in.setClusters("localhost:2181");

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("data", in.outputPort, console.input);

    /*
    LineOutputOperator out = dag.addOperator("out", new LineOutputOperator());
    out.setFilePath("/tmp/FromKafka");
    out.setFileName("test");
    out.setMaxLength(1024);        // max size of rolling output file

    // create stream connecting input adapter to output adapter
    dag.addStream("data", in.outputPort, out.input);
    */
  }
}

/**
 * Converts each tuple to a string and writes it as a new line to the output file
 */
class LineOutputOperator extends AbstractFileOutputOperator<byte[]>
{
  private static final String NL = System.lineSeparator();
  private static final Charset CS = StandardCharsets.UTF_8;
  private String fileName;

  @Override
  public byte[] getBytesForTuple(byte[] t) { return (new String(t, CS) + NL).getBytes(CS); }

  @Override
  protected String getFileName(byte[] tuple) { return fileName; }

  public String getFileName() { return fileName; }
  public void setFileName(final String v) { fileName = v; }
}
