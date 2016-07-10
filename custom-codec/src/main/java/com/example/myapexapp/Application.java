package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;

@ApplicationAnnotation(name="TestCustomStreamCodec")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomGenerator random = dag.addOperator("sequence", RandomGenerator.class);
    TestPartition testPartition = dag.addOperator("testPartition", TestPartition.class);
    MyCodec codec = new MyCodec();
    dag.setInputPortAttribute(testPartition.in, PortContext.STREAM_CODEC, codec);

    //Add locality if needed, e.g.: .setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("randomData", random.out, testPartition.in);
  }
}
