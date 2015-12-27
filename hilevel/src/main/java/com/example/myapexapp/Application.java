/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    final String s =
      "digraph G {\n" +
      "randGen [class=\"com.example.myapexapp.RandomNumberGenerator\"];\n" +
      "console [class=\"com.datatorrent.lib.io.ConsoleOutputOperator\"];\n" +
      "randGen -> console [id=randomData, src=out, tgt=input]\n" +
      "}";

    StringBuffer sb = new StringBuffer(s);
    Builder.build(sb, dag);
  }  // populateDAG

}
