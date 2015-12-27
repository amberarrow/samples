package com.example.myapexapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.alexmerz.graphviz.ParseException;
import com.alexmerz.graphviz.Parser;
import com.alexmerz.graphviz.objects.Edge;
import com.alexmerz.graphviz.objects.Graph;
import com.alexmerz.graphviz.objects.Id;
import com.alexmerz.graphviz.objects.Node;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.InputPort;

class NodeInfo {
  String nodeName;
  Class nodeClass;
  Operator nodeOperator;

  public NodeInfo(String name, Class clss, Operator op) {
    nodeName = name; nodeClass = clss; nodeOperator = op;
  }
}

/**
 * Build DAG from high-level description
 */
public class Builder {

  public static void build(final StringBuffer graphStr, final DAG dag) {
    Parser p = new Parser();
    try {
      p.parse(graphStr);
    } catch (ParseException e) {
      throw new RuntimeException("Error: Parser failed: " + e);
    }

    ArrayList<Graph> list = p.getGraphs();
    Graph g = list.get(0);  // only one graph

    try {
      // create nodes
      ArrayList<Node> nodes = g.getNodes(true);
      Map<String, NodeInfo> nodeMap = new HashMap<>();
      for (Node node : nodes) {
        String name = node.getId().getId();
        String className = node.getAttribute("class");
        Class nodeClass = Class.forName(className);
        Operator operator = dag.addOperator(name, nodeClass);
        NodeInfo info = new NodeInfo(name, nodeClass, operator);
        nodeMap.put(name, info);
      }

      // create streams
      ArrayList<Edge> edges = g.getEdges();
      for (Edge edge : edges) {
        Node src = edge.getSource().getNode(), tgt = edge.getTarget().getNode();
        String
          srcName = src.getId().getId(),
          tgtName = tgt.getId().getId(),
          srcPortName = edge.getAttribute("src"),
          tgtPortName = edge.getAttribute("tgt"),
          name = edge.getAttribute("id");

        NodeInfo srcInfo = nodeMap.get(srcName), tgtInfo = nodeMap.get(tgtName);

        Operator srcOp = srcInfo.nodeOperator, tgtOp = tgtInfo.nodeOperator;
        OutputPort outputPort
          = (OutputPort) srcInfo.nodeClass.getField(srcPortName).get(srcOp);
        InputPort inputPort
          = (InputPort) tgtInfo.nodeClass.getField(tgtPortName).get(tgtOp);
      
        dag.addStream(name, outputPort, inputPort);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }  // build
}
