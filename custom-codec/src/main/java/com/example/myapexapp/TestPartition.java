package com.example.myapexapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import javax.validation.ConstraintViolation;
import javax.validation.ValidatorFactory;
import javax.validation.Validator;
import javax.validation.Validation;

import com.datatorrent.api.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.Partitioner.PartitioningContext;

import com.datatorrent.common.util.BaseOperator;

/**
 * Simple operator to test partitioning
 */
public class TestPartition extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(TestPartition.class);

  private transient int id;             // operator/partition id
  private transient long curWindowId;   // current window id
  private transient long cnt;           // per-window tuple count

  public final transient DefaultInputPort<String> in = new DefaultInputPort<String>() {
    @Override
    public void process(String tuple)
    {
      LOG.debug("{}: tuple = {}, operator id = {}", cnt, tuple, id);
      ++cnt;
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    long appWindowId = context.getValue(context.ACTIVATION_WINDOW_ID);
    id = context.getId();
    LOG.debug("Started setup, appWindowId = {}, operator id = {}", appWindowId, id);
  }

  @Override
  public void beginWindow(long windowId)
  {
    cnt = 0;
    curWindowId = windowId;
    LOG.debug("window id = {}, operator id = {}", curWindowId, id);
  }

  @Override
  public void endWindow()
  {
    LOG.debug("window id = {}, operator id = {}, cnt = {}", curWindowId, id, cnt);
  }

}

