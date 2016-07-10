package com.example.myapexapp;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class MyCodec extends KryoSerializableStreamCodec<String> {
    @Override
    public int getPartition(String tuple) {
      String[] fields = tuple.split("\\^");
      return fields[1].hashCode();
    }
}
