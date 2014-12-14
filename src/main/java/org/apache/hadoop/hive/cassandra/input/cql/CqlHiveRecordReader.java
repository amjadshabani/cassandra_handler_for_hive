/**
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.cassandra.input.cql;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import org.apache.cassandra.hadoop.cql3.CqlRecordReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CqlHiveRecordReader extends RecordReader<VLongWritable, MapWritable>
        implements org.apache.hadoop.mapred.RecordReader<VLongWritable, MapWritable> {

  static final Logger LOG = LoggerFactory.getLogger(CqlHiveRecordReader.class);

  private final CqlRecordReader crr;

  public CqlHiveRecordReader(CqlRecordReader crr) { //, boolean isTransposed) {
    this.crr = crr;
  }

  @Override
  public void close() throws IOException {
    crr.close();
  }

  @Override
  public VLongWritable createKey() {
    return new VLongWritable(crr.createKey());
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
    };

  @Override
  public long getPos() throws IOException {
    return crr.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return crr.getProgress();
  }

  public static int callCount = 0;

  @Override
  public boolean next(VLongWritable key, MapWritable value) throws IOException {
    if (!nextKeyValue())
      return false;

    key.set(crr.getCurrentKey());
    value.clear();
    value.putAll(getCurrentValue());

    return true;
  }

  @Override
  public VLongWritable getCurrentKey() {
    return new VLongWritable(crr.getCurrentKey());
  }

  @Override
  public MapWritable getCurrentValue() {
    MapWritable new_value = createValue();
    for (ColumnDefinitions.Definition column : crr.getCurrentValue().getColumnDefinitions()){
      if(crr.getCurrentValue().getBytesUnsafe(column.getName())!=null)
        new_value.put(new Text(column.getName()),
                new BytesWritable(Bytes.getArray(crr.getCurrentValue().getBytesUnsafe(column.getName()))));

    }

    return new_value;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    crr.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return crr.nextKeyValue();
  }

  private MapWritableComparable mapToMapWritable(Map<String, ByteBuffer> map) {
    MapWritableComparable mw = new MapWritableComparable();
    for (Map.Entry<String, ByteBuffer> e : map.entrySet()) {
      if(e.getValue()!=null)
        mw.put(new Text(e.getKey()), convertByteBuffer(e.getValue()));
    }
    return mw;
  }

}
