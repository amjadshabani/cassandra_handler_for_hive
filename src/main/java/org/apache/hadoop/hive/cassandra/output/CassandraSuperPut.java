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

package org.apache.hadoop.hive.cassandra.output;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This represents the super column family in Cassandra.
 *
 */
public class CassandraSuperPut extends CassandraAbstractPut implements Writable {
  private ByteBuffer key;
  private List<CassandraPut> subColumns;

  public CassandraSuperPut(){
    subColumns = new ArrayList<CassandraPut>();
  }

  public CassandraSuperPut(ByteBuffer key){
    this();
    this.key = key;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int keyLen = in.readInt();
    byte[] keyBytes = new byte[keyLen];
    in.readFully(keyBytes);
    key = ByteBuffer.wrap(keyBytes);
    int ilevel = in.readInt();
    int cols = in.readInt();
    for (int i =0;i<cols;i++){
      CassandraPut cc = new CassandraPut();
      cc.readFields(in);
      subColumns.add(cc);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(key.remaining());
    out.write(ByteBufferUtil.getArray(key));
    out.writeInt(1);
    out.writeInt(subColumns.size());
    for (CassandraPut c: subColumns){
      c.write(out);
    }
  }

  public ByteBuffer getKey() {
    return key;
  }

  public void setKey(ByteBuffer key) {
    this.key = key;
  }

  public List<CassandraPut> getColumns() {
    return subColumns;
  }

  public void setSubColumns(List<CassandraPut> subColumns) {
    this.subColumns = subColumns;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("key: ");
    sb.append(this.key);
    for (CassandraPut col:this.subColumns){
      sb.append( "SubColumn : [" );
      sb.append( col.toString() );
      sb.append( "]" );
    }
    return sb.toString();
  }

  @Override
  public void write(String keySpace, CassandraProxyClient client, JobConf jc) throws IOException {
    ConsistencyLevel flevel = getConsistencyLevel(jc);
    int batchMutation = getBatchMutationSize(jc);
    Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
    Map<String, List<Mutation>> maps = new HashMap<String, List<Mutation>>();

    int count = 0;
    for (CassandraPut c : subColumns) {
      List<Column> columns = new ArrayList<Column>();
      for (CassandraColumn col : c.getColumns()) {
        Column cassCol = new Column();
        cassCol.setValue(col.getValue());
        cassCol.setTimestamp(col.getTimeStamp());
        cassCol.setName(col.getColumn());
        columns.add(cassCol);

        ColumnOrSuperColumn thisSuperCol = new ColumnOrSuperColumn();
        thisSuperCol.setSuper_column(new SuperColumn(c.getKey(), columns));

        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(thisSuperCol);

        List<Mutation> mutList = maps.get(col.getColumnFamily());
        if (mutList == null) {
          mutList = new ArrayList<Mutation>();
          maps.put(col.getColumnFamily(), mutList);
        }

        mutList.add(mutation);

        count ++;

        if (count == batchMutation) {
          mutation_map.put(key, maps);

          commitChanges(keySpace, client, flevel, mutation_map);

          //reset mutation map, maps and count;
          mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
          maps = new HashMap<String, List<Mutation>>();
          count = 0;
        }

      }
    }

    if(count > 0) {
      mutation_map.put(key, maps);
      commitChanges(keySpace, client, flevel, mutation_map);
    }
  }
}
