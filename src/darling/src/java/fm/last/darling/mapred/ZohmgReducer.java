/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package fm.last.darling.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;

import fm.last.darling.hbase.HBaseUtils;
import fm.last.darling.io.records.NSpacePoint;
import fm.last.darling.nspace.Projection;

public class ZohmgReducer extends Reducer<NSpacePoint, IntWritable, ImmutableBytesWritable, Put> {
  public void reduce(NSpacePoint point, Iterator<IntWritable> values,
      OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter) throws IOException {
    // sum the values.
    int sum = 0;
    while (values.hasNext())
      sum += values.next().get();

    byte[] n = Integer.valueOf(sum).toString().getBytes();
    // or, if you'd rather like to store byte-y ints:
    // byte[] n = Util.intToByteArray(sum);

    // HBase stuff.
    byte[] rowkey = HBaseUtils.formatRowkey(point.getUnit(), point.getTimestamp());
    ImmutableBytesWritable rk = new ImmutableBytesWritable(rowkey);
    Put put = new Put(rowkey);

    Projection p = new Projection(point);
    
    String cfq = p.toHBaseCFQ();
    
    String[] parts = cfq.split(":");
    
    String family    = parts[0];
    String qualifier = parts[1];
    
    put.add(family.getBytes("UTF-8"), qualifier.getBytes("UTF-8"), n);
    // dispatch to HBase.
    output.collect(rk, put);
  }
}
