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
package fm.last.darling;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import fm.last.darling.io.records.NSpacePoint;
import fm.last.darling.mapred.MapperWrapper;
import fm.last.darling.mapred.ZohmgCombiner;
import fm.last.darling.mapred.ZohmgReducer;

public class ZohmgProgram {
  public static final JobPriority DEFAULT_JOB_PRIORITY = JobPriority.NORMAL;

  public int start(String input) throws Exception {
    Path path = new Path(input);

    // TODO: read table/dataset from environment.
    String table = "zohmg";

    Job job = new Job();
    
    job.setJobName("zohmg!");
    FileInputFormat.addInputPath(job, path);

    Path output = new Path("yeah");
    FileOutputFormat.setOutputPath(job, output);

    // input
    job.setInputFormatClass(TextInputFormat.class);
    // wrapper
    job.setMapperClass(MapperWrapper.class);
    job.setMapOutputKeyClass(NSpacePoint.class);
    job.setMapOutputValueClass(IntWritable.class);
    // output
    job.setCombinerClass(ZohmgCombiner.class);
    job.setReducerClass(ZohmgReducer.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    //job.set(TableOutputFormat.OUTPUT_TABLE, table);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
