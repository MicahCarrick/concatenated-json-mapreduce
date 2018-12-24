/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package tech.carrick.mapreduce;

//import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple Hadoop MapReduce example to demonstrate the concatenated JSON input format.
 */
public class ConcatenatedJsonExample extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {

        //BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new ConcatenatedJsonExample(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ConcatenatedJsonExample.class);

        // Use the Concatenated JSON input format
        job.setInputFormatClass(ConcatenatedJsonInputFormat.class);

        // Input file is first argument
        FileInputFormat.setInputPaths(job, args[0]);

        // Output directory is second argument. It must not already exist.
        Path path = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, path);


        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }
}
