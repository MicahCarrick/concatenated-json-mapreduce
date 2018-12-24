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

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Parses out top-level JSON objects from a string. The key is the offset in the input and the
 * value is the JSON as text.
 */
public class ConcatenatedJsonRecordReader extends RecordReader<LongWritable, Text> {
    private static final byte[] OPEN_CHR = "{".getBytes();
    private static final byte[] CLOSE_CHR = "}".getBytes();
    private static final byte[] QUOTE_CHR = "\"".getBytes();
    private static final byte[] ESCAPE_CHR = "\\".getBytes();
    private LongWritable key;
    private Text value;
    private long startByte;
    private long currentByte;
    private long lastByte;
    private FSDataInputStream input;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        Configuration config = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) split;
        final Path path = fileSplit.getPath();

        startByte = fileSplit.getStart();
        lastByte = fileSplit.getLength() - 1;

        FileSystem fs = path.getFileSystem(config);
        input = fs.open(fileSplit.getPath());
        input.seek(startByte);
    }

    @Override
    public synchronized void close() throws IOException {
        input.close();
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return Math.min(1.0f, (float)(input.getPos() - startByte) / ((lastByte + 1)- startByte));
    }

    @Override
    public boolean nextKeyValue() throws IOException {

        if (input.getPos() <= lastByte) {
            int depth = 0;
            byte[] nextChar = new byte[1];
            long objStart = input.getPos();
            boolean inQuotes = false;

            input.readFully(nextChar);

            if (!Arrays.equals(nextChar, OPEN_CHR)) {
                // expecting opening bracket for a JSON object
                throw new RuntimeException("Expecting JSON object at byte: " + objStart);
            }

            // read until the closing bracket of the JSON object
            while (input.getPos() <= lastByte) {

                input.readFully(nextChar);

                if (Arrays.equals(nextChar, ESCAPE_CHR)) {
                    // skip over escaped characters
                    input.seek((input.getPos()+1));
                } else if (Arrays.equals(nextChar, QUOTE_CHR)) {
                    // toggle whether or not position is inside a quoted string
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (Arrays.equals(nextChar, OPEN_CHR)) {
                        // begin nested object
                        depth++;
                    } else if (Arrays.equals(nextChar, CLOSE_CHR)) {
                        if (depth == 0) {
                            // found the end of the object
                            long objEnd = input.getPos();
                            int len = (int) (objEnd - objStart);
                            byte[] buffer = new byte[len];
                            input.readFully(objStart, buffer, 0, len);

                            value = new Text(buffer);
                            key = new LongWritable(objStart);

                            return true;
                        } else {
                            // end nested object
                            depth--;
                        }
                    }
                }
            }

            // expecting closing bracket for a JSON object
            throw new RuntimeException("Expecting JSON object at byte: " + objStart);
        }

        return false;
    }

}
