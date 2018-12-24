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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;


public class ConcatenatedJsonRecordReaderTest {

    private ClassLoader classLoader = getClass().getClassLoader();
    private Configuration conf = new Configuration();
    private TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    @Test
    void findsSingleRecord() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("single-object.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);

        int recordCount = 0;
        while (rr.nextKeyValue()) {
            recordCount++;
        }

        assertEquals(1, recordCount);

        rr.close();
    }

    @Test
    void findsMulitipleRecords() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("multiple-objects.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);

        Text obj = new Text("{\"foo\": \"bar\"}");
        int recordCount = 0;
        while (rr.nextKeyValue()) {
            assertEquals(obj, rr.getCurrentValue());
            assertEquals((recordCount * obj.getLength()), rr.getCurrentKey().get());
            recordCount++;
        }

        assertEquals(10, recordCount);

        rr.close();
    }

    @Test
    void allowsNestedObjects() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("nested-object.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);
        rr.nextKeyValue();

        Text obj = new Text("{\"children\": [{\"one\": 1, \"two\": 2, \"three\": {\"a\": [1, 2, 3]}}]}");
        assertEquals(obj, rr.getCurrentValue());

        rr.close();
    }

    @Test
    void skipsEscapedCharacters() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("escaped-object.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);
        rr.nextKeyValue();

        Text obj = new Text("{\"name\": \"This \\\"quoted\\\" value is properly escaped.\"}");
        assertEquals(obj, rr.getCurrentValue());

        rr.close();
    }

    @Test
    void reportsProgress() throws IOException, InterruptedException {
        // multiple-objects.txt has 10 records each of the same size
        FileSplit split = getSplitForResource("multiple-objects.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);

        int recordCount = 0;
        while (rr.nextKeyValue()) {
            recordCount++;
            assertEquals(recordCount/10.0, rr.getProgress(), 0.000001);
        }

        rr.close();
    }

    @Test
    void isBoundedBySplitOffsets() throws IOException, InterruptedException {
        // get 3rd and 4th objects of file containing 10 objects
        Text obj = new Text("{\"foo\": \"bar\"}");
        long offset = obj.getLength();
        long length = offset + (obj.getLength() * 2);

        FileSplit split = getSplitForResource("multiple-objects.txt", offset, length);
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);

        int recordCount = 0;
        while (rr.nextKeyValue()) {
            assertEquals(obj, rr.getCurrentValue());
            assertEquals((offset + (recordCount * obj.getLength())), rr.getCurrentKey().get());
            recordCount++;
        }

        assertEquals(2, recordCount);

        rr.close();
    }


    @Test
    void throwsErrorOnNonJson() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("not-json.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);
        assertThrows(RuntimeException.class, () -> {
            rr.nextKeyValue();
        });

        rr.close();
    }

    @Test
    void throwsErrorOnMalformedJson() throws IOException, InterruptedException {
        FileSplit split = getSplitForResource("malformed-json.txt");
        ConcatenatedJsonRecordReader rr = new ConcatenatedJsonRecordReader();
        rr.initialize(split, context);
        assertThrows(RuntimeException.class, () -> {
            rr.nextKeyValue();
        });

        rr.close();
    }

    FileSplit getSplitForResource(String resourceName) {
        return getSplitForResource(resourceName, 0, -1);
    }

    FileSplit getSplitForResource(String resourceName, long offset, long length) {
        String fileName = classLoader.getResource(resourceName).getFile();
        File file = new File(fileName);
        Path path = new Path(fileName);
        if (length == -1) {
            length = file.length();
        }
        return new FileSplit(path, offset, length, null);
    }
}