Concatenated JSON Input Format and Record Reader
================================================

Custom `FileInputFormat` and `RecordReader` classes to handle "concatenated JSON" also known as "streaming JSON" in Hadoop MapReduce.

Concatenated JSON files contain JSON objects without any delimiter and are not within a top-level JSON array. This format is commonly found in persistent file-based representations of streaming JSON.

```json
{"one": 1}{"two": 2}{"three": 3}
```

Note that this IS NOT newline separated JSON objects.


## Basic Usage

Import the `ConcatenatedJsonInputFormat` class:
```java
import tech.carrick.mapreduce.ConcatenatedJsonInputFormat;
```

Call `setInputFormatClass` on the MapReduce job instance to set the input format to `ConcatenatedJsonInputFormat`:

```java
// job is an instance of org.apache.hadoop.mapreduce.Job
job.setInputFormatClass(ConcatenatedJsonInputFormat.class);
```

A simple example can be found in `src/java/tech/carrick/mapreduce/ConcatenatedJsonExample`.


## Build

This project is not currently using a build system but it's simple enough that it can be built the old-school way. The following will compile and package the example application into a JAR to be run on specific Hadoop version.

Of course you can always build with the IDE or build system of your choice. Notes on setting up IntelliJ IDEA are described in the "Development" section of this README.

---

Locate the root directory into which Hadoop is installed. In this example I have downloaded Hadoop 2.8.5 and extracted it to `/opt/hadoop2.8.5`. Export this path into an environment variable named `HADOOP_DIR`.

```bash
$ export HADOOP_DIR="/opt/hadoop-2.8.5"
```
Create a directory into which to write the compiled files and build artifacts. Then change into that directory:

```bash
$ mkdir out
$ cd out
```

Compile the Java sources with dependency Hadoop jars on the class path:

```bash
$ javac -d . \
-cp .:$HADOOP_DIR/share/hadoop/common/*:$HADOOP_DIR/share/hadoop/common/lib/*:$HADOOP_DIR/share/hadoop/mapreduce/* \
../src/java/tech/carrick/mapreduce/*.java
```

Create JAR package:

```bash
$ jar -cvfm concatenated-json-mapreduce.jar \
../src/META-INF/MANIFEST.MF tech/carrick/mapreduce/*.class
```

Verify the JAR contents:

```bash
$ jar tf concatenated-json-mapreduce.jar
META-INF/
META-INF/MANIFEST.MF
tech/carrick/mapreduce/ConcatenatedJsonExample.class
tech/carrick/mapreduce/ConcatenatedJsonInputFormat.class
tech/carrick/mapreduce/ConcatenatedJsonRecordReader.class
```


## Example Application

After following the build instructions above, run the example application in the JAR on your Hadoop cluster where the input file is the first argument and the output directory is the second argument.

Create a test file and put to HDFS:

```bash
$ echo -n '{"one": 1}{"two": 2}{"three": 3}' > input.txt
$ hadoop fs -put input.txt /user/hadoop/
```

Run the example:

```bash
$ hadoop jar concatenated-json-mapreduce.jar /user/hadoop/input.txt /user/hadoop/output/
```

Check the output:

```bash
$ hadoop fs -cat /user/hadoop/output/part-r-*
0	{"one": 1}
10	{"two": 2}
20	{"three": 3}
```

## Development

### IntelliJ IDEA

This project is not currently using a build system. Development, testing, and building JAR files has been done with [IntelliJ IDEA](https://www.jetbrains.com/idea/).
