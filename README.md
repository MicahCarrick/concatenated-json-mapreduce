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

This project is not currently using a build system. Development, testing, and building JAR files was done directly in [IntelliJ IDEA](https://www.jetbrains.com/idea/).


#### Add Hadoop Dependencies

JARs from the following directories need to be added to your project where the `/share` path is relative to your Hadoop installation root. For example it may be `/usr/share` or `/opt/hadoop/share`, etc.

* `/share/hadoop/common/*`
* `/share/hadoop/common/lib/*`
* `/share/hadoop/mapreduce/*`

To add these JARs to your IntelliJ project:

1. Press `CTRL+ALT+SHIFT+S` to bring up the **Project Structure** dialog
2. Select **Modules** from the list on the left-hand side
3. Select the **Dependencies** tab from the right pane
4. Click the plus (+) icon in the toolbar on the right-hand side
5. Select **JARs or Directories...**
6. Navigate to the path containing the JARs as indicated above and select all of the JARs within that path
7. Repeat of each path containing dependency JARs


#### Example Application Run Configuration

The example application can be run directly in IntelliJ from the local filesystem.

1. From the top menu select **Run** > **Edit Configurations**
2. Select the plus (+) icon in the top toolbar and select **Application**
3. Set **Main class** to `tech.carrick.mapreduce.ConcatenatedJsonExample`
4. Set **Program Arguments** to the input file and output directory on the local filesystem. For example: `test/resources/multiple-objects.txt /tmp/example-output/`
5. Add your `HADOOP_HOME` to **Environment variables**. For example: `/etc/hadoop` or `/opt/hadoop2.8.5/etc/hadoop` depending on where Hadoop is installed on your local environment.


#### Record Reader Test Configuration

The bulk of the logic is in the record reader code. To run the tests in `ConcatenatedJsonRecordReaderTest` create a JUnit run configuration in IntelliJ.

1. From the top menu select **Run** > **Edit Configurations**
2. Select the plus (+) icon in the top toolbar and select **JUnit**
3. Set **Class** to `tech.carrick.mapreduce.ConcatenatedJsonRecordReaderTest`
