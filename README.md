# 📘 Big Data Analytics (BDA) — MID-II Question Bank Answers
### Units 3, 4 & 5 | Part-B Long Answer Questions (5 Marks Each)

---

> **Subject:** Big Data Analytics (BDA) | **Year & Sem:** IV – I  
> **Branch:** CSM | **Regulation:** R23 | **Academic Year:** 2026–27  
> **Faculty:** Mr. K. Ramya | **Dept:** Allied Computer Science & Engineering

---

## 📋 Table of Contents

### 🔵 Unit III – Writing MapReduce Programs
| Q.No | Question |
|------|----------|
| Q1 & Q2 | Processing weather data using MapReduce |
| Q3 | Old vs New Hadoop API comparison |
| Q4 | Role of Driver class with example |
| Q5 | Mapper program for weather dataset |
| Q6 | Reducer program – maximum temperature |
| Q7 | Interaction between Driver, Mapper, Reducer |
| Q8 | Record Reader – functions & key-value conversion |
| Q9 | Combiner – purpose & performance improvement |
| Q10 | Partitioner – role & custom partitioning |
| Q11 | MapReduce solution for average temperature |
| Q12 | Complete execution flow of a MapReduce job |
| Q13 | Complete MapReduce program structure |

### 🟢 Unit IV – Stream Memory and Spark
| Q.No | Question |
|------|----------|
| Q1 | Stream Data Model and Architecture |
| Q2 | Stream Computing with example |
| Q3 | Sampling data in a stream |
| Q4 & Q5 | Reservoir Sampling working |
| Q6 & Q7 | Stream Filtering techniques & Bloom Filter |
| Q8 & Q9 | Counting Distinct Elements in Stream |
| Q10 | Apache Spark vs Hadoop MapReduce |
| Q11 | Spark Architecture and components |
| Q12 | Spark Installation and Configuration |
| Q13 | Spark RDD – concept, features, advantages |
| Q14 | Narrow vs Wide Transformations in Spark RDD |
| Q15 | Spark RDD Operations: map, filter, reduce, collect, flatMap |

### 🟡 Unit V – Pig and Hive
| Q.No | Question |
|------|----------|
| Q1 | Apache Pig Architecture and components |
| Q2 | Pig Latin Application Flow |
| Q3 | ABCs of Pig Latin – basic concepts |
| Q4 | Local Mode vs Distributed Mode in Pig |
| Q5 | Pig Script Interfaces |
| Q6 | Writing and Executing a Pig Latin Script |
| Q7 | Pig Latin Data Model and Data Types |
| Q8 | Apache Hive Architecture and components |
| Q9 & Q10 | Introduction to Hive & Getting Started |
| Q11 | Hive Data Types with examples |
| Q12 | Creating and Managing Databases and Tables in Hive |
| Q13 | Hive DML with examples |
| Q14 | HiveQL – Querying and Analyzing Data |
| Q15 | Hive Clients with examples |

---


---

# UNIT III – Writing MapReduce Programs

---

## Q1 & Q2. Describe the steps involved in processing weather data using the MapReduce framework.

**MapReduce** is a programming model used for processing and generating large datasets in a parallel, distributed manner across a cluster.

**Weather Dataset Overview:**
The classic weather dataset (from NCDC – National Climatic Data Center) consists of semi-structured flat ASCII text logs collected hourly by global weather sensors. It records temperature, humidity, rainfall, wind speed, atmospheric pressure, and timestamps from different weather stations.

**Steps in MapReduce Processing:**

**Step 1 – Input Splitting:**
The large weather dataset is divided into smaller input splits. Each split is processed by a separate mapper independently.

**Step 2 – Map Phase:**
Each mapper processes one input split, extracts the required fields, and emits key-value pairs.

*Example:*
```
Input:
Hyderabad, 2025-01-01, 30°C
Hyderabad, 2025-01-02, 32°C
Delhi, 2025-01-01, 18°C

Mapper Output:
(Hyderabad, 30)
(Hyderabad, 32)
(Delhi, 18)
```

**Step 3 – Shuffle and Sort Phase:**
The framework groups all values having the same key. Data is sorted automatically before being sent to reducers.
```
Hyderabad → [30, 32]
Delhi → [18]
```

**Step 4 – Reduce Phase:**
The reducer performs computations (average, max, min, total) for each location.
```
Reducer Output:
Hyderabad → Average Temperature = 31°C
Delhi → Average Temperature = 18°C
```

**Step 5 – Output:**
Final output is stored in HDFS.

**Applications:** Calculating average/max/min temperatures, analyzing rainfall patterns, climate trend detection.

---

## Q3. Compare the Old Hadoop API and the New Hadoop API used in the MapReduce framework.

The primary difference between the Old and New Hadoop MapReduce APIs is that the **Old API** relies heavily on Java interfaces, while the **New API** favors abstract classes and unifies system communications into a single Context object.

| Feature | New API (`mapreduce`) | Old API (`mapred`) |
|---|---|---|
| **Package** | `org.apache.hadoop.mapreduce` | `org.apache.hadoop.mapred` |
| **Mapper & Reducer** | Abstract classes | Interfaces |
| **Communication** | Single `Context` object | Separate `OutputCollector` and `Reporter` |
| **Job Control** | `Job` class | `JobClient` class |
| **Job Configuration** | `Configuration` class | `JobConf` class (extends Configuration) |
| **Output File Names** | `part-m-nnnnn` (map), `part-r-nnnnn` (reduce) | `part-nnnnn` (both) |
| **reduce() values** | `java.lang.Iterable` | `java.lang.Iterator` |
| **Mapper/Reducer execution control** | Both can override `run()` method | Only Mapper via `MapRunnable`; no equivalent for Reducer |
| **Availability** | Modern standard (Hadoop 0.21+) | Maintained for backward compatibility |

**Key Takeaway:** The New API is more flexible, modular, and extensible without breaking older implementations.

---

## Q4. Explain the role of the Driver class in a Hadoop MapReduce program with a suitable example.

The **Driver code** runs on the client machine and is responsible for building the configuration of the job and submitting it to the Hadoop Cluster.

**Responsibilities of Driver:**
1. Accepts command-line arguments (input/output paths from HDFS)
2. Creates a `JobConf` (or `Job`) object to configure the MapReduce job
3. Specifies Mapper, Reducer, Combiner, Partitioner classes
4. Sets input/output key-value types
5. Submits the job to the cluster

**Sample Driver Code:**
```java
public class MyDriver {
    public static void main(String[] args) throws Exception {
        // Create the JobConf object
        JobConf conf = new JobConf(MyDriver.class);
        // Set the name of the Job
        conf.setJobName("SampleJobName");
        // Set output Key type for the Mapper
        conf.setMapOutputKeyClass(Text.class);
        // Set output Value type for the Mapper
        conf.setMapOutputValueClass(IntWritable.class);
        // Set output Key type for the Reducer
        conf.setOutputKeyClass(Text.class);
        // Set output Value type for the Reducer
        conf.setOutputValueClass(IntWritable.class);
        // Set Mapper and Reducer classes
        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);
        // Set Input and Output paths
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        // Submit the Job
        JobClient.runJob(conf);
    }
}
```

**Note:** The output path should not exist before execution; it is created after successful run.

---

## Q5. Write and explain a Mapper program for processing weather dataset records in Hadoop.

The **Mapper** processes each input record and emits intermediate key-value pairs.

**Role of Mapper:**
- Reads input data record by record
- Extracts the station name and temperature
- Emits `<station, temperature>` key-value pairs

**Mapper Code (New API):**
```java
public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text station = new Text();
    private IntWritable temperature = new IntWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length >= 3) {
            station.set(fields[0].trim());
            int temp = Integer.parseInt(fields[2].trim().replace("°C", ""));
            temperature.set(temp);
            context.write(station, temperature);
        }
    }
}
```

**Input:** `Hyderabad, 2025-01-01, 30`
**Output:** `(Hyderabad, 30)`

The Mapper's output is then passed to the Shuffle and Sort phase before reaching the Reducer.

---

## Q6. Develop a Reducer program to calculate the maximum temperature from a weather dataset and explain its working.

The **Reducer** takes the sorted and grouped key-value pairs from the shuffle phase and computes the final result.

**Reducer Code:**
```java
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;
        for (IntWritable val : values) {
            if (val.get() > maxTemp) {
                maxTemp = val.get();
            }
        }
        result.set(maxTemp);
        context.write(key, result);
    }
}
```

**Working:**
1. Receives key = station name, values = list of temperatures
2. Iterates through all temperature values for each station
3. Finds the maximum temperature
4. Writes `<station, maxTemperature>` to output

**Example:**
- Input: `Hyderabad → [30, 32, 28, 35]`
- Output: `Hyderabad → 35`

---

## Q7. Explain the interaction between Driver, Mapper, and Reducer classes during MapReduce execution.

**Execution Flow:**

```
Driver → [configures job] → Submits to Cluster
         ↓
Input Splits → Mapper(s) → [key-value pairs]
                              ↓
                         Shuffle & Sort
                              ↓
                         Reducer(s) → Output (HDFS)
```

1. **Driver:** Configures the job (sets Mapper, Reducer, input/output paths, data types) and submits it using `JobClient.runJob()`.
2. **Mapper:** Receives input splits from the InputFormat via RecordReader. Processes each record and emits `<key, value>` pairs.
3. **Shuffle and Sort:** The framework groups all values by the same key and sorts them before sending to Reducers.
4. **Reducer:** Receives sorted grouped `<key, list<values>>` pairs. Processes each group and writes final output.

The Driver acts as the orchestrator; Mapper performs transformation; Reducer performs aggregation. Together they implement the divide-and-conquer strategy for distributed data processing.

---

## Q8. Describe the functions of Record Reader in Hadoop MapReduce and explain how it converts input data into key-value pairs.

**Record Reader** is the first phase component of MapReduce that reads data from an InputSplit and converts it into key-value pairs suitable for the Mapper.

**Functions of Record Reader:**
1. Reading input data from the InputSplit
2. Converting raw input data into key-value pairs
3. Providing one record at a time to the Mapper
4. Identifying the key and value from each input record
5. Converting data into a format the Mapper can process
6. Passing key-value pairs to the Mapper

**How it works:**
- For text files (TextInputFormat), it reads line by line
- Key = byte offset of the line in the file (LongWritable)
- Value = content of the line (Text)

**Example:**
```
Input File:
Line 1: What do you mean by Object
Line 2: What do you know about Java

Record Reader Output:
<1, "What do you mean by Object">
<2, "What do you know about Java">
```

The Mapper then processes each `<key, value>` pair produced by the Record Reader.

---

## Q9. Explain the purpose of a Combiner in Hadoop MapReduce. How does it improve performance?

**Combiner** is an optional mini-Reducer that runs locally on the mapper node before data is sent to the Reducer. It reduces the volume of data shuffled across the network.

**Functions of Combiner:**
1. Performs local aggregation of Mapper output
2. Combines similar keys and their values on the Mapper side
3. Reduces data transferred from Mapper to Reducer
4. Reduces network traffic during Shuffle phase
5. Works as a mini-Reducer on the Mapper side

**How Combiner improves performance:**
- Without Combiner: All intermediate key-value pairs are sent across network
- With Combiner: Locally aggregated, fewer key-value pairs sent

**Example:**
```
Mapper Output:
<What,1> <do,1> <you,1> <What,1> <Java,1> <Java,1>

After Combiner:
<What,2> <do,1> <you,1> <Java,2>
```

**Code:**
```java
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);   // Same logic as reducer
job.setReducerClass(IntSumReducer.class);
```

The Combiner class has the same input/output key-value types as the Reducer.

---

## Q10. Discuss the role of a Partitioner in MapReduce. Illustrate how custom partitioning can improve data distribution.

**Partitioner** works after the Map phase and before the Reduce phase. It determines which Reducer receives which key-value pair.

**Functions of Partitioner:**
- Divides Mapper output into partitions
- Sends each partition to a specific Reducer
- Ensures that the same key always goes to the same Reducer
- Helps distribute workload evenly among Reducers
- Number of partitions equals number of Reducers

**Default Partitioner:** Uses hash function: `hash(key) % numReducers`

**Custom Partitioner Example:**
```java
public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String city = key.toString();
        if (city.equals("Hyderabad")) return 0;
        else if (city.equals("Delhi")) return 1;
        else return 2;
    }
}
```

**Benefit:** Custom partitioning ensures that related data (e.g., all records of the same city) go to the same Reducer, improving locality and reducing redundant processing.

**Setting Partitioner:**
```java
job.setPartitionerClass(CustomPartitioner.class);
job.setNumReduceTasks(3);
```

---

## Q11. Design a MapReduce solution to find the average temperature from weather dataset.

**Approach:** Mapper emits `<city, temp>` pairs. Reducer computes average.

**Mapper:**
```java
public class AvgTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        context.write(new Text(parts[0].trim()),
                      new IntWritable(Integer.parseInt(parts[2].trim())));
    }
}
```

**Reducer:**
```java
public class AvgTempReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0, count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        context.write(key, new DoubleWritable((double) sum / count));
    }
}
```

**Output:**
- `Hyderabad → 31.0°C`
- `Delhi → 18.0°C`

---

## Q12. Analyze the complete execution flow of a MapReduce job from input splitting to output generation.

**Complete MapReduce Execution Flow:**

1. **Job Submission:** Driver submits job configuration to the cluster
2. **Input Splitting:** InputFormat splits input file into logical InputSplits
3. **Record Reader:** Converts each InputSplit into `<key, value>` pairs
4. **Map Phase:** Mapper processes each record, emits intermediate `<key, value>` pairs
5. **Combiner (Optional):** Locally aggregates Mapper output to reduce network load
6. **Partitioner:** Routes each key-value pair to the appropriate Reducer partition
7. **Shuffle Phase:** Intermediate data is transferred from Mappers to Reducers across the network
8. **Sort Phase:** All key-value pairs are sorted by key before entering the Reducer
9. **Reduce Phase:** Reducer processes each `<key, list<values>>` group and computes final output
10. **Record Writer:** Writes final output to HDFS (files named `part-r-00000`, etc.)

**Key roles summary:**
| Component | Role |
|---|---|
| Record Reader | Parses input into key-value pairs |
| Combiner | Local pre-aggregation (optional) |
| Partitioner | Routes data to correct Reducer |
| Reducer | Final aggregation |

---

## Q13. Write a complete Hadoop MapReduce program structure explaining all components.

**Complete Word Count MapReduce Program:**

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import java.util.StringTokenizer;

// MAPPER
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);  // emit <word, 1>
        }
    }
}

// REDUCER (also used as COMBINER)
public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) sum += val.get();
        result.set(sum);
        context.write(key, result);
    }
}

// DRIVER
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);   // Combiner
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
```

---

---

# UNIT IV – Stream Memory and Spark

---

## Q1. Explain the Stream Data Model and Architecture.

**Stream Data Model** is a big data architecture designed for handling **continuous, high-velocity, and potentially infinite flows of data in real-time**.

**Key Characteristics:**
- Data arrives non-stop and cannot be stored entirely
- Systems process elements **on-the-fly** using limited working storage
- Only compact summaries or recent windows are stored

**Data Stream Management System (DSMS):**
- Analogous to a database management system but for live data
- Manages multiple streams simultaneously
- Each stream may have different data rates and types
- Rate of arrival is NOT under system control (unlike traditional DBMS)

**Types of Stream Data:**
1. **Sensor Data** – GPS readings, temperature sensors (e.g., 3.5 TB/day from 1 million ocean sensors)
2. **Image Data** – Satellite imagery, surveillance cameras (London has 6 million cameras)
3. **Internet Traffic** – IP packets routed through network switches
4. **Web Traffic** – Google: hundreds of millions of search queries/day; Yahoo: billions of clicks/day

**Stream Architecture Components:**
- **Stream Sources** → **Stream Processor (Working Memory + Standing Queries)** → **Results**
- **Archival Store** – for backup but queries are NOT answered from archive

**Stream Query Types:**
1. **Standing Queries** – permanently executing, produce outputs continuously (e.g., average of last 24 readings)
2. **Ad-hoc Queries** – one-time queries on current stream window

---

## Q2. Describe Streaming Computing with a suitable example.

**Stream Computing** is a computing paradigm that ingests, processes, and analyzes **continuous, high-volume data streams in real time**, before the data is ever stored to disk.

**Unlike Batch Processing:**
- Batch: Collects data → Stores → Processes (delayed insights)
- Stream: Data arrives → Processed immediately → Results in real-time

**Core Architecture:**
A typical stream computing framework works as a **continuous pipeline**:
```
Data Sources → Ingestion Layer → Processing Engine → Output/Action
```

**Popular Frameworks:** Apache Spark Streaming, Apache Flink, Apache Kafka Streams

**Example – Real-Time Temperature Monitoring:**
- Sensors send temperature readings every second
- Stream processing system continuously computes:
  - Average of last 10 readings
  - Alert if temperature exceeds threshold

**Example SQL-like Stream Query:**
```sql
SELECT AVG(price)
FROM stock_stream [ROWS 10]
WHERE price > 100.0
```
This calculates the running average of the last 10 stock prices above 100.

**Use Cases:**
- Telecom: Combining data from multiple operations support systems
- Healthcare: Integrating medical devices and EHR systems
- Fraud detection, social media monitoring, real-time analytics

---

## Q3. Explain the concept of sampling data in a stream and discuss its significance.

**Sampling** is a technique used to select a **representative subset** of a data stream for processing when it is impractical to process all arriving data.

**Why Sampling is Needed:**
- Data arrives too fast to process everything
- Memory is limited; cannot store entire stream
- Need statistically valid conclusions from partial data

**Types of Sampling:**

**1. Uniform Sampling:**
- Each element is included in the sample with equal probability p
- Simple but may miss important patterns

**2. Reservoir Sampling (Random Sampling):**
- Maintains a fixed-size reservoir of `k` elements
- Each new element has `k/n` probability of being in reservoir (where `n` = elements seen so far)
- Algorithm:
  1. Fill reservoir with first `k` elements
  2. For each subsequent element `i`: generate random `j` from 1 to `i`
  3. If `j ≤ k`, replace reservoir[j] with the new element

**Significance of Sampling:**
1. Reduces memory requirements
2. Enables real-time processing of high-speed streams
3. Provides statistically valid estimates
4. Allows analysis of streams that would otherwise overflow memory
5. Powers approximate query answering systems

**Example:** Web analytics – instead of tracking every click, sample 1% of clicks to estimate total unique users.

---

## Q4 & Q5. Illustrate the working of Reservoir Sampling for processing stream data.

**Reservoir Sampling** is an algorithm that selects `k` random items from a stream of unknown or very large size `n`, ensuring each element has an equal probability of selection.

**Algorithm Steps:**
1. Initialize: Place the first `k` elements into the reservoir
2. For each new element `i` (where `i > k`):
   - Generate a random integer `j` in range `[1, i]`
   - If `j ≤ k`: Replace `reservoir[j]` with the current element
   - Otherwise: Discard the element

**Probability Analysis:**
- Each element has equal probability `k/n` of being in the final reservoir
- This ensures **unbiased random sampling**

**Example:**
- Stream: [a, b, c, d, e, f, g, h], k = 3
- Step 1: Reservoir = [a, b, c]
- Step 2 (d): random j from [1,4]. If j ≤ 3, replace reservoir[j] with d
- Step 3 (e): random j from [1,5]. If j ≤ 3, replace with e
- ...continues until stream ends

**Advantages:**
- Works with streams of unknown length
- Requires only O(k) memory
- Mathematically proven to be unbiased
- Can be used for both finite and infinite streams

---

## Q6 & Q7. Explain different techniques used for filtering streams / Demonstrate how stream filtering improves efficiency.

**Stream Filtering** refers to techniques that allow only certain stream elements (those whose keys belong to a set S) to pass through, while rejecting the rest.

**Challenge:** The set S may be too large to store in memory.

**Technique 1: Bit-Array Filtering**
- Create an array of `n` bits
- Hash each member of set S to a position in the bit array and set that bit to 1
- For each stream element: hash its key; if the bit is 1, let it through; if 0, reject it

**Technique 2: Bloom Filter**

A **Bloom filter** consists of:
1. An array of `n` bits (initially all 0s)
2. A collection of `k` hash functions (h₁, h₂, ..., hₖ)
3. A set S of m key values

**Initialization:**
- For each key K in S and each hash function hᵢ: set bit hᵢ(K) = 1

**Testing a stream element with key K:**
- Check all h₁(K), h₂(K), ..., hₖ(K)
- If **all are 1**: let element through ✓
- If **any is 0**: reject element (K is definitely not in S) ✗

**False Positive Rate:**
- Bloom filters may have false positives (elements not in S pass through)
- Adding more hash functions reduces false positive rate
- With 2 hash functions: false positive rate ≈ 4.93%
- Trade-off: more hash functions = lower false positive rate, but more computation

**How Filtering Improves Efficiency:**
1. Rejects irrelevant data early → less processing downstream
2. Reduces memory usage (compact bit-array vs full set storage)
3. Real-time filtering without database lookups
4. Scales to billions of elements

**Example:** Email spam filter checks sender against a bloom filter of known spam addresses.

---

## Q8 & Q9. Explain/Analyze Counting Distinct Elements in Stream Data Analytics.

**The Count-Distinct Problem:** How many different elements have appeared in the stream from the beginning or from a known past time?

**Example:** A website wants to know how many unique users visited in the past month. Each login = stream element.

**Obvious Approach:** Maintain a hash table of all seen elements → requires too much memory for billions of users.

**Flajolet-Martin Algorithm (FM Algorithm):**
Uses hashing and the probabilistic property of bit patterns.

**Approach:**
1. Hash each stream element to a binary number
2. Count the number of trailing zeros in the hash value
3. Maintain the maximum number of trailing zeros seen (`maxZeros`)
4. Estimate distinct count = 2^maxZeros

**Why it works:** If there are D distinct elements, approximately D/2 hash values end in 1 trailing zero, D/4 end in at least 2, etc.

**AMS (Alon-Matias-Szegedy) Algorithm for Moments:**
For estimating **second moments** (frequency of repeated elements):
1. Pick a random position `i` in the stream
2. Track `X.element` = element at position i
3. Track `X.value` = count of subsequent occurrences of that element
4. Estimate = n(2·X.value − 1)

**Challenges in counting distinct elements:**
1. Streams arrive too fast to store all seen elements
2. Memory constraints prevent maintaining full hash tables
3. Multiple streams simultaneously → memory multiplied
4. Need approximate answers with bounded error

**Solutions:**
- Use probabilistic data structures (Bloom filters, FM sketches)
- Trade accuracy for memory efficiency
- Use multiple hash functions and median of estimates

---

## Q10 & Q27 (Unit IV Part-A). Describe Apache Spark and discuss its advantages over Hadoop MapReduce.

**Apache Spark** is an open-source, distributed computing framework designed for fast, in-memory data processing. It was developed at UC Berkeley's AMPLab and later donated to the Apache Software Foundation.

**Key Features:**
- In-memory computation (up to 100× faster than Hadoop MapReduce)
- Supports batch, streaming, ML, and graph processing
- Compatible with HDFS, Hive, Cassandra, and more
- Supports Java, Scala, Python, R

**Why Data Sharing is Slow in MapReduce:**
- MapReduce requires disk read/write for every step
- Replication, serialization, and disk I/O cause overhead
- Most Hadoop apps spend 90%+ time on HDFS read-write

**Advantages of Spark over MapReduce:**

| Feature | Apache Spark | Hadoop MapReduce |
|---|---|---|
| **Processing Speed** | In-memory → 100× faster | Disk-based → slower |
| **Ease of Use** | High-level APIs (Scala, Python, Java, R) | Java-only (verbose) |
| **Data Processing** | Batch + Streaming + ML + SQL | Batch only |
| **Fault Tolerance** | RDD lineage | Data replication |
| **Iteration** | Efficient (stores in memory) | Slow (disk I/O each step) |
| **Real-Time** | Spark Streaming supported | Not suitable |
| **Code Lines** | ~10 lines | ~50+ lines |

---

## Q11. Explain Spark Architecture with a Neat Diagram and Discuss the Role of its Components.

**Apache Spark Architecture** follows a **Master-Worker (Driver-Executor)** model.

```
+------------------+
|   Driver Program |
|  (SparkContext)  |
+--------+---------+
         |
         | (connects to)
         v
+------------------+
|  Cluster Manager |
| (Standalone/YARN/|
|   Mesos/K8s)     |
+---+----------+---+
    |          |
    v          v
+-------+  +-------+
|Worker |  |Worker |
| Node  |  | Node  |
|Execut-|  |Execut-|
|  or   |  |  or   |
+-------+  +-------+
```

**Components and Their Roles:**

1. **Driver Program:**
   - Contains the main() function
   - Creates SparkContext
   - Converts user code into tasks (DAG of stages)
   - Schedules tasks to executors

2. **SparkContext:**
   - Entry point of any Spark application
   - Connects to the Cluster Manager
   - Coordinates all distributed operations

3. **Cluster Manager:**
   - Allocates resources across the cluster
   - Types: Standalone, YARN, Apache Mesos, Kubernetes
   - Launches executor processes on worker nodes

4. **Worker Nodes:**
   - Physical/virtual machines that run executor processes
   - Can run multiple executors

5. **Executors:**
   - JVM processes running on worker nodes
   - Execute tasks assigned by the Driver
   - Store RDD data in memory (caching)
   - Return results to Driver

6. **Tasks:**
   - Smallest unit of work in Spark
   - Run within an Executor on a partition of data

---

## Q12. Discuss the Installation and Configuration Process of Apache Spark.

**Prerequisites:**
- Java 8 or higher (`java -version`)
- Scala (optional, for Scala API)
- Python (for PySpark)

**Installation Steps:**

**Step 1: Install Java**
```bash
sudo apt install default-jdk
java -version
```

**Step 2: Download Apache Spark**
- Visit spark.apache.org/downloads
- Choose version, package type (pre-built for Hadoop)
- Download: `spark-3.x.x-bin-hadoop3.tgz`

**Step 3: Extract the Archive**
```bash
tar -xvzf spark-3.x.x-bin-hadoop3.tgz
mv spark-3.x.x-bin-hadoop3 /opt/spark
```

**Step 4: Configure Environment Variables**
Add to `~/.bashrc`:
```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
Then: `source ~/.bashrc`

**Step 5: Configure Spark**
```bash
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
# Edit spark-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
export SPARK_MASTER_HOST=localhost
```

**Step 6: Start Spark Shell**
```bash
spark-shell
```
If successful: `scala>` prompt appears

**Step 7: Run PySpark**
```bash
pyspark
```

**Quick Test:**
```scala
val data = Seq(1, 2, 3, 4, 5)
val rdd = sc.parallelize(data)
rdd.sum()   // Output: 15.0
```

---

## Q13. Explain the Concept, Features, and Advantages of Spark RDDs.

**RDD (Resilient Distributed Dataset)** is the fundamental data abstraction in Apache Spark. It is a **read-only, fault-tolerant, distributed collection of data** partitioned across the nodes of a cluster.

**Anatomy of an RDD (what the name means):**
- **Resilient:** If a partition is lost due to node failure, Spark automatically reconstructs it using lineage (transformation history)
- **Distributed:** Data is split into logical chunks (partitions) distributed across multiple machines for parallel execution
- **Dataset:** Holds records of any JVM-compatible type (Scala, Java, Python, user-defined classes)

**Key Features of RDDs:**
1. **Immutability:** Once created, an RDD cannot be modified. Operations create new RDDs.
2. **Lazy Evaluation:** Transformations are not executed immediately; they build a DAG (Directed Acyclic Graph). Execution happens only when an Action is called.
3. **Fault Tolerance:** RDD lineage graph allows re-computation of lost partitions without data replication.
4. **Partitioning:** Data is automatically partitioned across nodes for parallelism.
5. **In-Memory Processing:** Intermediate results are stored in RAM, avoiding disk I/O.
6. **Persistence/Caching:** RDDs can be cached in memory for reuse across multiple operations.

**Advantages:**
- Faster than MapReduce for iterative algorithms
- Supports interactive queries in memory
- Provides fault tolerance without expensive replication
- Works with diverse data sources (HDFS, S3, Cassandra, HBase)

**Creating an RDD:**
```python
# From a list
rdd = sc.parallelize([1, 2, 3, 4, 5])
# From HDFS file
rdd = sc.textFile("hdfs://path/to/file.txt")
```

---

## Q14. Differentiate Between Narrow and Wide Transformations in Spark RDD Operations.

**Narrow Transformations:**
- All data required to compute a partition comes from **a single parent partition**
- No shuffling of data across the network
- Fast and efficient
- Examples: `map()`, `filter()`, `flatMap()`, `mapPartitions()`

**Wide Transformations:**
- Data required to compute a partition may come from **multiple parent partitions**
- Requires shuffling data across the network (expensive)
- Creates a stage boundary in the DAG
- Examples: `groupByKey()`, `reduceByKey()`, `sortByKey()`, `join()`

| Feature | Narrow Transformation | Wide Transformation |
|---|---|---|
| **Data Dependency** | Single parent partition | Multiple parent partitions |
| **Network Shuffle** | No | Yes (expensive) |
| **Speed** | Faster | Slower |
| **Stage Boundary** | No | Yes |
| **Examples** | `map()`, `filter()` | `groupByKey()`, `reduceByKey()` |
| **Fault Recovery** | Re-compute from parent | Re-compute entire shuffle |

**Example of Narrow (map):**
```python
rdd = sc.parallelize([1,2,3,4,5])
result = rdd.map(lambda x: x + 2)  # [3,4,5,6,7] - no shuffle
```

**Example of Wide (groupByKey):**
```python
pairs = sc.parallelize([("a",1),("b",2),("a",3)])
grouped = pairs.groupByKey()  # shuffles "a" records to one partition
```

---

## Q15. Demonstrate Various Spark RDD Operations such as map(), filter(), reduce(), collect(), flatMap().

**Spark RDD Operations are divided into:**
- **Transformations** – create new RDDs (lazy)
- **Actions** – trigger execution, return results

**1. map(func)**
Applies a function to each element, returns a new RDD.
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2)
# result: [2, 4, 6, 8, 10]
```

**2. filter(func)**
Returns a new RDD containing only elements that satisfy the predicate.
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
even = rdd.filter(lambda x: x % 2 == 0)
# even: [2, 4]
```

**3. flatMap(func)**
Like map, but each input element can produce multiple output elements. Most common use: splitting strings into words.
```python
rdd = sc.parallelize(["Hello World", "Big Data"])
words = rdd.flatMap(lambda line: line.split(" "))
# words: ["Hello", "World", "Big", "Data"]
```

**4. reduce(func)**
Aggregates all elements using a function. Returns a single value (Action).
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
total = rdd.reduce(lambda a, b: a + b)
# total: 15
```

**5. collect()**
Returns all elements of the RDD as a list to the Driver (Action).
```python
rdd = sc.parallelize([1, 2, 3])
data = rdd.collect()
# data: [1, 2, 3]
```

**Other Important Operations:**
- `count()` – returns number of elements
- `distinct()` – removes duplicates
- `union(other)` – combines two RDDs of same type
- `intersection(other)` – returns common elements

---

---

# UNIT V – Pig and Hive

---

## Q1. Explain the Architecture of Apache Pig and Discuss the Role of its Components.

**Apache Pig** is a high-level data processing platform built on top of Hadoop that simplifies MapReduce programming using a scripting language called **Pig Latin**.

**Pig Architecture Components:**

```
User (Pig Latin Script)
        ↓
    Parser
        ↓
   Optimizer
        ↓
   Compiler
        ↓
  Execution Engine
        ↓
  Hadoop/MapReduce
        ↓
     Output
```

**1. Parser:**
- Receives the Pig Latin script
- Checks syntax and semantics
- Produces a **Logical Plan** (DAG of operators)
- Validates schemas and data types

**2. Optimizer:**
- Takes the Logical Plan and optimizes it
- Applies optimizations like: projection pushdown, filter pushdown
- Aims to reduce data volume and processing time

**3. Compiler:**
- Converts the optimized Logical Plan into a **Physical Plan**
- Translates Pig operations into MapReduce jobs
- Determines how many MapReduce jobs are needed

**4. Execution Engine:**
- Executes the compiled MapReduce jobs on Hadoop cluster
- Monitors job progress
- Returns results to the user

**5. Grunt Shell:**
- Interactive command-line shell for executing Pig Latin statements interactively
- Used for debugging and testing

**6. Pig Server / Pig Script:**
- Pig can be executed in batch mode via `.pig` script files
- Or programmatically using Pig APIs in Java

**Advantages of Pig:**
- Reduces MapReduce code from 200+ lines to ~10 lines
- Supports complex joins, aggregations, and ETL operations
- Automatically optimizes execution

---

## Q2. Describe the Pig Latin Application Flow with a Suitable Diagram.

**Pig Latin Application Flow** describes how a Pig Latin script gets executed from input to output.

**Flow Diagram:**
```
Input Data (HDFS)
       ↓
   LOAD statement
       ↓
   Data Transformations
   (FILTER, JOIN, GROUP, etc.)
       ↓
   STORE / DUMP
       ↓
Output Data (HDFS / Console)
```

**Step-by-Step Flow:**

**Step 1: LOAD**
Read data from HDFS into a relation.
```pig
students = LOAD '/data/students.txt' USING PigStorage(',')
           AS (id:int, name:chararray, age:int);
```

**Step 2: FILTER**
Select records matching a condition.
```pig
adults = FILTER students BY age >= 18;
```

**Step 3: GROUP**
Group records by a field.
```pig
grouped = GROUP adults BY age;
```

**Step 4: FOREACH/GENERATE**
Apply transformations to each record.
```pig
result = FOREACH grouped GENERATE group, COUNT($1) AS count;
```

**Step 5: ORDER BY**
Sort the result.
```pig
sorted = ORDER result BY count DESC;
```

**Step 6: STORE / DUMP**
Write results to HDFS or display on console.
```pig
STORE sorted INTO '/output/result' USING PigStorage(',');
-- or --
DUMP sorted;
```

**Key Characteristics of the Flow:**
- Pig Latin is procedural (step-by-step), unlike SQL (declarative)
- Each statement creates a new relation (immutable)
- Operations are lazy; execution starts only at DUMP/STORE

---

## Q3. Explain the Basic Concepts: Working through the ABCs of Pig Latin.

**ABCs of Pig Latin – Basic Concepts:**

**A – Atoms (Basic Data Types):**
Pig Latin supports the following primitive types:
- `int`, `long`, `float`, `double`
- `chararray` (String), `bytearray`
- `boolean`, `datetime`

**B – Bags:**
- An **unordered collection of tuples**
- Allows duplicate elements
- Created by GROUP operation
- Syntax: `{(val1, val2), (val3, val4)}`

**C – Complex Types:**
1. **Tuple** – Ordered set of fields: `(1, 'John', 3.5)`
2. **Bag** – Collection of tuples: `{(1, 'a'), (2, 'b')}`
3. **Map** – Key-value pairs: `[key#value]`

**Pig Latin Key Operations (ABCs):**

| Operation | Description | Example |
|---|---|---|
| **LOAD** | Read data | `A = LOAD 'file' AS (...)` |
| **FILTER** | Filter rows | `B = FILTER A BY age > 18` |
| **FOREACH** | Transform columns | `C = FOREACH A GENERATE name, age` |
| **GROUP** | Group rows | `D = GROUP A BY dept` |
| **JOIN** | Join relations | `E = JOIN A BY id, B BY id` |
| **ORDER BY** | Sort | `F = ORDER A BY name` |
| **LIMIT** | Restrict rows | `G = LIMIT A 10` |
| **STORE** | Write output | `STORE A INTO 'path'` |
| **DUMP** | Print to console | `DUMP A` |

**Example Pig Latin Script:**
```pig
-- Count employees per department
emp = LOAD '/data/emp.txt' USING PigStorage('\t')
      AS (id:int, name:chararray, dept:chararray, salary:float);
grp = GROUP emp BY dept;
result = FOREACH grp GENERATE group AS dept, COUNT($1) AS num_emp;
DUMP result;
```

---

## Q4. Compare Local Mode and Distributed Mode for Running Pig Scripts.

Apache Pig can be run in two modes:

| Feature | **Local Mode** | **Distributed (MapReduce) Mode** |
|---|---|---|
| **Command** | `pig -x local` | `pig` or `pig -x mapreduce` |
| **Execution** | Single JVM on local machine | Full Hadoop cluster |
| **File System** | Local filesystem | HDFS |
| **Use Case** | Development, testing, debugging | Production, large-scale data |
| **Data Size** | Small datasets only | Terabytes/Petabytes |
| **Speed** | Fast startup | Slower (cluster overhead) |
| **HDFS Required** | No | Yes |
| **Hadoop Required** | Not required | Required |
| **Parallelism** | Single-threaded | Multi-node parallel execution |
| **Fault Tolerance** | None | Built-in via HDFS replication |

**Local Mode Example:**
```bash
pig -x local
grunt> A = LOAD 'students.txt' AS (id:int, name:chararray);
grunt> DUMP A;
```

**Distributed Mode Example:**
```bash
pig
grunt> A = LOAD 'hdfs:///data/students.txt' AS (id:int, name:chararray);
grunt> DUMP A;
```

**When to Use Which:**
- **Local Mode:** Unit testing scripts, prototyping, small data exploration
- **Distributed Mode:** Production jobs, large datasets, multi-node parallel processing

---

## Q5. Discuss the Various Interfaces Available for Executing Pig Scripts.

Apache Pig provides multiple interfaces to execute scripts:

**1. Grunt Shell (Interactive Mode):**
- Interactive command-line shell
- Executes Pig Latin statements one at a time
- Ideal for exploration and debugging
```bash
pig -x local
grunt> data = LOAD 'file.txt';
grunt> DUMP data;
```

**2. Script File (Batch Mode):**
- Write script in a `.pig` file and execute it
- Best for production and automation
```bash
pig -x local myscript.pig
# OR
pig myscript.pig
```

**3. Embedded Mode (UDFs via Java/Python):**
- Embed Pig Latin in Java programs using `PigServer` API
- Allows programmatic control of Pig execution
```java
PigServer pigServer = new PigServer(ExecType.LOCAL);
pigServer.registerQuery("A = LOAD 'data.txt';");
pigServer.dumpSchema("A");
```

**4. Pig using HCatalog:**
- Integrates with HCatalog (Hive Metastore) for schema management
- Allows Pig to read/write Hive tables

**5. Web-based Interfaces:**
- **Apache Zeppelin** – web-based notebook supporting Pig scripts
- **Hue** – browser-based Hadoop UI with Pig editor

**Comparison:**

| Interface | Best For | Interactivity |
|---|---|---|
| Grunt Shell | Exploration/debugging | High |
| Script File | Production jobs | Low |
| Embedded (Java) | Application integration | Medium |
| Web UI (Hue/Zeppelin) | Visual development | High |

---

## Q6. Illustrate the Process of Writing and Executing a Pig Latin Script.

**Step-by-Step: Word Count in Pig Latin**

**Step 1: Prepare input data**
Create `words.txt`:
```
Hello World
Hello Pig
World is Big
```

**Step 2: Write the Pig Latin Script** (wordcount.pig)
```pig
-- Load input file
lines = LOAD '/input/words.txt' USING TextLoader() AS (line:chararray);

-- Split each line into words
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Group by word
grp_words = GROUP words BY word;

-- Count occurrences of each word
word_count = FOREACH grp_words GENERATE group AS word, COUNT($1) AS count;

-- Order by count descending
result = ORDER word_count BY count DESC;

-- Store output
STORE result INTO '/output/wordcount' USING PigStorage('\t');
```

**Step 3: Execute in Local Mode**
```bash
pig -x local wordcount.pig
```

**Step 4: Execute in MapReduce Mode (on cluster)**
```bash
pig wordcount.pig
```

**Step 5: View Output**
```
Hello    2
World    2
Big      1
is       1
Pig      1
```

**Key Pig Latin Commands Used:**
- `LOAD` – reads input
- `FOREACH...GENERATE` – transforms each record
- `FLATTEN` – unnests bags/tuples
- `TOKENIZE` – splits string into bag of words
- `GROUP BY` – groups records by key
- `COUNT` – counts elements in a bag
- `ORDER BY` – sorts results
- `STORE` – writes output to HDFS

---

## Q7. Explain the Data Model and Data Types Supported by Pig Latin.

**Pig Latin Data Model:**
Pig Latin uses a **nested data model** with four levels:

**1. Atom (Scalar):**
A simple atomic value – a number or string.
```
'John', 25, 3.14
```

**2. Tuple:**
An ordered set of fields (like a row in a table).
```
(1, 'John', 25, 'CSE')
```

**3. Bag:**
An unordered collection of tuples (allows duplicates).
```
{(1, 'John'), (2, 'Mary'), (3, 'John')}
```

**4. Map:**
A set of key-value pairs where keys are strings.
```
['name'#'John', 'age'#25]
```

**Pig Latin Data Types:**

| Category | Type | Description |
|---|---|---|
| **Numeric** | `int` | 32-bit signed integer |
| | `long` | 64-bit signed integer |
| | `float` | 32-bit floating point |
| | `double` | 64-bit floating point |
| **Text** | `chararray` | UTF-8 encoded string |
| | `bytearray` | Byte array (default, untyped) |
| **Other** | `boolean` | true/false |
| | `datetime` | Date and time |
| **Complex** | `tuple` | Ordered list of fields |
| | `bag` | Collection of tuples |
| | `map` | Key-value pairs |

**Example Schema Declaration:**
```pig
emp = LOAD 'emp.txt' USING PigStorage(',')
      AS (id:int, name:chararray, salary:float, dept:chararray);
```

---

## Q8. Describe the Architecture of Apache Hive and Explain the Function of Each Component.

**Apache Hive** is a data warehousing infrastructure built on top of Hadoop that provides an SQL-like interface (HiveQL) to query and analyze large datasets stored in HDFS.

**Hive Architecture:**

```
User (HiveQL Query)
      ↓
Hive Interfaces (CLI/Web UI/JDBC/ODBC/Thrift)
      ↓
      Driver
    /        \
Compiler    Execution Engine
    |               |
 Metastore      Hadoop/MapReduce
    |               |
RDBMS (MySQL)  HDFS (DataNodes)
```

**Components and Functions:**

**1. Hive Interfaces:**
- CLI (Command Line Interface), Web UI, Beeline, JDBC/ODBC, Thrift Server
- Accept HiveQL queries from users/applications

**2. Driver:**
- Central component that manages query lifecycle
- Receives queries from interfaces
- Manages session handles and lifecycle statistics

**3. Compiler:**
- Parses HiveQL query for syntax/semantics
- Fetches metadata from Metastore
- Generates a **logical plan** → converts to **physical plan** (MapReduce DAG)

**4. Metastore:**
- Stores metadata about tables, columns, partitions, schemas, storage formats
- Typically backed by a relational database (MySQL, Derby)
- Provides metadata to Compiler during query parsing

**5. Execution Engine:**
- Executes the physical plan (as MapReduce jobs)
- Coordinates with JobTracker (NameNode) to schedule tasks on DataNodes
- Can also execute via Tez or Spark (modern Hive versions)

**6. HDFS / Storage:**
- Actual data resides in HDFS
- Hive tables are stored as files in HDFS directories

---

## Q9 & Q10. Explain "Saying Hello to Hive" and Getting Started with Apache Hive.

**Introduction to Hive:**
Apache Hive is a data warehouse solution that:
- Sits on top of Hadoop (HDFS + MapReduce)
- Translates SQL-like HiveQL queries into MapReduce jobs
- Allows users familiar with SQL to work with big data

**How Hive is Put Together (Workflow):**

| Step | Operation | Description |
|---|---|---|
| 1 | Execute Query | Interface sends query to Driver |
| 2 | Get Plan | Compiler parses query, checks syntax |
| 3 | Get Metadata | Compiler requests metadata from Metastore |
| 4 | Send Metadata | Metastore returns metadata to Compiler |
| 5 | Send Plan | Compiler sends execution plan to Driver |
| 6 | Execute Plan | Driver sends plan to Execution Engine |
| 7 | Execute Job | Engine submits MapReduce job to JobTracker |
| 8 | Fetch Result | Engine retrieves results from DataNodes |
| 9 | Send Results | Engine returns results to Driver |
| 10 | Send Results | Driver returns results to Interface |

**Getting Started with Hive:**

**Start Hive CLI:**
```bash
hive
```

**Create a Database:**
```sql
hive> CREATE DATABASE IF NOT EXISTS mydb;
hive> SHOW DATABASES;
```

**Create a Table:**
```sql
hive> USE mydb;
hive> CREATE TABLE employee (
        eid INT,
        name STRING,
        salary FLOAT,
        dept STRING
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      STORED AS TEXTFILE;
```

**Load Data:**
```sql
hive> LOAD DATA LOCAL INPATH '/home/user/emp.csv'
      OVERWRITE INTO TABLE employee;
```

**Query Data:**
```sql
hive> SELECT * FROM employee WHERE salary > 30000;
```

---

## Q11. Explain Various Hive Data Types with Suitable Examples.

Hive data types are classified into **four categories:**

**1. Column Types (Primitive):**

**Integral Types:**
| Type | Range | Example |
|---|---|---|
| TINYINT | -128 to 127 | `10Y` |
| SMALLINT | -32768 to 32767 | `100S` |
| INT | -2³¹ to 2³¹-1 | `1000` |
| BIGINT | -2⁶³ to 2⁶³-1 | `10000L` |

**String Types:**
| Type | Max Length | Example |
|---|---|---|
| VARCHAR | 1 to 65535 | `'Hello'` |
| CHAR | 255 | `'A'` |
| STRING | Unlimited | `'Apache Hive'` |

**Date/Time Types:**
- `TIMESTAMP` – `'2025-01-01 10:30:00.000'`
- `DATE` – `'2025-01-01'`

**Floating Point:**
- `FLOAT` – 32-bit: `3.14`
- `DOUBLE` – 64-bit: `3.14159265358979`
- `DECIMAL` – arbitrary precision: `DECIMAL(10,2)` → `12345.67`

**2. Literals:**
- Floating point: composed of DOUBLE type
- Decimal: higher range than DOUBLE (approx. -10⁻³⁰⁸ to 10³⁰⁸)

**3. Null Values:**
- Missing values represented as `NULL`

**4. Complex Types:**
```sql
-- Array
col ARRAY<STRING>          -- ARRAY['a', 'b', 'c']

-- Map
col MAP<STRING, INT>       -- MAP['key1', 1, 'key2', 2]

-- Struct
col STRUCT<name:STRING, age:INT>   -- STRUCT('John', 25)

-- Union
col UNIONTYPE<int, double, array<string>>
```

**Example Table with Mixed Types:**
```sql
CREATE TABLE student (
    id       INT,
    name     VARCHAR(100),
    gpa      DOUBLE,
    courses  ARRAY<STRING>,
    address  STRUCT<city:STRING, zip:INT>
);
```

---

## Q12. Describe the Process of Creating and Managing Databases and Tables in Hive.

**Creating and Managing Databases:**

```sql
-- Create database
hive> CREATE DATABASE IF NOT EXISTS college;

-- Show all databases
hive> SHOW DATABASES;

-- Use a database
hive> USE college;

-- Drop database
hive> DROP DATABASE IF EXISTS college CASCADE;
```

**Creating Tables:**

**Managed (Internal) Table:**
```sql
hive> CREATE TABLE IF NOT EXISTS employee (
        eid        INT,
        name       STRING,
        salary     FLOAT,
        dept       STRING
      )
      COMMENT 'Employee Details'
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
      STORED AS TEXTFILE;
```

**External Table:**
```sql
hive> CREATE EXTERNAL TABLE ext_employee (
        eid   INT,
        name  STRING
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      LOCATION '/external/data/emp/';
```

**Difference between Managed and External Tables:**
| Feature | Managed Table | External Table |
|---|---|---|
| Data ownership | Hive owns data | Data is external |
| DROP behavior | Drops data + metadata | Drops only metadata |
| Use case | Hive-only data | Shared with other tools |

**Altering Tables:**
```sql
-- Rename table
hive> ALTER TABLE employee RENAME TO emp;

-- Add column
hive> ALTER TABLE emp ADD COLUMNS (email STRING);

-- Change column name/type
hive> ALTER TABLE emp CHANGE salary sal DOUBLE;

-- Drop table
hive> DROP TABLE IF EXISTS emp;
```

**Loading Data:**
```sql
-- From local file
hive> LOAD DATA LOCAL INPATH '/home/data/emp.txt'
      OVERWRITE INTO TABLE employee;

-- From HDFS
hive> LOAD DATA INPATH '/hdfs/data/emp.txt'
      INTO TABLE employee;
```

---

## Q13. Explain how Hive Data Manipulation Language (DML) works with examples.

**Hive DML** consists of statements used to insert, update, delete, and manage data in Hive tables.

**1. INSERT INTO / INSERT OVERWRITE:**
```sql
-- Insert from query result
hive> INSERT INTO TABLE high_earners
      SELECT eid, name, salary FROM employee
      WHERE salary > 50000;

-- Overwrite table with query result
hive> INSERT OVERWRITE TABLE dept_avg
      SELECT dept, AVG(salary) AS avg_sal
      FROM employee
      GROUP BY dept;
```

**2. LOAD DATA (bulk insert):**
```sql
hive> LOAD DATA LOCAL INPATH '/home/user/emp.txt'
      OVERWRITE INTO TABLE employee;
```

**3. SELECT (Query Data):**
```sql
hive> SELECT name, salary FROM employee WHERE salary > 30000;
hive> SELECT dept, COUNT(*) AS count FROM employee GROUP BY dept;
hive> SELECT * FROM employee ORDER BY salary DESC;
```

**4. UPDATE and DELETE (Hive 0.14+ with ACID):**
Requires bucketed ACID tables:
```sql
hive> UPDATE employee SET salary = salary * 1.1 WHERE dept = 'CSE';
hive> DELETE FROM employee WHERE eid = 1001;
```

**5. CREATE TABLE AS SELECT (CTAS):**
```sql
hive> CREATE TABLE cse_emp AS
      SELECT * FROM employee WHERE dept = 'CSE';
```

**6. TRUNCATE TABLE:**
```sql
hive> TRUNCATE TABLE employee;
```

---

## Q14. Demonstrate how HiveQL is used to Query and Analyze Data Stored in Hadoop.

**HiveQL (Hive Query Language)** is an SQL-like language used to query and analyze data in HDFS via Hive.

**Step 1: Setup – Create and Load Table**
```sql
CREATE TABLE employee (
    emp_id   INT,
    emp_name STRING,
    emp_sal  FLOAT,
    dept     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/data/employee.csv' INTO TABLE employee;
```

**Step 2: Basic Querying**
```sql
-- All employees
SELECT * FROM employee;

-- Filter by salary
SELECT emp_name, emp_sal FROM employee WHERE emp_sal > 50000;
```

**Step 3: Aggregations and Grouping**
```sql
-- Count per department
SELECT dept, COUNT(*) AS num_emp FROM employee GROUP BY dept;

-- Average salary per department
SELECT dept, AVG(emp_sal) AS avg_salary FROM employee GROUP BY dept;

-- Max/Min salary
SELECT MAX(emp_sal), MIN(emp_sal) FROM employee;
```

**Step 4: Sorting**
```sql
-- Order by salary
SELECT emp_id, emp_name, emp_sal FROM employee ORDER BY emp_sal DESC;
```

**Step 5: Joins**
```sql
-- Inner Join with department table
SELECT e.emp_name, d.dept_name
FROM employee e
JOIN department d ON (e.dept = d.dept_id);
```

**Step 6: Storing Query Results**
```sql
INSERT OVERWRITE TABLE high_salary_emp
SELECT emp_name, emp_sal FROM employee WHERE emp_sal > 75000;
```

**Step 7: Using Built-in Functions**
```sql
SELECT emp_name, UPPER(emp_name) AS upper_name, LENGTH(emp_name) AS len
FROM employee;
```

---

## Q15. Explain about Examining the Hive Clients with Suitable Examples.

**Hive Clients** are various interfaces through which users and applications connect to Hive to execute queries.

**1. Hive CLI (Command Line Interface):**
- The traditional command-line tool
- Used directly on the Hadoop gateway node
```bash
hive
hive> SHOW DATABASES;
hive> SELECT * FROM employee LIMIT 5;
hive> quit;
```

**2. Beeline (Modern CLI):**
- Replaces the old Hive CLI in modern Hive
- Connects to HiveServer2 via JDBC
```bash
beeline -u jdbc:hive2://localhost:10000
beeline> !connect jdbc:hive2://localhost:10000
beeline> SHOW TABLES;
```

**3. JDBC Client (Java Application):**
- Connects from Java programs using JDBC driver
```java
Class.forName("org.apache.hive.jdbc.HiveDriver");
Connection con = DriverManager.getConnection(
    "jdbc:hive2://localhost:10000/default", "hive", "");
Statement stmt = con.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM employee");
while (rs.next()) {
    System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
}
```

**4. ODBC Client:**
- Allows ODBC-compatible tools (e.g., Tableau, Excel) to connect to Hive
- Uses Microsoft's ODBC standard
- Enables BI tools to query Hive tables via standard interface

**5. Thrift Server (HiveServer2):**
- Provides a server endpoint accessible over TCP
- Supports multiple concurrent clients
- Used as the backend for Beeline, JDBC, and ODBC connections
```bash
# Start HiveServer2
hive --service hiveserver2
```

**6. Web UI / Hue:**
- Browser-based interface for writing and running HiveQL queries
- Provides query history, result visualization, and table browser
- URL: `http://hadoop-master:8888`

**Comparison of Hive Clients:**

| Client | Protocol | Best For |
|---|---|---|
| Hive CLI | Local | Admin/Development |
| Beeline | JDBC | Production queries |
| JDBC | JDBC | Java applications |
| ODBC | ODBC | BI tools (Tableau) |
| Web UI/Hue | HTTP | Visual/interactive use |

---

*All answers are based on course materials: UNIT-III-CSM.pdf, R23 BDA UNIT-IV - CSM.pdf, and R23 BDA UNIT V-CSD.pdf.*
