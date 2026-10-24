<div align="center">

# 📘 Big Data Analytics (BDA) — MID II Complete Question Bank Solutions

### 🎯 Units III, IV & V — Part-A (2 Marks) & Part-B (5 Marks)

![Hadoop MapReduce](https://img.shields.io/badge/Unit%203-MapReduce%20Programs-blue?style=for-the-badge&logo=apachehadoop)
![Spark & Streams](https://img.shields.io/badge/Unit%204-Streams%20%26%20Spark-green?style=for-the-badge&logo=apachespark)
![Pig & Hive](https://img.shields.io/badge/Unit%205-Pig%20%26%20Hive-orange?style=for-the-badge&logo=apachehive)

| 📚 Subject | 🏛️ Branch | 📅 Year & Sem | 📜 Regulation | 🎯 Coverage |
|:---:|:---:|:---:|:---:|:---:|
| **Big Data Analytics (BDA)** | **CSD / CSM** | **IV Year – I Sem** | **R23** | **Units 3, 4, 5 (Part-A 2M & Part-B 5M)** |

</div>

---

## 📑 Master Table of Contents

- [📌 PART-A: Short Answer Questions (2 Marks Each)](#-part-a-short-answer-questions-2-marks-each)
  - [🔵 Unit III: Writing MapReduce Programs (Part-A)](#-unit-iii-writing-mapreduce-programs-part-a)
    - [Q1 — What is MapReduce? State its main purpose in Hadoop.](#q1--what-is-mapreduce-state-its-main-purpose-in-hadoop-2m)
    - [Q2 — What is a weather dataset in Hadoop MapReduce?](#q2--what-is-a-weather-dataset-in-hadoop-mapreduce-2m)
    - [Q3 — Define Mapper and mention its role in MapReduce processing.](#q3--define-mapper-and-mention-its-role-in-mapreduce-processing-2m)
    - [Q4 — What is the function of a Reducer in a MapReduce program?](#q4--what-is-the-function-of-a-reducer-in-a-mapreduce-program-2m)
    - [Q5 — What is Driver code in a Hadoop MapReduce application?](#q5--what-is-driver-code-in-a-hadoop-mapreduce-application-2m)
    - [Q6 — What is a Record Reader? Why is it required in MapReduce?](#q6--what-is-a-record-reader-why-is-it-required-in-mapreduce-2m)
    - [Q7 — What is a Combiner? How does it improve MapReduce performance?](#q7--what-is-a-combiner-how-does-it-improve-mapreduce-performance-2m)
    - [Q8 — Differences between Old and New Hadoop MapReduce APIs](#q8--list-any-two-differences-between-the-old-and-new-hadoop-mapreduce-apis-2m)
    - [Q9 — Significance of key-value pairs in Hadoop MapReduce](#q9--explain-the-significance-of-key-value-pairs-in-hadoop-mapreduce-2m)
    - [Q10 — Input and output components of a Mapper](#q10--what-are-the-input-and-output-components-of-a-mapper-2m)
  - [🟢 Unit IV: Stream Memory and Spark (Part-A)](#-unit-iv-stream-memory-and-spark-part-a)
    - [Q1 — Define stream computing.](#q1--define-stream-computing-2m)
    - [Q2 — What is a stream data model?](#q2--what-is-a-stream-data-model-2m)
    - [Q3 — List the key components of stream processing architecture.](#q3--list-the-key-components-of-stream-processing-architecture-2m)
    - [Q4 — Differentiate between stream processing and batch processing.](#q4--differentiate-between-stream-processing-and-batch-processing-2m)
    - [Q5 — What is meant by data streams?](#q5--what-is-meant-by-data-streams-2m)
    - [Q6 — Define sampling in stream data processing.](#q6--define-sampling-in-stream-data-processing-2m)
    - [Q7 — State any two advantages of sampling data in a stream.](#q7--state-any-two-advantages-of-sampling-data-in-a-stream-2m)
    - [Q8 — What is filtering in stream processing?](#q8--what-is-filtering-in-stream-processing-2m)
    - [Q9 — Mention any two applications of stream filtering.](#q9--mention-any-two-applications-of-stream-filtering-2m)
    - [Q10 — Counting distinct elements in a stream](#q10--what-is-meant-by-counting-distinct-elements-in-a-stream-2m)
    - [Q11 — Define Apache Spark.](#q11--define-apache-spark-2m)
    - [Q12 — List the major components of Spark architecture.](#q12--list-the-major-components-of-spark-architecture-2m)
    - [Q13 — What is Resilient Distributed Dataset (RDD)?](#q13--what-is-resilient-distributed-dataset-rdd-2m)
    - [Q14 — Differentiate between transformation and action operations in Spark RDD.](#q14--differentiate-between-transformation-and-action-operations-in-spark-rdd-2m)
  - [🟡 Unit V: Pig and Hive (Part-A)](#-unit-v-pig-and-hive-part-a)
    - [Q1 — What is Apache Pig?](#q1--what-is-apache-pig-2m)
    - [Q2 — State the purpose of Pig Latin in Hadoop.](#q2--state-the-purpose-of-pig-latin-in-hadoop-2m)
    - [Q3 — List the major components of Pig Architecture.](#q3--list-the-major-components-of-pig-architecture-2m)
    - [Q4 — What is the Pig Latin application flow?](#q4--what-is-the-pig-latin-application-flow-2m)
    - [Q5 — Define relation in Pig Latin.](#q5--define-relation-in-pig-latin-2m)
    - [Q6 — Basic data types supported by Pig Latin](#q6--what-are-the-basic-data-types-supported-by-pig-latin-2m)
    - [Q7 — Differentiate between Local Mode and Distributed Mode in Pig.](#q7--differentiate-between-local-mode-and-distributed-mode-in-pig-2m)
    - [Q8 — Role of Grunt Shell in Pig](#q8--what-is-the-role-of-grunt-shell-in-pig-2m)
    - [Q9 — Interfaces available for executing Pig scripts](#q9--list-the-different-interfaces-available-for-executing-pig-scripts-2m)
    - [Q10 — What is Apache Hive?](#q10--what-is-apache-hive-2m)
    - [Q11 — Key components of Hive Architecture](#q11--mention-the-key-components-of-hive-architecture-2m)
    - [Q12 — Hive data types and examples](#q12--what-are-hive-data-types-give-two-examples-2m)
    - [Q13 — Purpose of a Hive database](#q13--what-is-the-purpose-of-a-hive-database-2m)
    - [Q15 — Differentiate between Managed Table and External Table](#q15--differentiate-between-managed-table-and-external-table-2m)
- [📌 PART-B: Long Answer Questions (5 Marks Each)](#-part-b-long-answer-questions-5-marks-each)
  - [🔵 Unit III — Writing MapReduce Programs (Part-B)](#-unit-iii--writing-mapreduce-programs)
  - [🟢 Unit IV — Stream Memory and Spark (Part-B)](#-unit-iv--stream-memory-and-spark)
  - [🟡 Unit V — Pig and Hive (Part-B)](#-unit-v--pig-and-hive)

---

# 📌 PART-A: Short Answer Questions (2 Marks Each)

---

## 🔵 Unit III: Writing MapReduce Programs (Part-A)

> **Syllabus:** MapReduce Purpose · Weather Dataset · Mapper & Reducer Roles · Driver Code · RecordReader · Combiner · Old vs New API · Key-Value Pairs · Mapper Input/Output

---

<details open>
<summary><b>Q1 — What is MapReduce? State its main purpose in Hadoop. [2M]</b></summary>

<br>

- **Definition:** **MapReduce** is a software programming model and processing framework designed by Google and implemented in Apache Hadoop to process massive volumes of unstructured or structured data in parallel across distributed clusters of commodity machines.
- **Main Purpose in Hadoop:**
  1. **Divide-and-Conquer Parallelism:** Decomposes a massive job into two distinct phases: **Map** (filtering, parsing, and projecting records) and **Reduce** (aggregating, combining, and summarizing results).
  2. **Data Locality:** Ships computation code directly to the cluster nodes where the data blocks reside in HDFS, eliminating costly cross-network raw data transfers and ensuring fault tolerance and linear scalability.

</details>

---

<details open>
<summary><b>Q2 — What is a weather dataset in Hadoop MapReduce? [2M]</b></summary>

<br>

- **Definition:** The **weather dataset** (typically sourced from the National Climatic Data Center - **NCDC**) is a classic semi-structured flat ASCII text dataset widely used as a benchmark for teaching and demonstrating Hadoop MapReduce workflows.
- **Key Characteristics:**
  - Contains hourly/daily weather sensor readings across global meteorological stations: station identifier, timestamp, latitude/longitude, atmospheric pressure, wind speed, precipitation, and air temperature.
  - In MapReduce problems, it is utilized to demonstrate how the **Mapper** extracts specific columns (e.g., Year and Temperature, filtering out missing values `+9999`) and the **Reducer** aggregates them to compute maximum, minimum, or average temperatures per year or station.

</details>

---

<details open>
<summary><b>Q3 — Define Mapper and mention its role in MapReduce processing. [2M]</b></summary>

<br>

- **Definition:** A **Mapper** is the initial processing unit in a MapReduce application that extends the `Mapper` base class and overrides the `map()` method to process input records.
- **Role in MapReduce Processing:**
  1. Takes individual `<Key, Value>` pairs generated from an Input Split by the `RecordReader` (e.g., `<ByteOffset, LineText>`).
  2. Executes user-defined business logic to parse, tokenize, filter, and transform the input line, emitting zero or more intermediate `<Key, Value>` pairs (e.g., `<Year, Temperature>` or `<Word, 1>`) for sorting and partitioning.

</details>

---

<details open>
<summary><b>Q4 — What is the function of a Reducer in a MapReduce program? [2M]</b></summary>

<br>

- **Definition:** A **Reducer** is the secondary computation component in MapReduce that implements the `reduce()` method to process aggregated intermediate results.
- **Functions:**
  1. **Aggregation:** Receives intermediate keys along with an iterator over all associated values: `<Key, Iterable<Values>>` produced by the framework's Shuffle and Sort phase.
  2. **Final Emission:** Performs data reduction operations (such as summation, averaging, counting, or extreme value finding) on each group and writes final `<Key, Value>` results to HDFS via `OutputFormat`.

</details>

---

<details open>
<summary><b>Q5 — What is Driver code in a Hadoop MapReduce application? [2M]</b></summary>

<br>

- **Definition:** The **Driver code** is the client-side Java class containing the standard `main()` method that configures, orchestrates, and submits the MapReduce job to the Hadoop cluster.
- **Key Functions:**
  1. Instantiates and configures a `Job` (or `JobConf` in old API) object with cluster configuration properties.
  2. Explicitly binds the Mapper, Reducer, Combiner, and Partitioner classes.
  3. Defines input/output key-value data types (e.g., `Text`, `IntWritable`) and specifies input/output HDFS file paths before calling `job.waitForCompletion(true)`.

</details>

---

<details open>
<summary><b>Q6 — What is a Record Reader? Why is it required in MapReduce? [2M]</b></summary>

<br>

- **Definition:** A **RecordReader** is an internal Hadoop component (instantiated by `InputFormat`) that breaks down raw byte splits into discrete records.
- **Why It Is Required:**
  1. Input Splits in HDFS are simply arbitrary byte boundaries that do not respect logical record boundaries (e.g., text lines may be cut in half).
  2. The `RecordReader` (e.g., `LineRecordReader`) identifies delimiters (like newlines `\n`) and converts raw byte streams into structured `<Key, Value>` pairs (e.g., `<ByteOffset, TextLine>`) that the Mapper's `map()` method can consume.

</details>

---

<details open>
<summary><b>Q7 — What is a Combiner? How does it improve MapReduce performance? [2M]</b></summary>

<br>

- **Definition:** A **Combiner** (often termed a **Semi-Reducer** or **Mini-Reducer**) is an optional optimization component that executes locally on the Mapper node immediately following map execution.
- **How It Improves Performance:**
  1. **Reduces Network Congestion:** It pre-aggregates intermediate key-value pairs locally on each machine (e.g., combining ten `<hadoop, 1>` pairs into `<hadoop, 10>`) before the Shuffle phase.
  2. **Lightens Reducer Workload:** Drastically slashes the total volume of data serialized and transferred across the network to reducers, eliminating bandwidth bottlenecks.

</details>

---

<details open>
<summary><b>Q8 — List any two differences between the old and new Hadoop MapReduce APIs. [2M]</b></summary>

<br>

| Feature | 🆕 New Hadoop API (`mapreduce`) | 🔁 Old Hadoop API (`mapred`) |
|:---|:---|:---|
| **Package** | `org.apache.hadoop.mapreduce` | `org.apache.hadoop.mapred` |
| **Class Architecture** | Uses **abstract classes** for Mapper & Reducer | Uses Java **interfaces** for Mapper & Reducer |
| **Communication / Context** | Uses a unified **`Context`** object for I/O and reporting | Uses separate **`OutputCollector`** and **`Reporter`** |
| **Job Configuration** | Handled via **`Job`** and **`Configuration`** classes | Handled via **`JobConf`** and **`JobClient`** classes |

</details>

---

<details open>
<summary><b>Q9 — Explain the significance of key-value pairs in Hadoop MapReduce. [2M]</b></summary>

<br>

- **Uniform Data Representation:** All data entering, exiting, or passing between phases of a MapReduce pipeline is strictly structured as `<Key, Value>` tuples ($K_1, V_1 	o K_2, V_2 	o K_3, V_3$).
- **Deterministic Routing & Grouping:** The **Key** acts as the universal routing token. The framework hashes the key to assign partitions, sorts records by key, and groups identical keys together, ensuring all values associated with a specific entity land on the same reducer node for correct aggregation.

</details>

---

<details open>
<summary><b>Q10 — What are the input and output components of a Mapper? [2M]</b></summary>

<br>

- **Input Components to Mapper ($K_1, V_1$):**
  - **$K_1$ (Input Key):** Typically the byte offset of the beginning of the line from the start of the file (type: `LongWritable`).
  - **$V_1$ (Input Value):** The actual text content of that specific record or line (type: `Text`).
- **Output Components from Mapper ($K_2, V_2$):**
  - **$K_2$ (Intermediate Key):** Extracted target attribute, category, or word (e.g., `Text` representing a word or year).
  - **$V_2$ (Intermediate Value):** Extracted metric, count, or measurement (e.g., `IntWritable` representing frequency count `1` or temperature `32`).

</details>

---

## 🟢 Unit IV: Stream Memory and Spark (Part-A)

> **Syllabus:** Stream Computing · Stream Data Model · Architecture · Stream vs Batch · Data Streams · Sampling · Filtering & Applications · Counting Distinct Elements · Spark · Spark Architecture · RDDs · Transformation vs Action

---

<details open>
<summary><b>Q1 — Define stream computing. [2M]</b></summary>

<br>

- **Definition:** **Stream computing** (or stream processing) is a real-time computing paradigm designed to ingest, process, enrich, and analyze continuous, high-velocity, and potentially infinite streams of data as they arrive, before they are stored to disk.
- **Key Characteristic:** Analyzes data **in motion** with sub-second latency using single-pass algorithms, unlike traditional batch paradigms that store data on disk and analyze it **at rest**.

</details>

---

<details open>
<summary><b>Q2 — What is a stream data model? [2M]</b></summary>

<br>

- **Definition:** The **Stream Data Model** is a big data architecture and conceptual model that handles data as an unbounded, continuous, time-ordered sequence of tuples arriving at variable and rapid rates.
- **Key Characteristics:**
  1. The total volume of data is infinite and cannot fit entirely into main memory or secondary disk storage.
  2. Data elements can only be examined once (single-pass); past records are discarded unless held in a temporary sliding window buffer.

</details>

---

<details open>
<summary><b>Q3 — List the key components of stream processing architecture. [2M]</b></summary>

<br>

1. **Data Ingestion / Sources Layer:** Connectors and messaging queues (e.g., Apache Kafka, Flume, AWS Kinesis) capturing raw real-time events from IoT devices, logs, and clickstreams.
2. **Stream Processing Engine (DSMS):** Distributed engine executing continuous queries, filtering, sliding window aggregations, and joins (e.g., Spark Streaming, Apache Flink, Storm).
3. **Working / Main Memory Storage:** Fast, volatile RAM buffer maintaining in-flight window states and temporary stateful caches.
4. **Output Sinks & Alerting Layer:** Real-time dashboards, notification queues, and long-term storage sinks (HDFS, NoSQL databases).

</details>

---

<details open>
<summary><b>Q4 — Differentiate between stream processing and batch processing. [2M]</b></summary>

<br>

| Feature | ⚡ Stream Processing | 📦 Batch Processing |
|:---|:---|:---|
| **Data Scope** | Unbounded, continuous real-time streams | Bounded, static chunks accumulated over intervals |
| **Latency** | Low latency (milliseconds to seconds) | High latency (minutes, hours, or overnight) |
| **Data State** | Data in **motion** (processed on the fly) | Data at **rest** (stored on disk/HDFS first) |
| **Access Model** | Single-pass (one-look algorithm) | Multi-pass random access disk I/O |
| **Example Tools** | Apache Spark Streaming, Apache Flink | Hadoop MapReduce, Apache Hive |

</details>

---

<details open>
<summary><b>Q5 — What is meant by data streams? [2M]</b></summary>

<br>

- **Definition:** A **data stream** is a continuous, rapid, time-ordered sequence of discrete data elements (tuples) generated automatically and asynchronously by external sources.
- **Core Attributes:**
  - **High Velocity:** Arrives at unpredictable, bursts of high speed.
  - **Unbounded Volume:** Has no predefined beginning or termination.
  - **Volatile:** Value diminishes rapidly over time; must be acted upon immediately.

</details>

---

<details open>
<summary><b>Q6 — Define sampling in stream data processing. [2M]</b></summary>

<br>

- **Definition:** **Stream sampling** is the process of extracting a statistically unbiased, representative subset of data elements from a continuous, high-volume data stream when memory or computing constraints prevent storing or processing the full stream.
- **Core Goal:** To answer statistical queries (such as mean, distribution, or frequency) with high precision using a small, bounded memory footprint (e.g., using Reservoir Sampling or Hash-based Sampling).

</details>

---

<details open>
<summary><b>Q7 — State any two advantages of sampling data in a stream. [2M]</b></summary>

<br>

1. **Drastically Reduces Memory & Storage Costs:** Permits high-throughput real-time stream analysis within limited main memory (RAM) without overflowing memory buffers or requiring massive disk arrays.
2. **Accelerates Real-Time Response Time:** Reduces computational load, allowing stream engines to compute statistics, detect patterns, and trigger low-latency alerts in milliseconds.

</details>

---

<details open>
<summary><b>Q8 — What is filtering in stream processing? [2M]</b></summary>

<br>

- **Definition:** **Stream filtering** is the process of evaluating incoming stream tuples against a set of target criteria or predicates in real time, accepting only tuples that match the criteria while discarding non-matching tuples.
- **Mechanism:** Often implemented using space-efficient probabilistic data structures such as **Bloom Filters** when the allowed key set is too large to fit in main memory.

</details>

---

<details open>
<summary><b>Q9 — Mention any two applications of stream filtering. [2M]</b></summary>

<br>

1. **Email Spam Detection:** Checking incoming email sender addresses against a massive set of known spam/phishing email addresses using Bloom Filters in main memory to block malicious messages before delivery.
2. **Network Intrusion Detection & Fraud Prevention:** Inspecting live network packets or financial transaction streams to filter out normal traffic and immediately flag blacklisted IP addresses or unauthorized transaction patterns.

</details>

---

<details open>
<summary><b>Q10 — What is meant by counting distinct elements in a stream? [2M]</b></summary>

<br>

- **Definition:** Counting distinct elements refers to determining or estimating the number of unique, non-duplicate elements (cardinality) observed in a continuous, high-volume data stream over time.
- **Algorithmic Solution:** Since keeping a complete set of seen items in memory causes memory exhaustion, stream algorithms such as the **Flajolet-Martin (FM) algorithm** use hash functions and count trailing zeros to estimate cardinality with $O(\log N)$ bits of memory.

</details>

---

<details open>
<summary><b>Q11 — Define Apache Spark. [2M]</b></summary>

<br>

- **Definition:** **Apache Spark** is an open-source, general-purpose distributed cluster-computing framework designed for ultra-fast large-scale data processing and analytics.
- **Key Advantage:** It performs **in-memory computing** by caching data partitions in RAM across iterations, making it up to **100× faster** than Hadoop MapReduce for iterative machine learning, graph processing, and interactive queries.

</details>

---

<details open>
<summary><b>Q12 — List the major components of Spark architecture. [2M]</b></summary>

<br>

1. **Driver Program:** The central coordinator running the application's `main()` method, creating `SparkContext`, and translating user code into a Directed Acyclic Graph (DAG) of stages and tasks.
2. **Cluster Manager:** Allocates and manages cluster hardware resources (e.g., Standalone, Apache Mesos, Hadoop YARN, or Kubernetes).
3. **Executors (Worker Nodes):** Distributed worker processes running on cluster nodes that execute assigned individual tasks and store cached RDD partitions in RAM/disk.

</details>

---

<details open>
<summary><b>Q13 — What is Resilient Distributed Dataset (RDD)? [2M]</b></summary>

<br>

- **Definition:** An **RDD** (Resilient Distributed Dataset) is the foundational data abstraction in Apache Spark.
- **Four Core Properties:**
  1. **Resilient:** Fault-tolerant; automatically recovers lost partitions using lineage graphs (DAG) without data replication.
  2. **Distributed:** Data is split into logical partitions distributed across cluster nodes for parallel processing.
  3. **Dataset:** A collection of partitioned records containing primitive types, tuples, or objects.
  4. **Immutable:** Read-only once created; new RDDs are formed only through transformations.

</details>

---

<details open>
<summary><b>Q14 — Differentiate between transformation and action operations in Spark RDD. [2M]</b></summary>

<br>

| Feature | 🔄 Transformation | ⚡ Action |
|:---|:---|:---|
| **Definition** | Creates a new RDD from an existing RDD | Computes a final value/result from an RDD |
| **Execution Model** | **Lazy evaluation** (recorded in DAG lineage, not executed immediately) | **Eager execution** (triggers execution of all lineage transformations) |
| **Return Value** | Returns a new **`RDD`** pointer | Returns **non-RDD values** (e.g., Array, Int, String) or writes to disk |
| **Common Examples** | `map()`, `filter()`, `flatMap()`, `groupByKey()` | `collect()`, `count()`, `first()`, `saveAsTextFile()` |

</details>

---

## 🟡 Unit V: Pig and Hive (Part-A)

> **Syllabus:** Apache Pig · Pig Latin Purpose · Architecture · Application Flow · Relations · Data Types · Local vs Distributed Mode · Grunt Shell · Execution Interfaces · Apache Hive · Hive Architecture · Hive Data Types · Hive Database · Managed vs External Tables

---

<details open>
<summary><b>Q1 — What is Apache Pig? [2M]</b></summary>

<br>

- **Definition:** **Apache Pig** is a high-level dataflow platform and execution runtime built on top of Apache Hadoop designed for analyzing and transforming large-scale datasets.
- **Core Feature:** Provides a procedural scripting language called **Pig Latin**, allowing users to write data transformations without having to write low-level Java MapReduce code.

</details>

---

<details open>
<summary><b>Q2 — State the purpose of Pig Latin in Hadoop. [2M]</b></summary>

<br>

- **Purpose:**
  1. **Simplifies Big Data Programming:** Enables developers to express complex data extraction, filtering, joining, and grouping operations in a few concise, readable statements (reducing ~200 lines of Java MapReduce to ~10 lines of Pig Latin).
  2. **Automatic Translation & Optimization:** The Pig compiler automatically translates and optimizes Pig Latin scripts into physical Directed Acyclic Graphs (DAGs) of MapReduce/Tez/Spark jobs.

</details>

---

<details open>
<summary><b>Q3 — List the major components of Pig Architecture. [2M]</b></summary>

<br>

1. **Parser:** Checks Pig Latin script syntax and data types, generating a Logical Plan (DAG).
2. **Optimizer:** Applies logical optimizations (such as projection pushdown and early filter pushdown) to minimize data movement.
3. **Compiler:** Converts the optimized logical plan into a sequence of physical MapReduce jobs.
4. **Execution Engine:** Submits and coordinates the generated MapReduce jobs on the Hadoop cluster.
5. **Grunt Shell & Pig Server:** Interactive CLI and client application runtime interface.

</details>

---

<details open>
<summary><b>Q4 — What is the Pig Latin application flow? [2M]</b></summary>

<br>

- **Application Pipeline:**
  $$	ext{1. LOAD (from HDFS)} \longrightarrow 	ext{2. TRANSFORM (FILTER / GROUP / FOREACH / JOIN)} \longrightarrow 	ext{3. DUMP (Display) or STORE (Save to HDFS)}$$
- **Lazy Evaluation:** Pig Latin statements are parsed and validated into an execution graph, but no data processing occurs until a terminal action like `DUMP` (print to console) or `STORE` (write to HDFS) is executed.

</details>

---

<details open>
<summary><b>Q5 — Define relation in Pig Latin. [2M]</b></summary>

<br>

- **Definition:** In Pig Latin, a **Relation** is an outer data structure representing an unordered, unbounded collection (bag) of **tuples**.
- **Key Characteristics:**
  - A relation does not require an enforced schema (can be schema-free or strongly typed).
  - Relations are **immutable**: applying operators (like `FILTER` or `FOREACH`) never modifies the existing relation; it constructs a new resulting relation.

</details>

---

<details open>
<summary><b>Q6 — What are the basic data types supported by Pig Latin? [2M]</b></summary>

<br>

1. **Simple / Primitive Types:**
   - `int` (32-bit signed integer)
   - `long` (64-bit signed integer)
   - `float` (32-bit floating point)
   - `double` (64-bit floating point)
   - `chararray` (string format in UTF-8)
   - `bytearray` (raw blob / byte array)
   - `boolean` (`true`/`false`)
   - `datetime` (date and time with timezone)
2. **Complex Types:**
   - **Tuple:** An ordered sequence of fields (e.g., `(John, 25)`).
   - **Bag:** An unordered collection of tuples (e.g., `{(John, 25), (Alice, 30)}`).
   - **Map:** Key-value lookup pairs (e.g., `['name'#'John', 'age'#'25']`).

</details>

---

<details open>
<summary><b>Q7 — Differentiate between Local Mode and Distributed Mode in Pig. [2M]</b></summary>

<br>

| Feature | 💻 Local Mode | 🌐 Distributed Mode (MapReduce Mode) |
|:---|:---|:---|
| **Command** | `pig -x local` | `pig` or `pig -x mapreduce` |
| **Execution Environment** | Runs inside a single local JVM process | Runs distributed across the nodes of a Hadoop cluster |
| **File System Used** | Reads and writes from the **Local File System** (`file:///`) | Reads and writes from **HDFS** (`hdfs://`) |
| **Primary Use Case** | Fast local testing, debugging, and small datasets | Production data processing on massive Big Data clusters |

</details>

---

<details open>
<summary><b>Q8 — What is the role of Grunt Shell in Pig? [2M]</b></summary>

<br>

- **Role:**
  1. The **Grunt Shell** is the interactive command-line interface (CLI) provided by Apache Pig for typing and evaluating Pig Latin statements interactively line by line.
  2. It allows developers to test syntax, inspect data schemas using `DESCRIBE`, check query execution plans using `EXPLAIN`, illustrate intermediate tuples using `ILLUSTRATE`, and navigate HDFS directly.

</details>

---

<details open>
<summary><b>Q9 — List the different interfaces available for executing Pig scripts. [2M]</b></summary>

<br>

1. **Grunt Shell (Interactive Mode):** Interactive command line where Pig Latin commands are entered and executed step by step (`grunt>`).
2. **Script Mode (Batch Mode):** Executing an external `.pig` script file from the terminal (e.g., `pig script.pig` or `pig -x local script.pig`).
3. **Embedded Mode:** Embedding Pig Latin programs into Java, Python, or JavaScript applications via the `PigServer` API class.

</details>

---

<details open>
<summary><b>Q10 — What is Apache Hive? [2M]</b></summary>

<br>

- **Definition:** **Apache Hive** is an open-source distributed data warehouse software system built on top of Apache Hadoop used for summarizing, querying, and analyzing massive structured datasets.
- **Key Advantage:** Translates a SQL-like declarative language called **HiveQL (HQL)** into execution pipelines of MapReduce, Apache Tez, or Apache Spark jobs, allowing SQL users to query petabytes of HDFS data without writing Java code.

</details>

---

<details open>
<summary><b>Q11 — Mention the key components of Hive Architecture. [2M]</b></summary>

<br>

1. **User Interface / Hive Clients:** CLI, Beeline, Hive Web UI, and external applications connecting through JDBC/ODBC drivers via Thrift Server.
2. **Driver:** Coordinates query execution lifecycle; contains the **Compiler** (parses HQL into AST), **Optimizer** (optimizes execution plan), and **Executor** (submits tasks).
3. **Metastore:** Central relational database (e.g., MySQL or Derby) that stores table schemas, partition information, column data types, and HDFS file locations.
4. **Execution Engine:** Hadoop cluster runtime (MapReduce, Tez, or Spark) executing physical tasks against data stored in HDFS.

</details>

---

<details open>
<summary><b>Q12 — What are Hive data types? Give two examples. [2M]</b></summary>

<br>

- **Definition:** Hive provides SQL-standard primitive and complex data types to structure HDFS data:
  1. **Primitive Data Types:**
     - Numeric: `INT`, `BIGINT`, `FLOAT`, `DOUBLE` (e.g., `emp_id INT`, `salary DOUBLE`)
     - String: `STRING`, `VARCHAR(50)`, `CHAR(10)` (e.g., `emp_name STRING`)
     - Date/Time: `TIMESTAMP`, `DATE`
  2. **Complex Data Types:**
     - `ARRAY`: Ordered collection of elements of same type (e.g., `skills ARRAY<STRING>`).
     - `MAP`: Unordered key-value pairs (e.g., `preferences MAP<STRING, STRING>`).
     - `STRUCT`: Group of named fields (e.g., `address STRUCT<city:STRING, pincode:INT>`).

</details>

---

<details open>
<summary><b>Q13 — What is the purpose of a Hive database? [2M]</b></summary>

<br>

- **Purpose:**
  1. **Logical Isolation & Namespace:** Acts as a catalog container to group related tables, views, and partitions together, preventing table name collisions across projects or teams.
  2. **HDFS Storage Organization:** Establishes a distinct directory hierarchy in HDFS (by default under `/user/hive/warehouse/<database_name>.db/`) with associated user access and permission controls.

</details>

---

<details open>
<summary><b>Q14 — Question Bank Note [2M]</b></summary>

<br>

> **Note:** In the official question bank file `BDA QB-CSD.docx`, row 14 of Table 4 (Unit V Part-A) is empty. The question numbering continues directly to Question 15 below.

</details>

---

<details open>
<summary><b>Q15 — Differentiate between Managed Table and External Table. [2M]</b></summary>

<br>

| Feature | 🏢 Managed Table (Internal Table) | 🌍 External Table |
|:---|:---|:---|
| **DDL Syntax** | `CREATE TABLE table_name (...)` | `CREATE EXTERNAL TABLE table_name (...) LOCATION 'hdfs_path'` |
| **Data Ownership** | Hive has full ownership of both schema metadata and actual data | Hive owns only metadata; actual data files are owned externally |
| **Default Location** | Stored in Hive warehouse directory (`/user/hive/warehouse/`) | Stored in custom external HDFS or S3 directory path |
| **`DROP TABLE` Impact** | Deletes **both** table metadata in Metastore **and** actual data files in HDFS 🗑️ | Deletes **only** metadata in Metastore; **actual HDFS data files remain intact** ✅ |

</details>

---



# 📌 PART-B: Long Answer Questions (5 Marks Each)

---

## 🔵 Unit III — Writing MapReduce Programs

> **Syllabus:** A Weather Dataset · Hadoop API (Old & New) · Driver · Mapper · Reducer · RecordReader · Combiner · Partitioner

---

<details>
<summary><b>Q1 & Q2 — Describe the steps involved in processing weather data using the MapReduce framework.</b></summary>

<br>

**MapReduce** is a programming model used for processing and generating large datasets in a parallel, distributed manner across a cluster.

### 🌦️ Weather Dataset Overview

The classic **NCDC (National Climatic Data Center)** weather dataset consists of semi-structured flat ASCII text logs collected hourly by global weather sensors. It records:
- Temperature, humidity, rainfall, wind speed, atmospheric pressure
- Timestamps from different weather stations

### ⚙️ Steps in MapReduce Processing

**Step 1 — Input Splitting**
> The large weather dataset is divided into smaller input splits. Each split is processed by a separate mapper independently.

**Step 2 — Map Phase**
> Each mapper processes one input split, extracts the required fields, and emits key-value pairs.

```
Input:
  Hyderabad, 2025-01-01, 30°C
  Hyderabad, 2025-01-02, 32°C
  Delhi,     2025-01-01, 18°C

Mapper Output:
  (Hyderabad, 30)
  (Hyderabad, 32)
  (Delhi, 18)
```

**Step 3 — Shuffle and Sort Phase**
> The framework groups all values having the same key and sorts automatically before sending to reducers.

```
Hyderabad → [30, 32]
Delhi     → [18]
```

**Step 4 — Reduce Phase**
> The reducer performs computations (average, max, min) for each location.

```
Reducer Output:
  Hyderabad → Average Temperature = 31°C
  Delhi     → Average Temperature = 18°C
```

**Step 5 — Output**
> Final output is stored in HDFS.

### ✅ Applications

| Use Case | Description |
|---|---|
| Average Temperature | Compute city-wise average temperature |
| Max/Min Detection | Find hottest/coldest day |
| Rainfall Analysis | Aggregate rainfall by region |
| Climate Trend | Detect patterns over years |

</details>

---

<details>
<summary><b>Q3 — Compare the Old Hadoop API and the New Hadoop API used in the MapReduce framework.</b></summary>

<br>

> **Old API** → `org.apache.hadoop.mapred` (uses Java interfaces)
> **New API** → `org.apache.hadoop.mapreduce` (uses abstract classes, introduced in Hadoop 0.21)

| Feature | 🆕 New API (`mapreduce`) | 🔁 Old API (`mapred`) |
|:---|:---|:---|
| **Package** | `org.apache.hadoop.mapreduce` | `org.apache.hadoop.mapred` |
| **Mapper & Reducer** | Abstract classes | Interfaces |
| **Communication** | Single `Context` object | Separate `OutputCollector` & `Reporter` |
| **Job Control** | `Job` class | `JobClient` class |
| **Job Configuration** | `Configuration` class | `JobConf` (extends Configuration) |
| **Output File Names** | `part-m-nnnnn` / `part-r-nnnnn` | `part-nnnnn` |
| **Reduce values type** | `java.lang.Iterable` | `java.lang.Iterator` |
| **Execution control** | Both Mapper & Reducer override `run()` | Only Mapper via `MapRunnable` |
| **Status** | Modern standard ✅ | Backward compatibility only ⚠️ |

> **Key Takeaway:** The New API is more flexible, modular, and extensible without breaking older implementations.

</details>

---

<details>
<summary><b>Q4 — Explain the role of the Driver class in a Hadoop MapReduce program with a suitable example.</b></summary>

<br>

The **Driver code** runs on the **client machine** and is responsible for building the configuration of the job and submitting it to the Hadoop Cluster.

### 📋 Responsibilities of Driver

1. Accepts command-line arguments (input/output paths from HDFS)
2. Creates a `JobConf` / `Job` object to configure the MapReduce job
3. Specifies Mapper, Reducer, Combiner, Partitioner classes
4. Sets input/output key-value types
5. Submits the job to the cluster

### 💻 Sample Driver Code

```java
public class MyDriver {
    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(MyDriver.class);
        conf.setJobName("SampleJobName");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
```

> ⚠️ **Note:** The output directory must NOT exist before running. It is created after successful execution.

</details>

---

<details>
<summary><b>Q5 — Write and explain a Mapper program for processing weather dataset records in Hadoop.</b></summary>

<br>

The **Mapper** processes each input record independently and emits intermediate key-value pairs.

### 🔄 Role of Mapper

- Reads input data record by record
- Extracts station name and temperature
- Emits `<station, temperature>` key-value pairs

### 💻 Mapper Code (New API)

```java
public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text station     = new Text();
    private IntWritable temp = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line     = value.toString();
        String[] fields = line.split(",");

        if (fields.length >= 3) {
            station.set(fields[0].trim());
            int temperature = Integer.parseInt(fields[2].trim().replace("°C", ""));
            temp.set(temperature);
            context.write(station, temp);
        }
    }
}
```

| Input | Output |
|---|---|
| `Hyderabad, 2025-01-01, 30` | `(Hyderabad, 30)` |
| `Delhi, 2025-01-01, 18` | `(Delhi, 18)` |

> The Mapper's output is passed to the **Shuffle and Sort** phase before reaching the Reducer.

</details>

---

<details>
<summary><b>Q6 — Develop a Reducer program to calculate the maximum temperature from a weather dataset and explain its working.</b></summary>

<br>

The **Reducer** takes sorted and grouped key-value pairs and computes the final result.

### 💻 Reducer Code

```java
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
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

### 🔍 Working

| Step | Description |
|:---:|---|
| 1️⃣ | Receives `key` = station name, `values` = list of temperatures |
| 2️⃣ | Iterates through all temperature values for each station |
| 3️⃣ | Finds the maximum temperature |
| 4️⃣ | Writes `<station, maxTemperature>` to output |

```
Input:  Hyderabad → [30, 32, 28, 35]
Output: Hyderabad → 35
```

</details>

---

<details>
<summary><b>Q7 — Explain the interaction between Driver, Mapper, and Reducer classes during MapReduce execution.</b></summary>

<br>

### 🔗 Execution Flow

```
┌─────────────────────────────────────────────────┐
│  1. Driver configures & submits job to cluster  │
└────────────────┬────────────────────────────────┘
                 ▼
┌─────────────────────────────────────────────────┐
│  2. InputFormat splits input into InputSplits   │
│  3. RecordReader converts splits to KV pairs    │
│  4. Mapper processes records, emits KV pairs    │
└────────────────┬────────────────────────────────┘
                 ▼
┌─────────────────────────────────────────────────┐
│  5. Shuffle & Sort groups same keys together    │
└────────────────┬────────────────────────────────┘
                 ▼
┌─────────────────────────────────────────────────┐
│  6. Reducer aggregates and writes final output  │
│  7. Output stored in HDFS                       │
└─────────────────────────────────────────────────┘
```

| Component | Role |
|---|---|
| **Driver** | Orchestrates the job; configures and submits to cluster |
| **Mapper** | Transformation — reads input, emits intermediate KV pairs |
| **Shuffle & Sort** | Groups values by key, sorts before Reducer |
| **Reducer** | Aggregation — computes final result from grouped values |

> Together they implement the **divide-and-conquer** strategy for distributed data processing.

</details>

---

<details>
<summary><b>Q8 — Describe the functions of Record Reader in Hadoop MapReduce and explain how it converts input data into key-value pairs.</b></summary>

<br>

**Record Reader** reads data from an InputSplit and converts it into key-value pairs for the Mapper.

### ⚙️ Functions of Record Reader

| # | Function |
|:---:|---|
| 1 | Reading input data from the InputSplit |
| 2 | Converting raw input data into key-value pairs |
| 3 | Providing **one record at a time** to the Mapper |
| 4 | Identifying the key and value from each input record |
| 5 | Converting data into a format the Mapper can process |
| 6 | Passing key-value pairs to the Mapper |

### 🔄 How It Works (TextInputFormat)

- **Key** = byte offset of the line (`LongWritable`)
- **Value** = content of the line (`Text`)

```
Input File:
  Line 1: What do you mean by Object
  Line 2: What do you know about Java
  Line 3: What is Java Virtual Machine

Record Reader Output:
  <1, "What do you mean by Object">
  <2, "What do you know about Java">
  <3, "What is Java Virtual Machine">
```

</details>

---

<details>
<summary><b>Q9 — Explain the purpose of a Combiner in Hadoop MapReduce. How does it improve performance?</b></summary>

<br>

**Combiner** is an **optional mini-Reducer** that runs locally on the mapper node **before** data is sent to the Reducer.

### ⚙️ Functions of Combiner

| # | Function |
|:---:|---|
| 1 | Performs **local aggregation** of Mapper output |
| 2 | Combines similar keys and values on the Mapper side |
| 3 | Reduces data transferred from Mapper to Reducer |
| 4 | Reduces network traffic during Shuffle phase |
| 5 | Works as a **mini-Reducer** on Mapper side |
| 6 | Optional — Hadoop may or may not execute it |

### 📊 Performance Impact

```
Without Combiner → 6 pairs transferred over network:
  <What,1> <do,1> <you,1> <What,1> <Java,1> <Java,1>

With Combiner → only 4 pairs transferred:
  <What,2> <do,1> <you,1> <Java,2>  ✅
```

### 💻 Setting Combiner in Code

```java
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);   // Same logic as Reducer
job.setReducerClass(IntSumReducer.class);
```

> ⚠️ The Combiner must have the **same input/output key-value types** as the Reducer.

</details>

---

<details>
<summary><b>Q10 — Discuss the role of a Partitioner in MapReduce. Illustrate how custom partitioning can improve data distribution.</b></summary>

<br>

**Partitioner** determines **which Reducer receives which key-value pair** after the Map phase.

### ⚙️ Functions

- Divides Mapper output into partitions
- Sends each partition to a specific Reducer
- Ensures the **same key always goes to the same Reducer**
- Number of partitions = Number of Reducers

### 🔧 Default vs Custom

| Type | Formula |
|---|---|
| **Default HashPartitioner** | `hash(key) % numReducers` |
| **Custom Partitioner** | User-defined logic |

### 💻 Custom Partitioner Example

```java
public class CityPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String city = key.toString();
        if (city.equals("Hyderabad")) return 0;
        else if (city.equals("Delhi"))    return 1;
        else                              return 2;
    }
}

// Setting in Driver:
job.setPartitionerClass(CityPartitioner.class);
job.setNumReduceTasks(3);
```

> ✅ **Benefit:** All records of the same city go to the same Reducer — improving data locality and reducing redundant processing.

</details>

---

<details>
<summary><b>Q11 — Design a MapReduce solution to find the average temperature from a weather dataset.</b></summary>

<br>

### 💻 Mapper

```java
public class AvgTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        context.write(
            new Text(parts[0].trim()),
            new IntWritable(Integer.parseInt(parts[2].trim()))
        );
    }
}
```

### 💻 Reducer

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

### 📊 Output

```
Hyderabad → 31.0°C
Delhi     → 18.0°C
```

</details>

---

<details>
<summary><b>Q12 — Analyze the complete execution flow of a MapReduce job from input splitting to output generation.</b></summary>

<br>

| Step | Phase | Description |
|:---:|---|---|
| 1️⃣ | **Job Submission** | Driver submits job configuration to the cluster |
| 2️⃣ | **Input Splitting** | InputFormat splits input file into logical InputSplits |
| 3️⃣ | **Record Reader** | Converts each InputSplit into `<key, value>` pairs |
| 4️⃣ | **Map Phase** | Mapper processes records, emits intermediate `<key, value>` |
| 5️⃣ | **Combiner** *(optional)* | Locally aggregates Mapper output to reduce network load |
| 6️⃣ | **Partitioner** | Routes each key-value pair to the appropriate Reducer |
| 7️⃣ | **Shuffle Phase** | Data transferred from Mappers to Reducers across network |
| 8️⃣ | **Sort Phase** | All key-value pairs sorted by key before Reducer |
| 9️⃣ | **Reduce Phase** | Reducer processes `<key, list<values>>`, computes output |
| 🔟 | **Record Writer** | Writes final output to HDFS (`part-r-00000`, etc.) |

### 🗂️ Key Roles

| Component | Role |
|---|---|
| Record Reader | Parses raw input into key-value pairs |
| Combiner | Local pre-aggregation *(optional)* |
| Partitioner | Routes data to correct Reducer |
| Reducer | Final aggregation and output |

</details>

---

<details>
<summary><b>Q13 — Write a complete Hadoop MapReduce program structure explaining all components.</b></summary>

<br>

### 💻 Complete Word Count MapReduce Program (New API)

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    // ── MAPPER ─────────────────────────────────────────────────
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // ── REDUCER (also used as COMBINER) ────────────────────────
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // ── DRIVER ─────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### 🗂️ Component Summary

| Component | Class | Role |
|---|---|---|
| **Driver** | `WordCount.main()` | Configures & submits the job |
| **Mapper** | `TokenizerMapper` | Splits lines into words, emits `<word, 1>` |
| **Combiner** | `IntSumReducer` | Local aggregation before shuffle |
| **Reducer** | `IntSumReducer` | Sums word counts globally |
| **Record Reader** | `TextInputFormat` *(default)* | Reads lines as `<offset, line>` |
| **Partitioner** | `HashPartitioner` *(default)* | Routes keys to Reducers by hash |

</details>

---

## 🟢 Unit IV — Stream Memory and Spark

> **Syllabus:** Stream Concepts · Stream Data Model · Stream Computing · Sampling · Filtering · Counting Distinct Elements · Apache Spark · Spark Architecture · Spark Installation · Spark RDD · RDD Operations

---

<details>
<summary><b>Q1 — Explain the Stream Data Model and Architecture.</b></summary>

<br>

**Stream Data Model** is a big data architecture designed for handling **continuous, high-velocity, and potentially infinite flows of data in real-time**.

### 📌 Key Characteristics

| Property | Description |
|---|---|
| **Continuous** | Data arrives non-stop, cannot be fully stored |
| **On-the-fly** | Systems process elements using limited working storage |
| **Summarized** | Only compact summaries or recent windows are stored |

### 🏗️ Data Stream Management System (DSMS)

```
┌──────────────────────────────────────────────────────────┐
│             Data Stream Management System                │
│                                                          │
│  Stream 1 ──►┐                                           │
│  Stream 2 ──►│  Stream Processor  ──► Query Results      │
│  Stream N ──►│  (Working Memory)                        │
│              └──► Archival Store (read-only backup)      │
└──────────────────────────────────────────────────────────┘
```

### 📡 Types of Stream Data

| Type | Example | Volume |
|---|---|---|
| **Sensor Data** | Ocean GPS sensors | 3.5 TB/day per million sensors |
| **Image Data** | London surveillance cameras | 6 million streams |
| **Internet Traffic** | IP packets at network switches | Real-time routing |
| **Web Traffic** | Google search queries | Hundreds of millions/day |

### 🔍 Stream Query Types

| Type | Description | Example |
|---|---|---|
| **Standing Query** | Permanently executing, continuous output | Average of last 24 readings |
| **Ad-hoc Query** | One-time query on current window | Count unique users this month |

</details>

---

<details>
<summary><b>Q2 — Describe Streaming Computing with a suitable example.</b></summary>

<br>

**Stream Computing** ingests, processes, and analyzes **continuous, high-volume data streams in real time**, before the data is stored to disk.

### ⚖️ Batch Processing vs Stream Computing

| Aspect | Batch Processing | Stream Computing |
|---|---|---|
| **Data arrival** | Collected in chunks | Continuous, non-stop |
| **Processing time** | After storing data | While data is in motion |
| **Latency** | High (minutes/hours) | Low (milliseconds) |
| **Use case** | Historical analysis | Real-time decisions |

### 🏗️ Core Architecture

```
Data Sources ──► Ingestion Layer ──► Processing Engine ──► Output/Action
  (Sensors,         (Kafka,            (Spark, Flink,        (Dashboard,
   Web logs)         Flume)             Storm)                Alerts, DB)
```

### 💡 Example — Real-Time Stock Price Monitoring

```sql
SELECT AVG(price)
FROM   stock_stream [ROWS 10]
WHERE  price > 100.0
```

> Continuously calculates average of the last 10 stock prices above 100.

### 🛠️ Popular Frameworks

| Framework | Use Case |
|---|---|
| Apache Spark Streaming | Micro-batch stream processing |
| Apache Flink | True real-time stream processing |
| Apache Kafka Streams | Event-driven applications |

</details>

---

<details>
<summary><b>Q3 — Explain the concept of sampling data in a stream and discuss its significance.</b></summary>

<br>

**Sampling** selects a **representative subset** of a data stream when processing all data is impractical.

### ❓ Why Sampling is Needed

- Data arrives too fast to process everything
- Memory is limited; cannot store the entire stream
- Need statistically valid conclusions from partial data

### 🔢 Types of Sampling

**1. Uniform Sampling**
- Each element included with equal probability `p`
- Simple but may miss rare or important patterns

**2. Reservoir Sampling**
- Maintains a fixed-size reservoir of `k` elements
- Each new element has `k/n` probability of selection

```
Algorithm:
  Step 1: Fill reservoir with first k elements
  Step 2: For each element i (i > k):
            j = random integer in [1, i]
            if j <= k: replace reservoir[j] with element i
```

### 📈 Significance of Sampling

| Benefit | Explanation |
|---|---|
| Reduced Memory | Only k items stored at any time |
| Real-Time Processing | Enables high-speed stream processing |
| Statistical Validity | Mathematically proven unbiased estimates |
| Scalability | Handles infinite or extremely large streams |

> **Example:** Web analytics — sample 1% of clicks to estimate total unique users with acceptable error.

</details>

---

<details>
<summary><b>Q4 & Q5 — Illustrate the working of Reservoir Sampling for processing stream data.</b></summary>

<br>

**Reservoir Sampling** selects `k` random items from a stream of unknown size `n`, ensuring **equal probability of selection** for every element.

### 📐 Algorithm

```
Input:  Stream of elements, reservoir size k
Output: k randomly selected elements

1. Place first k elements into reservoir[]
2. For each new element at position i (i > k):
      j = random integer in range [1, i]
      if j <= k:
          reservoir[j] = current element   ← replace
      else:
          discard current element
```

### 🔍 Step-by-Step Example

Stream: `[a, b, c, d, e, f, g, h]`, `k = 3`

| Step | New Element | Action | Reservoir |
|:---:|:---:|---|:---:|
| 1 | a | Add | `[a, -, -]` |
| 2 | b | Add | `[a, b, -]` |
| 3 | c | Add | `[a, b, c]` |
| 4 | d | j from [1,4]; if j<=3, replace | Updated if j<=3 |
| 5 | e | j from [1,5]; if j<=3, replace | Updated if j<=3 |

### 📊 Probability Analysis

```
Probability that any element i is in the final reservoir:
  P = k / n

This guarantees perfectly unbiased random sampling.
```

### ✅ Advantages

| Advantage | Description |
|---|---|
| **O(k) Memory** | Only k items stored regardless of stream size |
| **Unknown Length** | Works without knowing stream size in advance |
| **Unbiased** | Mathematically proven equal probability |
| **Infinite Streams** | Applicable to both finite and infinite streams |

</details>

---

<details>
<summary><b>Q6 & Q7 — Explain techniques for filtering streams / Demonstrate how stream filtering improves efficiency.</b></summary>

<br>

**Stream Filtering** allows only elements whose keys belong to a set S to pass through — without storing S entirely in memory.

### 🔧 Technique 1: Bit-Array Filtering

```
Step 1: Create an array of n bits (all 0)
Step 2: For each key K in set S:
          set bit[hash(K)] = 1
Step 3: For each stream element:
          if bit[hash(element.key)] == 1 → PASS  ✓
          if bit[hash(element.key)] == 0 → REJECT ✗
```

### 🌸 Technique 2: Bloom Filter

**Components:**
1. An array of `n` bits (initially all 0s)
2. `k` hash functions: `h1, h2, ..., hk`
3. A set S of `m` key values

**Initialization:**
```
For each key K in S:
  For each hash function hi:
    Set bit[ hi(K) ] = 1
```

**Testing a stream element with key K:**
```
Check: h1(K), h2(K), ..., hk(K)
  All bits = 1  →  PASS  (may be in S)  ✓
  Any bit  = 0  →  REJECT (definitely NOT in S)  ✗
```

### 📉 False Positive Rate

| # Hash Functions | False Positive Rate |
|:---:|:---:|
| 1 | ~11.75% |
| 2 | ~4.93% |
| 3 | ~2.14% |
| 4 | ~1.16% |

### 🚀 How Filtering Improves Efficiency

| Benefit | Impact |
|---|---|
| Early rejection of irrelevant data | Less processing downstream |
| Compact bit-array vs full set | Significant memory savings |
| No database lookups needed | Real-time filtering |
| Scales to billions of elements | Enterprise-grade performance |

> **Example:** Email spam filter checks sender against a Bloom filter of known spam senders.

</details>

---

<details>
<summary><b>Q8 & Q9 — Explain / Analyze Counting Distinct Elements in Stream Data Analytics.</b></summary>

<br>

### ❓ The Count-Distinct Problem

> How many **different elements** have appeared in the stream?

**Example:** A website counts how many unique users visited in the past month. Each login = one stream element.

### 🔣 Flajolet-Martin (FM) Algorithm

```
For each stream element:
  1. Hash it to a binary number
  2. Count trailing zeros in the hash
  3. Track maximum trailing zeros (maxZeros)

Estimate of distinct elements = 2^maxZeros
```

> **Why it works:** If D distinct elements exist, ~D/2 hashes end in ≥1 trailing zero, ~D/4 end in ≥2, etc.

### 🧮 AMS Algorithm (Alon-Matias-Szegedy) for Moments

Estimates the **second moment** (sum of squares of frequencies):

```
1. Pick a random position i in the stream
2. X.element = element found at position i
3. X.value   = count of subsequent occurrences of X.element
4. Estimate  = n x (2 x X.value - 1)
```

**Example:**
```
Stream: a, b, c, b, d, a, c, d, a, b   (n = 10)
X.element = b, X.value = 3
Estimate  = 10 x (2x3 - 1) = 50
```

### 📊 Challenges

| Challenge | Description |
|---|---|
| Speed | Streams arrive too fast to store all elements |
| Memory | Full hash tables require too much RAM |
| Scale | Multiple streams simultaneously multiply requirements |
| Accuracy | Need approximate answers with bounded error |

### ✅ Solutions

- Use probabilistic data structures (Bloom filters, FM sketches)
- Trade accuracy for memory efficiency
- Use multiple hash functions and take median of estimates

</details>

---

<details>
<summary><b>Q10 — Describe Apache Spark and discuss its advantages over Hadoop MapReduce.</b></summary>

<br>

**Apache Spark** is an open-source, distributed computing framework designed for fast, **in-memory** data processing.

### 🔑 Key Features

| Feature | Description |
|---|---|
| **In-Memory Processing** | Up to 100x faster than Hadoop MapReduce |
| **Unified Engine** | Batch + Streaming + ML + SQL + Graph |
| **Multi-language** | Scala, Java, Python, R |
| **Compatibility** | Works with HDFS, Hive, Cassandra, S3 |

### ⚡ Why MapReduce is Slow

```
MapReduce:
  Read from HDFS → Map → Write to disk → Read from disk → Reduce → Write to HDFS
                           ⚠️ Disk I/O at every step!

Spark RDD:
  Read from HDFS → Map → [In Memory] → Reduce → Write to HDFS
                           ✅ Data stays in RAM between steps!
```

### 🆚 Spark vs MapReduce

| Feature | Apache Spark | Hadoop MapReduce |
|:---|:---:|:---:|
| **Processing Speed** | 100x faster (in-memory) | Disk-based, slower |
| **Ease of Use** | High-level APIs | Java-only (verbose) |
| **Data Processing** | Batch + Stream + ML + SQL | Batch only |
| **Fault Tolerance** | RDD lineage | Data replication |
| **Real-Time** | ✅ Spark Streaming | ❌ Not suitable |
| **Languages** | Scala, Python, Java, R | Java primarily |

</details>

---

<details>
<summary><b>Q11 — Explain Spark Architecture with a neat diagram and discuss the role of its components.</b></summary>

<br>

### 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────┐
│                    Driver Program                    │
│                  (SparkContext)                      │
│  * main() function                                   │
│  * Builds DAG of operations                          │
│  * Schedules tasks to Executors                      │
└──────────────────┬───────────────────────────────────┘
                   │ connects to
                   ▼
┌──────────────────────────────────────────────────────┐
│               Cluster Manager                        │
│       (Standalone / YARN / Mesos / K8s)              │
│  * Allocates resources                               │
│  * Launches Executors on Worker Nodes                │
└──────┬──────────────────────────────┬────────────────┘
       │                              │
       ▼                              ▼
┌──────────────┐              ┌──────────────┐
│  Worker Node │              │  Worker Node │
│ ┌──────────┐ │              │ ┌──────────┐ │
│ │ Executor │ │              │ │ Executor │ │
│ │  Task 1  │ │              │  Task 1   │ │
│ │  Task 2  │ │              │  Task 2   │ │
│ │  Cache   │ │              │  Cache    │ │
│ └──────────┘ │              └──────────── │
└──────────────┘              └──────────────┘
```

### 🔩 Components and Roles

| Component | Role |
|---|---|
| **Driver Program** | Contains `main()`, creates SparkContext, builds task DAG, schedules tasks |
| **SparkContext** | Entry point of any Spark application; coordinates all distributed operations |
| **Cluster Manager** | Allocates resources; types: Standalone, YARN, Mesos, Kubernetes |
| **Worker Nodes** | Physical/virtual machines that host Executors |
| **Executors** | JVM processes that run tasks, store RDD partitions in memory |
| **Tasks** | Smallest unit of work; runs on a single data partition within an Executor |

</details>

---

<details>
<summary><b>Q12 — Discuss the installation and configuration process of Apache Spark.</b></summary>

<br>

### 📋 Prerequisites

| Requirement | Version |
|---|---|
| Java | 8 or higher |
| Scala | Optional (for Scala API) |
| Python | Optional (for PySpark) |

### 🪜 Step-by-Step Installation

**Step 1 — Install Java**
```bash
sudo apt install default-jdk
java -version
```

**Step 2 — Download Apache Spark**
```
Visit:   https://spark.apache.org/downloads/
Select:  Pre-built for Apache Hadoop
File:    spark-3.x.x-bin-hadoop3.tgz
```

**Step 3 — Extract**
```bash
tar -xvzf spark-3.x.x-bin-hadoop3.tgz
mv spark-3.x.x-bin-hadoop3 /opt/spark
```

**Step 4 — Set Environment Variables**
```bash
# Add to ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
source ~/.bashrc
```

**Step 5 — Configure Spark**
```bash
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh

# Edit spark-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
export SPARK_MASTER_HOST=localhost
```

**Step 6 — Test Installation**
```bash
spark-shell
# Success: scala> prompt appears

# Quick test:
val data = Seq(1, 2, 3, 4, 5)
val rdd  = sc.parallelize(data)
rdd.sum()   // Output: 15.0
```

**Step 7 — Run PySpark**
```bash
pyspark
```

</details>

---

<details>
<summary><b>Q13 — Explain the concept, features, and advantages of Spark RDDs.</b></summary>

<br>

**RDD (Resilient Distributed Dataset)** is the **fundamental data abstraction** in Apache Spark.

### 🔤 What the Name Means

| Letter | Stands For | Meaning |
|:---:|---|---|
| **R** | Resilient | Lost partitions auto-reconstructed using lineage graph |
| **D** | Distributed | Data split into partitions across multiple machines |
| **D** | Dataset | Holds any JVM-compatible data type |

### 🔑 Key Features

| Feature | Description |
|---|---|
| **Immutability** | Once created, an RDD cannot be modified |
| **Lazy Evaluation** | Transformations build a DAG; execution only on Action |
| **Fault Tolerance** | Lineage graph allows re-computation without replication |
| **Partitioning** | Data automatically partitioned for parallel execution |
| **In-Memory** | Intermediate results stored in RAM, avoiding disk I/O |
| **Caching** | RDDs can be persisted in memory for reuse |

### 📦 Creating RDDs

```python
# From a list
rdd = sc.parallelize([1, 2, 3, 4, 5])

# From HDFS file
rdd = sc.textFile("hdfs:///data/input.txt")

# From another RDD (transformation)
rdd2 = rdd.map(lambda x: x * 2)
```

### ✅ Advantages

- 100x faster than MapReduce for iterative algorithms
- Supports interactive in-memory queries
- Fault tolerance without expensive data replication
- Works with HDFS, S3, Cassandra, HBase, and more

</details>

---

<details>
<summary><b>Q14 — Differentiate between Narrow and Wide Transformations in Spark RDD operations with examples.</b></summary>

<br>

### 🔀 Narrow Transformation

> All data required to compute a partition comes from a **single parent partition** — no shuffling needed.

```
Parent RDD          Child RDD
[Partition 1] ───► [Partition 1]   One-to-one mapping, no network shuffle ✅
[Partition 2] ───► [Partition 2]
```

**Examples:** `map()`, `filter()`, `flatMap()`, `mapPartitions()`

```python
rdd    = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x + 2)    # [3, 4, 5, 6, 7] — no shuffle ✅
```

### 🔀 Wide Transformation

> Data may come from **multiple parent partitions** — requires a network shuffle.

```
Parent RDD               Child RDD
[Partition 1] ──►─┐
[Partition 2] ──►─┼──► [Partition A]   Many-to-one, shuffle required ⚠️
[Partition 3] ──►─┘
```

**Examples:** `groupByKey()`, `reduceByKey()`, `sortByKey()`, `join()`

```python
pairs   = sc.parallelize([("a",1), ("b",2), ("a",3)])
grouped = pairs.groupByKey()    # shuffles all "a" records to one partition
```

### 📊 Comparison Table

| Feature | Narrow Transformation | Wide Transformation |
|:---|:---:|:---:|
| **Data Dependency** | Single parent partition | Multiple parent partitions |
| **Network Shuffle** | No ✅ | Yes ⚠️ |
| **Speed** | Faster | Slower |
| **Stage Boundary** | No | Yes |
| **Examples** | `map()`, `filter()` | `groupByKey()`, `join()` |

</details>

---

<details>
<summary><b>Q15 — Demonstrate various Spark RDD operations: map(), filter(), reduce(), collect(), flatMap() with examples.</b></summary>

<br>

| Category | Description | Trigger |
|---|---|---|
| **Transformations** | Create new RDDs (lazy) | On Action call |
| **Actions** | Return results / trigger execution | Immediately |

---

### 1️⃣ `map(func)` — Transformation
Applies a function to **every element**.

```python
rdd    = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2)
# result: [2, 4, 6, 8, 10]
```

### 2️⃣ `filter(func)` — Transformation
Returns elements that satisfy a predicate.

```python
rdd  = sc.parallelize([1, 2, 3, 4, 5])
even = rdd.filter(lambda x: x % 2 == 0)
# even: [2, 4]
```

### 3️⃣ `flatMap(func)` — Transformation
Each input element can produce **multiple output elements**.

```python
rdd   = sc.parallelize(["Hello World", "Big Data"])
words = rdd.flatMap(lambda line: line.split(" "))
# words: ["Hello", "World", "Big", "Data"]
```

### 4️⃣ `reduce(func)` — Action
Aggregates all elements into a **single value**.

```python
rdd   = sc.parallelize([1, 2, 3, 4, 5])
total = rdd.reduce(lambda a, b: a + b)
# total: 15
```

### 5️⃣ `collect()` — Action
Returns **all elements** as a list to the Driver.

```python
rdd  = sc.parallelize([1, 2, 3])
data = rdd.collect()
# data: [1, 2, 3]
```

### 🗂️ Additional Operations

| Operation | Type | Description | Example |
|---|:---:|---|---|
| `count()` | Action | Number of elements | `rdd.count()` → `5` |
| `distinct()` | Transformation | Remove duplicates | `rdd.distinct()` |
| `union(other)` | Transformation | Combine two RDDs | `rdd1.union(rdd2)` |
| `intersection()` | Transformation | Common elements | `rdd1.intersection(rdd2)` |
| `sortByKey()` | Transformation | Sort key-value pairs | `pairs.sortByKey()` |
| `take(n)` | Action | First n elements | `rdd.take(3)` |

</details>

---

## 🟡 Unit V — Pig and Hive

> **Syllabus:** Pig Architecture · Pig Latin Flow · ABCs of Pig Latin · Local vs Distributed Mode · Pig Interfaces · Hive Architecture · Hive Data Types · Databases & Tables · Hive DML · HiveQL · Hive Clients

---

<details>
<summary><b>Q1 — Explain the architecture of Apache Pig and discuss the role of its components.</b></summary>

<br>

**Apache Pig** is a high-level data processing platform built on top of Hadoop that simplifies MapReduce using a scripting language called **Pig Latin**.

### 🏗️ Pig Architecture

```
┌──────────────────────────────────────────────┐
│           User (Pig Latin Script)            │
└─────────────────┬────────────────────────────┘
                  ▼
┌──────────────────────────────────────────────┐
│                  Parser                      │
│  * Syntax & semantic checking                │
│  * Produces Logical Plan (DAG)               │
└─────────────────┬────────────────────────────┘
                  ▼
┌──────────────────────────────────────────────┐
│                 Optimizer                    │
│  * Projection & filter pushdown              │
│  * Reduces data volume                       │
└─────────────────┬────────────────────────────┘
                  ▼
┌──────────────────────────────────────────────┐
│                 Compiler                     │
│  * Converts Logical Plan to Physical Plan    │
│  * Determines number of MapReduce jobs       │
└─────────────────┬────────────────────────────┘
                  ▼
┌──────────────────────────────────────────────┐
│              Execution Engine                │
│  * Executes MapReduce jobs on Hadoop cluster │
└─────────────────┬────────────────────────────┘
                  ▼
┌──────────────────────────────────────────────┐
│        Hadoop / MapReduce / HDFS             │
└──────────────────────────────────────────────┘
```

### 🔩 Component Roles

| Component | Role |
|---|---|
| **Parser** | Checks syntax/semantics, creates Logical Plan DAG |
| **Optimizer** | Applies optimizations (pushdowns) to reduce data volume |
| **Compiler** | Converts optimized plan to Physical Plan (MapReduce jobs) |
| **Execution Engine** | Executes jobs on Hadoop cluster, monitors progress |
| **Grunt Shell** | Interactive CLI for testing and debugging Pig Latin |
| **Pig Server** | Enables batch execution via `.pig` script files |

### ✅ Why Use Pig?

| Metric | Without Pig | With Pig |
|---|---|---|
| Code Lines | 200+ lines Java | ~10 lines Pig Latin |
| Skill Required | Java developer | SQL-like scripting |
| Optimization | Manual | Automatic |

</details>

---

<details>
<summary><b>Q2 — Describe the Pig Latin Application Flow with a suitable diagram.</b></summary>

<br>

### 🔄 Flow Diagram

```
┌────────────────────────────┐
│    Input Data (HDFS)       │
└─────────────┬──────────────┘
              │  LOAD
              ▼
┌────────────────────────────┐
│      Raw Relation          │
└─────────────┬──────────────┘
              │  FILTER / FOREACH / JOIN / GROUP
              ▼
┌────────────────────────────┐
│   Transformed Relation     │
└─────────────┬──────────────┘
              │  ORDER / LIMIT
              ▼
┌────────────────────────────┐
│     Sorted Relation        │
└─────────────┬──────────────┘
              │  STORE / DUMP
              ▼
┌────────────────────────────┐
│  Output (HDFS / Console)   │
└────────────────────────────┘
```

### 📝 Step-by-Step Flow

**Step 1 — LOAD**
```pig
students = LOAD '/data/students.txt' USING PigStorage(',')
           AS (id:int, name:chararray, age:int);
```

**Step 2 — FILTER**
```pig
adults = FILTER students BY age >= 18;
```

**Step 3 — GROUP**
```pig
grouped = GROUP adults BY age;
```

**Step 4 — FOREACH / GENERATE**
```pig
result = FOREACH grouped GENERATE group, COUNT($1) AS count;
```

**Step 5 — ORDER BY**
```pig
sorted = ORDER result BY count DESC;
```

**Step 6 — STORE / DUMP**
```pig
STORE sorted INTO '/output/result' USING PigStorage(',');
-- OR:
DUMP sorted;
```

### 📌 Key Properties

| Property | Description |
|---|---|
| **Procedural** | Step-by-step (unlike SQL's declarative style) |
| **Immutable Relations** | Each operation creates a new relation |
| **Lazy Evaluation** | Execution only begins at `DUMP` or `STORE` |

</details>

---

<details>
<summary><b>Q3 — Explain the basic concepts: Working through the ABCs of Pig Latin.</b></summary>

<br>

### 🔤 A — Atoms (Primitive Types)

| Type | Description | Example |
|---|---|---|
| `int` | 32-bit integer | `25` |
| `long` | 64-bit integer | `1000000L` |
| `float` | 32-bit float | `3.14` |
| `double` | 64-bit float | `3.14159` |
| `chararray` | String (UTF-8) | `'John'` |
| `bytearray` | Untyped byte array | *(default)* |
| `boolean` | true / false | `true` |
| `datetime` | Date and time | `2025-01-01` |

### 🔤 B — Bags

> An **unordered collection of tuples** (allows duplicates).

```pig
-- Bag created by GROUP operation:
{ (1, 'Alice'), (2, 'Bob'), (1, 'Charlie') }
```

### 🔤 C — Complex Types

| Type | Description | Example |
|---|---|---|
| **Tuple** | Ordered set of fields | `(1, 'John', 3.5)` |
| **Bag** | Collection of tuples | `{(1,'a'), (2,'b')}` |
| **Map** | Key-value pairs | `['name'#'John', 'age'#'25']` |

### 🔧 Pig Latin Key Operations

| Operation | Description | Example |
|---|---|---|
| `LOAD` | Read data from HDFS | `A = LOAD 'file' AS (...)` |
| `FILTER` | Select rows by condition | `B = FILTER A BY age > 18` |
| `FOREACH` | Transform columns | `C = FOREACH A GENERATE name, age` |
| `GROUP` | Group rows by key | `D = GROUP A BY dept` |
| `JOIN` | Join two relations | `E = JOIN A BY id, B BY id` |
| `ORDER BY` | Sort results | `F = ORDER A BY name ASC` |
| `LIMIT` | Restrict number of rows | `G = LIMIT A 10` |
| `STORE` | Write output to HDFS | `STORE A INTO '/path'` |
| `DUMP` | Print to console | `DUMP A` |

### 💻 Example Script

```pig
-- Count employees per department
emp    = LOAD '/data/emp.txt' USING PigStorage('\t')
         AS (id:int, name:chararray, dept:chararray, salary:float);
grp    = GROUP emp BY dept;
result = FOREACH grp GENERATE group AS dept, COUNT($1) AS num_emp;
DUMP result;
```

</details>

---

<details>
<summary><b>Q4 — Compare Local Mode and Distributed Mode for running Pig scripts.</b></summary>

<br>

| Feature | Local Mode | Distributed Mode |
|:---|:---:|:---:|
| **Command** | `pig -x local` | `pig` or `pig -x mapreduce` |
| **Environment** | Single JVM on local machine | Full Hadoop cluster |
| **File System** | Local filesystem | HDFS |
| **Data Size** | Small datasets only | Terabytes / Petabytes |
| **Startup Speed** | Fast | Slower (cluster overhead) |
| **HDFS Required** | No | Yes |
| **Parallelism** | Single-threaded | Multi-node parallel |
| **Fault Tolerance** | None | Built-in (HDFS replication) |
| **Use Case** | Dev / testing / debugging | Production / large-scale |

### 💻 Examples

**Local Mode:**
```bash
pig -x local
grunt> A = LOAD 'students.txt' AS (id:int, name:chararray);
grunt> DUMP A;
```

**Distributed Mode:**
```bash
pig
grunt> A = LOAD 'hdfs:///data/students.txt' AS (id:int, name:chararray);
grunt> DUMP A;
```

### 🎯 When to Use Which

| Scenario | Recommended Mode |
|---|---|
| Writing/testing a new script | Local Mode |
| Processing production HDFS data | Distributed Mode |
| Exploring a small CSV file | Local Mode |
| ETL pipeline on Hadoop cluster | Distributed Mode |

</details>

---

<details>
<summary><b>Q5 — Discuss the various interfaces available for executing Pig scripts.</b></summary>

<br>

| Interface | Description | Best For |
|---|---|---|
| **Grunt Shell** | Interactive CLI for Pig Latin | Exploration and debugging |
| **Script File** | `.pig` file executed in batch | Production automation |
| **Embedded Mode** | Java `PigServer` API | Application integration |
| **HCatalog** | Connects to Hive Metastore | Schema-aware processing |
| **Web UI (Hue)** | Browser-based Pig editor | Visual development |
| **Apache Zeppelin** | Notebook-style interface | Data science workflows |

### 💻 Interface Examples

**1. Grunt Shell (Interactive)**
```bash
pig -x local
grunt> data = LOAD 'file.txt' AS (line:chararray);
grunt> DUMP data;
```

**2. Script File (Batch)**
```bash
pig -x local myscript.pig     # Local
pig myscript.pig               # Cluster
```

**3. Embedded Mode (Java)**
```java
PigServer pigServer = new PigServer(ExecType.LOCAL);
pigServer.registerQuery("A = LOAD 'data.txt' AS (name:chararray);");
pigServer.registerQuery("B = FILTER A BY name == 'John';");
Iterator<Tuple> result = pigServer.openIterator("B");
```

**4. Hue (Web UI)**
```
URL: http://hadoop-master:8888
Navigate: Query Editors → Pig Editor
```

</details>

---

<details>
<summary><b>Q6 — Illustrate the process of writing and executing a Pig Latin script.</b></summary>

<br>

### 📝 Word Count in Pig Latin

**Input File** (`words.txt`):
```
Hello World
Hello Pig
World is Big
```

**Pig Latin Script** (`wordcount.pig`):
```pig
-- Load input file
lines = LOAD '/input/words.txt' USING TextLoader() AS (line:chararray);

-- Split each line into individual words
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Group occurrences of each word
grp_words = GROUP words BY word;

-- Count each word
word_count = FOREACH grp_words GENERATE group AS word, COUNT($1) AS count;

-- Sort by frequency
result = ORDER word_count BY count DESC;

-- Write to HDFS
STORE result INTO '/output/wordcount' USING PigStorage('\t');
```

**Execute:**
```bash
pig -x local wordcount.pig     # Local testing
pig wordcount.pig               # Production
```

**Output:**
```
Hello    2
World    2
Big      1
is       1
Pig      1
```

### 🔑 Commands Used

| Command | Purpose |
|---|---|
| `LOAD` | Read from HDFS |
| `FOREACH...GENERATE` | Transform each record |
| `FLATTEN` | Unnests bags into flat records |
| `TOKENIZE` | Splits string into bag of words |
| `GROUP BY` | Groups records by key |
| `COUNT` | Counts elements in a bag |
| `ORDER BY` | Sorts results |
| `STORE` | Writes output to HDFS |

</details>

---

<details>
<summary><b>Q7 — Explain the data model and data types supported by Pig Latin.</b></summary>

<br>

### 🏗️ Pig Latin Nested Data Model

```
Dataset (relation)
  └── Bag  { unordered collection of tuples }
        └── Tuple  ( ordered set of fields )
              └── Field  ( atom / complex value )
```

### 📊 Primitive Data Types

| Type | Size | Example |
|---|---|---|
| `int` | 32-bit | `25` |
| `long` | 64-bit | `1000000L` |
| `float` | 32-bit | `3.14` |
| `double` | 64-bit | `3.14159` |
| `chararray` | String | `'John'` |
| `bytearray` | Byte array | *(default untyped)* |
| `boolean` | true/false | `true` |
| `datetime` | Date-time | `2025-01-01T00:00:00` |

### 📦 Complex Data Types

| Type | Description | Example |
|---|---|---|
| **Tuple** | Ordered set of fields | `(1, 'John', 3.5)` |
| **Bag** | Unordered collection of tuples | `{(1,'a'), (2,'b')}` |
| **Map** | Key-value string pairs | `['name'#'John', 'age'#'25']` |

### 💻 Schema Declaration

```pig
emp = LOAD 'emp.txt' USING PigStorage(',')
      AS (
        id     : int,
        name   : chararray,
        salary : float,
        dept   : chararray
      );
```

</details>

---

<details>
<summary><b>Q8 — Describe the architecture of Apache Hive and explain the function of each component.</b></summary>

<br>

**Apache Hive** provides an **SQL-like interface (HiveQL)** to query and analyze data stored in HDFS.

### 🏗️ Hive Architecture

```
┌──────────────────────────────────────────────────────┐
│            Hive Interfaces                           │
│   CLI | Beeline | Web UI | JDBC/ODBC | Thrift       │
└───────────────────┬──────────────────────────────────┘
                    │ User Query (HiveQL)
                    ▼
┌──────────────────────────────────────────────────────┐
│                    Driver                            │
│         (manages query lifecycle & sessions)         │
└────────────┬───────────────────────────┬─────────────┘
             │                           │
             ▼                           ▼
┌─────────────────────┐    ┌──────────────────────────┐
│      Compiler       │◄──►│       Metastore           │
│  * Parse HiveQL     │    │  * Table schemas          │
│  * Logical Plan     │    │  * Column info            │
│  * Physical Plan    │    │  * Partitions             │
│  * MapReduce DAG    │    │  (backed by MySQL/Derby)  │
└──────────┬──────────┘    └──────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│              Execution Engine                        │
│     (submits MapReduce / Tez / Spark jobs)           │
└────────────────────┬─────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────┐
│        Hadoop Cluster (NameNode + DataNodes)         │
│                     HDFS                            │
└──────────────────────────────────────────────────────┘
```

### 🔩 Component Roles

| Component | Role |
|---|---|
| **Hive Interfaces** | Accept HiveQL from users/applications |
| **Driver** | Manages query lifecycle and sessions |
| **Compiler** | Parses HiveQL → Logical Plan → Physical Plan |
| **Metastore** | Stores table schemas, columns, partitions, formats |
| **Execution Engine** | Executes physical plan as MapReduce/Tez/Spark jobs |
| **HDFS** | Actual data storage layer |

### 🔄 Query Execution Workflow

| Step | From → To | Action |
|:---:|---|---|
| 1 | Interface → Driver | Send HiveQL query |
| 2 | Driver → Compiler | Parse & generate plan |
| 3 | Compiler → Metastore | Request metadata |
| 4 | Metastore → Compiler | Return metadata |
| 5 | Compiler → Driver | Send execution plan |
| 6 | Driver → Execution Engine | Execute plan |
| 7 | Engine → Hadoop | Submit MapReduce job |
| 8 | Hadoop → Engine → Interface | Return final results |

</details>

---

<details>
<summary><b>Q9 & Q10 — Explain "Saying Hello to Hive" and Getting Started with Apache Hive.</b></summary>

<br>

### 👋 Introduction to Hive

Apache Hive:
- Sits on top of Hadoop (HDFS + MapReduce)
- Translates HiveQL → MapReduce jobs automatically
- Allows SQL-familiar users to work with big data
- Contains a default database called `default`

### 🚀 Getting Started — Step by Step

**Start Hive CLI**
```bash
hive
```

**Step 1 — Create Database**
```sql
hive> CREATE DATABASE IF NOT EXISTS college;
hive> SHOW DATABASES;
-- Output: college  default
hive> USE college;
```

**Step 2 — Create Table**
```sql
hive> CREATE TABLE IF NOT EXISTS employee (
        eid    INT,
        name   STRING,
        salary FLOAT,
        dept   STRING
      )
      COMMENT 'Employee Details'
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      STORED AS TEXTFILE;
```

**Step 3 — Load Data**
```sql
hive> LOAD DATA LOCAL INPATH '/home/user/emp.csv'
      OVERWRITE INTO TABLE employee;
```

**Step 4 — Verify & Query**
```sql
hive> DESCRIBE employee;
hive> SELECT * FROM employee LIMIT 5;
hive> SELECT dept, COUNT(*) FROM employee GROUP BY dept;
```

</details>

---

<details>
<summary><b>Q11 — Explain various Hive data types with suitable examples.</b></summary>

<br>

### 1️⃣ Integral Types

| Type | Range | Postfix | Example |
|---|---|:---:|---|
| `TINYINT` | -128 to 127 | Y | `10Y` |
| `SMALLINT` | -32768 to 32767 | S | `100S` |
| `INT` | -2³¹ to 2³¹-1 | — | `1000` |
| `BIGINT` | -2⁶³ to 2⁶³-1 | L | `10000L` |

### 2️⃣ String Types

| Type | Max Length | Example |
|---|:---:|---|
| `VARCHAR` | 1 to 65535 | `'Hello World'` |
| `CHAR` | 255 | `'A'` |
| `STRING` | Unlimited | `'Apache Hive'` |

### 3️⃣ Date / Time Types

| Type | Format | Example |
|---|---|---|
| `TIMESTAMP` | `YYYY-MM-DD HH:MM:SS` | `'2025-01-01 10:30:00'` |
| `DATE` | `YYYY-MM-DD` | `'2025-01-01'` |

### 4️⃣ Decimal / Floating Types

| Type | Precision | Example |
|---|---|---|
| `FLOAT` | 32-bit | `3.14` |
| `DOUBLE` | 64-bit | `3.14159265` |
| `DECIMAL(p,s)` | Arbitrary | `DECIMAL(10,2)` → `12345.67` |

### 5️⃣ Complex Types

```sql
-- Array
courses   ARRAY<STRING>                   -- ['Math', 'CS']

-- Map
scores    MAP<STRING, INT>                -- {'Math': 90, 'CS': 85}

-- Struct
address   STRUCT<city:STRING, zip:INT>    -- {'Chennai', 600001}
```

**Full Example:**
```sql
CREATE TABLE student (
    id       INT,
    name     VARCHAR(100),
    gpa      DOUBLE,
    courses  ARRAY<STRING>,
    scores   MAP<STRING, INT>,
    address  STRUCT<city:STRING, zip:INT>
);
```

</details>

---

<details>
<summary><b>Q12 — Describe the process of creating and managing databases and tables in Hive.</b></summary>

<br>

### 🗄️ Managing Databases

```sql
-- Create
hive> CREATE DATABASE IF NOT EXISTS mydb;

-- List
hive> SHOW DATABASES;

-- Switch
hive> USE mydb;

-- Drop (CASCADE also drops all tables)
hive> DROP DATABASE IF EXISTS mydb CASCADE;
```

### 📋 Creating Tables

**Managed (Internal) Table**
```sql
hive> CREATE TABLE IF NOT EXISTS employee (
        eid    INT,
        name   STRING,
        salary FLOAT,
        dept   STRING
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      STORED AS TEXTFILE;
```

**External Table** *(data not deleted when table is dropped)*
```sql
hive> CREATE EXTERNAL TABLE ext_employee (
        eid  INT,
        name STRING
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      LOCATION '/external/data/emp/';
```

### ⚖️ Managed vs External Tables

| Feature | Managed Table | External Table |
|---|:---:|:---:|
| **Data Ownership** | Hive owns data | Data is external |
| **DROP behavior** | Drops data + metadata | Drops metadata only |
| **Use Case** | Hive-only pipelines | Shared with other tools |

### ✏️ Altering Tables

```sql
-- Rename
hive> ALTER TABLE employee RENAME TO emp;

-- Add column
hive> ALTER TABLE emp ADD COLUMNS (email STRING);

-- Change column
hive> ALTER TABLE emp CHANGE salary sal DOUBLE;

-- Drop
hive> DROP TABLE IF EXISTS emp;
```

### 📥 Loading Data

```sql
-- From local file
hive> LOAD DATA LOCAL INPATH '/home/user/emp.txt'
      OVERWRITE INTO TABLE employee;

-- From HDFS
hive> LOAD DATA INPATH '/hdfs/data/emp.txt'
      INTO TABLE employee;
```

</details>

---

<details>
<summary><b>Q13 — Explain how Hive Data Manipulation Language (DML) works with examples.</b></summary>

<br>

### 1️⃣ INSERT INTO / INSERT OVERWRITE

```sql
-- Append
INSERT INTO TABLE high_earners
SELECT eid, name, salary FROM employee WHERE salary > 50000;

-- Overwrite
INSERT OVERWRITE TABLE dept_avg
SELECT dept, AVG(salary) AS avg_sal FROM employee GROUP BY dept;
```

### 2️⃣ LOAD DATA (Bulk Insert)

```sql
LOAD DATA LOCAL INPATH '/home/user/emp.txt' OVERWRITE INTO TABLE employee;
```

### 3️⃣ SELECT (Query)

```sql
SELECT name, salary FROM employee WHERE salary > 30000;
SELECT dept, COUNT(*) FROM employee GROUP BY dept;
SELECT * FROM employee ORDER BY salary DESC;
```

### 4️⃣ UPDATE and DELETE *(ACID-enabled tables)*

```sql
UPDATE employee SET salary = salary * 1.1 WHERE dept = 'CSE';
DELETE FROM employee WHERE eid = 1001;
```

### 5️⃣ CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE cse_emp AS
SELECT * FROM employee WHERE dept = 'CSE';
```

### 6️⃣ TRUNCATE TABLE

```sql
TRUNCATE TABLE employee;   -- Removes all rows, keeps table structure
```

</details>

---

<details>
<summary><b>Q14 — Demonstrate how HiveQL is used to query and analyze data stored in Hadoop.</b></summary>

<br>

### 🏗️ Step 1 — Setup

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

### 🔍 Step 2 — Basic Queries

```sql
SELECT * FROM employee;
SELECT emp_name, emp_sal FROM employee WHERE emp_sal > 50000;
```

### 📊 Step 3 — Aggregations

```sql
SELECT dept, COUNT(*) AS num_emp FROM employee GROUP BY dept;
SELECT dept, AVG(emp_sal) AS avg_salary FROM employee GROUP BY dept;
SELECT MAX(emp_sal), MIN(emp_sal) FROM employee;
```

### 🔤 Step 4 — Sorting

```sql
SELECT emp_id, emp_name, emp_sal FROM employee ORDER BY emp_sal DESC;
```

### 🔗 Step 5 — Joins

```sql
-- Inner Join
SELECT e.emp_name, d.dept_name
FROM   employee e
JOIN   department d ON (e.dept = d.dept_id);

-- Left Outer Join
SELECT c.ID, c.NAME, o.AMOUNT
FROM   CUSTOMERS c
LEFT OUTER JOIN ORDERS o ON (c.ID = o.CUSTOMER_ID);
```

### 💾 Step 6 — Store Results

```sql
INSERT OVERWRITE TABLE high_salary_employees
SELECT emp_name, emp_sal FROM employee WHERE emp_sal > 75000;
```

### 🔧 Step 7 — Built-in Functions

```sql
SELECT emp_name, UPPER(emp_name) AS upper_name, LENGTH(emp_name) AS len
FROM   employee;
```

</details>

---

<details>
<summary><b>Q15 — Explain about Examining the Hive Clients with suitable examples.</b></summary>

<br>

### 🗂️ Hive Client Types

| Client | Protocol | Best For |
|---|---|---|
| **Hive CLI** | Local | Admin / quick queries |
| **Beeline** | JDBC (HiveServer2) | Production queries |
| **JDBC** | JDBC | Java applications |
| **ODBC** | ODBC | BI tools (Tableau, Excel) |
| **Thrift** | Thrift | Multi-language clients |
| **Web UI / Hue** | HTTP | Visual development |

### 💻 Client Examples

**1. Hive CLI**
```bash
hive
hive> SHOW DATABASES;
hive> SELECT * FROM employee LIMIT 5;
hive> quit;
```

**2. Beeline (Modern CLI)**
```bash
beeline -u jdbc:hive2://localhost:10000
0: jdbc:hive2://localhost:10000> SHOW TABLES;
```

**3. JDBC Client (Java)**
```java
Class.forName("org.apache.hive.jdbc.HiveDriver");

Connection con = DriverManager.getConnection(
    "jdbc:hive2://localhost:10000/default", "hive", "");

Statement stmt = con.createStatement();
ResultSet rs   = stmt.executeQuery("SELECT * FROM employee");

while (rs.next()) {
    System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
}
con.close();
```

**4. Start HiveServer2**
```bash
hive --service hiveserver2
```

**5. Web UI (Hue)**
```
URL:  http://hadoop-master:8888
Path: Query Editors → Hive Editor
```

### ⚖️ Client Comparison

| Feature | Hive CLI | Beeline | JDBC | Web UI |
|---|:---:|:---:|:---:|:---:|
| Interactive | ✅ | ✅ | ❌ | ✅ |
| Multi-user | ❌ | ✅ | ✅ | ✅ |
| Remote Access | ❌ | ✅ | ✅ | ✅ |
| BI Tool Support | ❌ | ❌ | ✅ | ✅ |
| Visualization | ❌ | ❌ | ❌ | ✅ |

</details>

---

<div align="center">

---

*All answers are based on course materials:*
`UNIT-III-CSM.pdf` &nbsp;·&nbsp; `R23 BDA UNIT-IV - CSM.pdf` &nbsp;·&nbsp; `R23 BDA UNIT V-CSD.pdf`

**Department of Allied Computer Science and Engineering**

</div>
