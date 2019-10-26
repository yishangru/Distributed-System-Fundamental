# Spark RDD - fundamental data structure
**RDD** stands for resilient distributed dataset, which is a collection of elements partitioned across nodes of cluster that can be operated on in parallel. RDD is fundamental data structure in Spark, enabling fault tolerant, execution with mutiple nodes, automatic data rollback, and in-memory computation.

- In Memory Computation

Increase processing speed

- Lazy Evaluation 

Not generate output without dump or store operation

- Fault Tolerance

Any lost partition of RDD can be rolled back by applying simple transformations onto the corresponding part in lineage

# Operations on RDD
Apply **Transformation** to an RDD to **Access**, **Modify** and **Filter** current RDD to generate a new one. There are two types of **Transformation**: **Narrow Transformation** and **Wide Transformation**

- **Narrow Transformation**

**Narrow Transformation** applies on one RDD, like *map*, *filter*, *flatMap* and *partition*.

- **Wide Transformation**

**Wide Transformation** applies on multiple partitions of an RDD, like *reduceBy*, *union*.