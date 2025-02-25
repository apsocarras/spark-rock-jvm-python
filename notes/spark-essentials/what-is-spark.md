### Spark is a Unified Computing Engine for Distributed Data Processing 

Data loading, queries, machine learning, streaming 
Consistent, composable APIs across langauges 
Optimizations across different libraries 
Compute engine means it is detached from data storage and I/O
In-memory, parallelized data processing engine

Stdlib: Spark SQL, MLLib, Streaming, Graphx
Hundreds of open source spark libraries exist 

### Motivation for spark 

storage cheap, compute expensive, blah blah we've heard it.

Supplanted MapReduce 
* Inefficient for large multi-step data processing applications (e.g. ML)
* Every step required another pass over the data, written and deployed as a separate application on the cluster

Spark optimized these multi-step applications
In-memory computation and data sharing across nodes 

### Spark can read data from wherever. 

It only cares about compute 

### Spark Architecture 

applications: Streaming ML GraphX Other libraries
high-level (structured) APIs
low-level APIs: RDDs