# DuckDB - Spark Extension
## Introduction
DuckDB extension to connect to Spark via Spark Connect API - so DuckDB can tap into the ecosystem of Spark.

The vision of this project is creating hybrid data processing systems, combine the strength of both distributed computing (scalable) and local computing (blazing fast).

## Overall Design

### Overview

The `duckdb-spark`
extension stand between DuckDB and Spark Connect Driver, allow DuckDB to utilized resources of scalable distributed Spark clusters and connect to broad range of data systems (Cloudera, Databricks, etc) and lakehouse (Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon, etc) to explore, analyze data without caring much about underlying details of storage or table format.

The reasons why this project works are:

1. Spark Connect API, built on 2 amazing language-agnostic technologies: gRPC and Apache Arrow in-memory columnar data format
2. DuckDB integration with Apache Arrow: zero-copy data access and conversion from DuckDB data format to Arrow RecordBatch


```mermaid
flowchart TD

spark_cluster(Spark Cluster)
spark_connect(Spark Connect Driver)
duckdb_ext(duckdb-spark extension)
duckdb(DuckDB)

duckdb --> duckdb_ext -- gRPC + Arrow --> spark_connect --> spark_cluster
```

### The read path

```mermaid
flowchart TD

storage(Data Storage)
catalog(Data Catalog)
spark_connect(Spark Connect Driver & Cluster)
duckdb_ext(DuckDB Extension with grpc and arrow)
duckdb(DuckDB)

catalog -- get tables metadata --> spark_connect
storage -- read tables as DataFrame --> spark_connect -- send table data as Arrow batches via gRPC --> duckdb_ext -- read data as arrow table --> duckdb
```

### The write path

```mermaid
flowchart TD

storage(Data Storage)
catalog(Data Catalog)
spark_connect(Spark Connect Driver)
duckdb_ext(DuckDB Extension with grpc and arrow)
duckdb(DuckDB)

duckdb -- write data as arrow table --> duckdb_ext -- write data as arrow batch via gRPC --> spark_connect -- write data from DataFrame to tables --> storage
spark_connect -- update table metadata --> catalog
```

## Documentation
- [Extension Docs](./docs/README.md)
