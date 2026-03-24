# Big Data Assignment 2 — Simple Search Engine using Hadoop MapReduce

This project implements a simple search engine for plain text documents using:

- Hadoop MapReduce for indexing
- Cassandra for index storage
- Spark on YARN for query processing and BM25 ranking

The system prepares documents from a Wikipedia parquet file, builds an inverted index, stores the index in Cassandra, and retrieves top relevant documents for user queries.

---

## Project Structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── docker/
│   ├── entrypoint.sh
│   └── hadoop/
│       ├── core-site.xml
│       ├── hdfs-site.xml
│       ├── mapred-site.xml
│       ├── yarn-site.xml
│       ├── workers
│       └── hadoop-env.sh
└── app/
    ├── app.sh
    ├── prepare_data.py
    ├── prepare_data.sh
    ├── create_index.sh
    ├── store_index.sh
    ├── index.sh
    ├── search.sh
    ├── query.py
    ├── app.py
    ├── requirements.txt
    ├── data_source/
    │   └── a.parquet
    └── mapreduce/
        ├── mapper1.py
        ├── reducer1.py
        ├── mapper2.py
        └── reducer2.py