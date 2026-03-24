#!/usr/bin/env python3
import math
import re
import sys
import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75
KEYSPACE = "search_engine"


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


def bm25(tf, df, dl, n_docs, avgdl, k1=K1, b=B):
    if tf <= 0 or df <= 0 or n_docs <= 0 or avgdl <= 0:
        return 0.0

    idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)
    denom = tf + k1 * (1.0 - b + b * (dl / avgdl))
    return idf * ((tf * (k1 + 1.0)) / denom)


def get_query():
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return sys.stdin.read().strip()


def fetch_stats(session):
    rows = session.execute("SELECT stat_key, stat_value FROM corpus_stats")
    stats = {row.stat_key: row.stat_value for row in rows}
    return int(stats.get("N", 0)), float(stats.get("avgdl", 0.0))


def fetch_df(session, terms):
    if not terms:
        return {}

    prepared = session.prepare("SELECT term, df FROM terms WHERE term = ?")
    out = {}
    for term in terms:
        row = session.execute(prepared, (term,)).one()
        if row is not None:
            out[term] = int(row.df)
    return out


def fetch_postings(session, terms):
    if not terms:
        return []

    prepared = session.prepare("SELECT term, doc_id, tf FROM postings WHERE term = ?")
    rows = []
    for term in terms:
        for row in session.execute(prepared, (term,)):
            rows.append((row.term, row.doc_id, int(row.tf)))
    return rows


def fetch_documents(session, doc_ids):
    if not doc_ids:
        return {}

    prepared = session.prepare("SELECT doc_id, title, doc_len FROM documents WHERE doc_id = ?")
    docs = {}
    for doc_id in doc_ids:
        row = session.execute(prepared, (doc_id,)).one()
        if row is not None:
            docs[row.doc_id] = (row.title, int(row.doc_len))
    return docs


def connect_with_retry(cluster, keyspace, attempts=12, delay=5):
    last_error = None
    session = None

    for _ in range(attempts):
        try:
            session = cluster.connect()
            session.set_keyspace(keyspace)
            return session
        except Exception as e:
            last_error = e
            if session is not None:
                try:
                    session.shutdown()
                except Exception:
                    pass
                session = None
            time.sleep(delay)

    raise last_error


def main():
    query = get_query()
    if not query:
        print("Empty query", file=sys.stderr)
        sys.exit(1)

    q_terms = tokenize(query)
    if not q_terms:
        print("No valid query terms", file=sys.stderr)
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("bm25-query")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    cluster = Cluster(["cassandra"])
    session = None

    try:
        session = connect_with_retry(cluster, KEYSPACE)

        n_docs, avgdl = fetch_stats(session)
        df_map = fetch_df(session, q_terms)
        valid_terms = [t for t in q_terms if t in df_map]

        if not valid_terms:
            print("No matching terms in index")
            return

        postings = fetch_postings(session, valid_terms)
        if not postings:
            print("No matching documents")
            return

        doc_ids = sorted({doc_id for _, doc_id, _ in postings})
        docs_map = fetch_documents(session, doc_ids)

        bc_df = sc.broadcast(df_map)
        bc_docs = sc.broadcast(docs_map)
        bc_n = sc.broadcast(n_docs)
        bc_avgdl = sc.broadcast(avgdl)

        postings_rdd = sc.parallelize(postings)

        scores = (
            postings_rdd
            .map(
                lambda x: (
                    x[1],
                    bm25(
                        tf=x[2],
                        df=bc_df.value.get(x[0], 0),
                        dl=bc_docs.value.get(x[1], ("", 0))[1],
                        n_docs=bc_n.value,
                        avgdl=bc_avgdl.value,
                    ),
                )
            )
            .reduceByKey(lambda a, b: a + b)
        )

        top10 = scores.takeOrdered(10, key=lambda x: -x[1])

        for rank, (doc_id, score) in enumerate(top10, start=1):
            title = docs_map.get(doc_id, ("", 0))[0]
            print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")

    finally:
        if session is not None:
            session.shutdown()
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()