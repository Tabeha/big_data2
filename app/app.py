#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

KEYSPACE = "search_engine"


def connect():
    cluster = Cluster(["cassandra"], load_balancing_policy=RoundRobinPolicy())
    session = cluster.connect()
    return cluster, session


def create_schema(session):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            doc_len int
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS terms (
            term text PRIMARY KEY,
            df int
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_key text PRIMARY KEY,
            stat_value double
        )
        """
    )


def truncate_tables(session):
    for table in ["documents", "terms", "postings", "corpus_stats"]:
        session.execute(f"TRUNCATE {table}")


def load_stage1(session, path):
    ins_doc = session.prepare(
        "INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )
    ins_post = session.prepare(
        "INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)"
    )

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            parts = raw.split("\t")
            tag = parts[0]

            if tag == "DOC" and len(parts) == 4:
                _, doc_id, title, doc_len = parts
                session.execute(ins_doc, (doc_id, title, int(doc_len)))
            elif tag == "POSTING" and len(parts) == 4:
                _, term, doc_id, tf = parts
                session.execute(ins_post, (term, doc_id, int(tf)))


def load_stage2(session, path):
    ins_term = session.prepare(
        "INSERT INTO terms (term, df) VALUES (?, ?)"
    )
    ins_stat = session.prepare(
        "INSERT INTO corpus_stats (stat_key, stat_value) VALUES (?, ?)"
    )

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            parts = raw.split("\t")
            tag = parts[0]

            if tag == "TERM" and len(parts) == 3:
                _, term, df = parts
                session.execute(ins_term, (term, int(float(df))))
            elif tag == "STAT" and len(parts) == 3:
                _, stat_key, stat_value = parts
                session.execute(ins_stat, (stat_key, float(stat_value)))


def main():
    if len(sys.argv) != 3:
        print("usage: python3 app.py <stage1_file> <stage2_file>", file=sys.stderr)
        sys.exit(1)

    stage1_file = sys.argv[1]
    stage2_file = sys.argv[2]

    cluster, session = connect()
    try:
        create_schema(session)
        truncate_tables(session)
        load_stage1(session, stage1_file)
        load_stage2(session, stage2_file)
        print("Index stored in Cassandra successfully")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()