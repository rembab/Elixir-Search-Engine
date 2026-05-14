import sys
import os
import json
import sqlite3
import argparse
import numpy as np
from scipy.sparse import csr_matrix, save_npz, load_npz
from scipy.sparse.linalg import svds


def load_matrix(db_path, matrix_name):
    cache_file = f"data/db/{matrix_name}_cache.npz"

    if os.path.exists(cache_file):
        A_T = load_npz(cache_file)
        max_term_id = A_T.shape[1] - 1
        return A_T, max_term_id

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    doc_ids, term_ids, vals = [], [], []
    c.execute(f"SELECT doc_id, term_id, val FROM {matrix_name}")

    while True:
        chunk = c.fetchmany(100000)
        if not chunk:
            break
        for row in chunk:
            doc_ids.append(row[0])
            term_ids.append(row[1])
            vals.append(row[2])

    conn.close()

    if not vals:
        raise ValueError(f"Matrix {matrix_name} is empty.")

    max_doc_id = max(doc_ids)
    max_term_id = max(term_ids)

    A_T = csr_matrix(
        (vals, (doc_ids, term_ids)), shape=(max_doc_id + 1, max_term_id + 1)
    )

    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    save_npz(cache_file, A_T)

    return A_T, max_term_id


def process_query(A_T, max_term_id, query_vector, top_k):
    q = np.zeros(max_term_id + 1)

    for item in query_vector:
        t_id, weight = item
        if t_id is not None and t_id <= max_term_id:
            q[t_id] = weight

    scores = A_T @ q
    top_indices = np.argsort(scores)[-top_k:][::-1]

    results = []
    for idx in top_indices:
        score = float(scores[idx])
        results.append({"doc_id": int(idx), "score": score})

    return results


def compute_and_save_svd(db_path, source_matrix, target_matrix, k):
    A_T, _ = load_matrix(db_path, source_matrix)

    k = min(k, min(A_T.shape) - 1)

    u, s, vt = svds(A_T.astype(float), k=k)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute(f"DROP TABLE IF EXISTS {target_matrix}")
    c.execute(f"""
    CREATE TABLE {target_matrix} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id INTEGER NOT NULL,
      term_id INTEGER NOT NULL,
      val FLOAT(24) NOT NULL,
      UNIQUE(doc_id, term_id)
    )
    """)

    s_vt = np.diag(s) @ vt

    for doc_id in range(A_T.shape[0]):
        row_dense = u[doc_id, :] @ s_vt

        non_zeros = np.where(np.abs(row_dense) > 1e-4)[0]

        if len(non_zeros) > 0:
            rows_to_insert = [
                (doc_id, int(t_id), float(row_dense[t_id])) for t_id in non_zeros
            ]
            c.executemany(
                f"INSERT INTO {target_matrix} (doc_id, term_id, val) VALUES (?, ?, ?)",
                rows_to_insert,
            )

        if doc_id > 0 and doc_id % 1000 == 0:
            conn.commit()

    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    matrices = {}
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
            action = request.get("action", "search")

            if action == "search":
                matrix_name = request["matrix_name"]
                query_vector = request["query"]
                top_k = request.get("top_k", 10)

                if matrix_name not in matrices:
                    A_T, max_term_id = load_matrix(args.db, matrix_name)
                    matrices[matrix_name] = (A_T, max_term_id)
                else:
                    A_T, max_term_id = matrices[matrix_name]

                results = process_query(A_T, max_term_id, query_vector, top_k)
                response = {"status": "ok", "results": results}

            elif action == "svd":
                source_matrix = request["matrix_name"]
                target_matrix = request["new_matrix_name"]
                k = request["k"]

                compute_and_save_svd(args.db, source_matrix, target_matrix, k)
                response = {"status": "ok", "message": f"SVD saved to {target_matrix}"}

        except Exception as e:
            response = {"status": "error", "message": str(e)}

        print(json.dumps(response))
        sys.stdout.flush()
