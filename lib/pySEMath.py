import sys
import os
import json
import sqlite3
import argparse
import numpy as np
from scipy.sparse import csr_matrix, save_npz, load_npz


def load_matrix(db_path, matrix_name):
    """Loads the matrix, using a fast .npz cache if available."""
    cache_file = f"{matrix_name}_cache.npz"

    if os.path.exists(cache_file):
        print(f"Loading cached matrix from {cache_file}...", file=sys.stderr)
        A_T = load_npz(cache_file)
        max_term_id = A_T.shape[1] - 1
        return A_T, max_term_id

    print(
        f"Building matrix '{matrix_name}' from SQLite (this may take a while)...",
        file=sys.stderr,
    )
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

    print(f"Saving matrix to {cache_file} for fast future loads...", file=sys.stderr)
    save_npz(cache_file, A_T)

    return A_T, max_term_id


def process_query(A_T, max_term_id, query_vector, top_k):
    """Calculates cosine similarity for a given query vector."""
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

        except Exception as e:
            response = {"status": "error", "message": str(e)}

        print(json.dumps(response))
        sys.stdout.flush()
