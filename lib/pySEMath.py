import sys
import os
import json
import sqlite3
import argparse
import numpy as np
from scipy.sparse import csr_matrix, save_npz, load_npz
from sklearn.decomposition import TruncatedSVD


def load_matrix(db_path, matrix_name):
    cache_file = f"data/db/{matrix_name}_cache.npz"

    if os.path.exists(cache_file):
        A_T = load_npz(cache_file)
        max_term_id = A_T.shape[1] - 1
        return A_T, max_term_id

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute(f"SELECT COUNT(*) FROM {matrix_name}")
    total_rows = c.fetchone()[0]

    if total_rows == 0:
        raise ValueError(f"Matrix {matrix_name} is empty.")

    doc_ids = np.empty(total_rows, dtype=np.int32)
    term_ids = np.empty(total_rows, dtype=np.int32)
    vals = np.empty(total_rows, dtype=np.float32)

    c.execute(f"SELECT doc_id, term_id, val FROM {matrix_name}")

    idx = 0
    while True:
        chunk = c.fetchmany(100000)
        if not chunk:
            break

        chunk_len = len(chunk)
        arr = np.array(chunk)
        doc_ids[idx : idx + chunk_len] = arr[:, 0]
        term_ids[idx : idx + chunk_len] = arr[:, 1]
        vals[idx : idx + chunk_len] = arr[:, 2]
        idx += chunk_len

    conn.close()

    max_doc_id = int(np.max(doc_ids))
    max_term_id = int(np.max(term_ids))

    A_T = csr_matrix(
        (vals, (doc_ids, term_ids)), shape=(max_doc_id + 1, max_term_id + 1)
    )

    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    save_npz(cache_file, A_T)

    return A_T, max_term_id


def process_query(matrix_data, query_vector, top_k):
    max_term_id = matrix_data["max_term_id"]
    q = np.zeros(max_term_id + 1)

    for item in query_vector:
        t_id, weight = item
        if t_id is not None and t_id <= max_term_id:
            q[t_id] = weight

    if matrix_data["type"] == "sparse":
        scores = matrix_data["A_T"] @ q
    else:
        scores = matrix_data["u"] @ (matrix_data["s"] * (matrix_data["vt"] @ q))

    top_indices = np.argsort(scores)[-top_k:][::-1]

    results = []
    for idx in top_indices:
        score = float(scores[idx])
        results.append({"doc_id": int(idx), "score": score})

    return results


def compute_and_save_svd(db_path, source_matrix, target_matrix, k):
    A_T, _ = load_matrix(db_path, source_matrix)

    k = min(k, min(A_T.shape) - 1)

    svd = TruncatedSVD(n_components=k, algorithm="randomized", random_state=42)

    u = svd.fit_transform(A_T)
    s = svd.singular_values_
    vt = svd.components_

    u = u / s

    os.makedirs("data/db", exist_ok=True)
    np.savez(f"data/db/{target_matrix}_svd.npz", u=u, s=s, vt=vt)


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
                    svd_cache = f"data/db/{matrix_name}_svd.npz"
                    if os.path.exists(svd_cache):
                        data = np.load(svd_cache)
                        matrices[matrix_name] = {
                            "type": "svd",
                            "u": data["u"],
                            "s": data["s"],
                            "vt": data["vt"],
                            "max_term_id": data["vt"].shape[1] - 1,
                        }
                    else:
                        A_T, max_term_id = load_matrix(args.db, matrix_name)
                        matrices[matrix_name] = {
                            "type": "sparse",
                            "A_T": A_T,
                            "max_term_id": max_term_id,
                        }

                results = process_query(matrices[matrix_name], query_vector, top_k)
                response = {"status": "ok", "results": results}

            elif action == "svd":
                source_matrix = request["matrix_name"]
                target_matrix = request["new_matrix_name"]
                k = request["k"]

                compute_and_save_svd(args.db, source_matrix, target_matrix, k)
                response = {
                    "status": "ok",
                    "message": f"SVD saved to {target_matrix}_svd.npz",
                }

        except Exception as e:
            response = {"status": "error", "message": str(e)}

        print(json.dumps(response))
        sys.stdout.flush()
