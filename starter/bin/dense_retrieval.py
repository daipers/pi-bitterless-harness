#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any

import numpy
from harnesslib import sha256_file, sha256_text

TOKEN_RE = __import__("re").compile(r"[a-z0-9]+")
DEFAULT_HASH_DIM = 8192
DEFAULT_EMBEDDING_DIM = 256
DEFAULT_PROJECTION_SEED = 7
DEFAULT_SCORE_MODE = "cosine"


def _stable_hash(token: str, *, hash_dim: int) -> int:
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % max(1, hash_dim)


def hashed_terms(text: str) -> list[str]:
    tokens = TOKEN_RE.findall(text.lower())
    terms = list(tokens)
    for left, right in zip(tokens, tokens[1:]):
        terms.append(f"{left}__{right}")
    return terms


def hashed_feature_vector(text: str, *, hash_dim: int = DEFAULT_HASH_DIM) -> numpy.ndarray:
    vector = numpy.zeros(max(1, hash_dim), dtype=numpy.float32)
    for term in hashed_terms(text):
        vector[_stable_hash(term, hash_dim=hash_dim)] += 1.0
    return vector


def projection_matrix(
    *,
    hash_dim: int = DEFAULT_HASH_DIM,
    embedding_dim: int = DEFAULT_EMBEDDING_DIM,
    seed: int = DEFAULT_PROJECTION_SEED,
) -> numpy.ndarray:
    rng = numpy.random.default_rng(seed)
    return rng.standard_normal((hash_dim, embedding_dim), dtype=numpy.float32) / numpy.sqrt(
        max(1, embedding_dim)
    )


def normalize_vector(vector: numpy.ndarray) -> numpy.ndarray:
    norm = float(numpy.linalg.norm(vector))
    if norm <= 0:
        return numpy.zeros_like(vector, dtype=numpy.float32)
    return (vector / norm).astype(numpy.float32)


def encode_feature_vector(
    feature_vector: numpy.ndarray,
    *,
    feature_weights: numpy.ndarray,
    projection: numpy.ndarray,
) -> numpy.ndarray:
    weighted = feature_vector.astype(numpy.float32) * feature_weights.astype(numpy.float32)
    return normalize_vector(weighted @ projection)


def encode_text(
    text: str,
    *,
    hash_dim: int,
    feature_weights: numpy.ndarray,
    projection: numpy.ndarray,
) -> numpy.ndarray:
    return encode_feature_vector(
        hashed_feature_vector(text, hash_dim=hash_dim),
        feature_weights=feature_weights,
        projection=projection,
    )


def cosine_similarity(left: numpy.ndarray, right: numpy.ndarray) -> float:
    if left.size == 0 or right.size == 0:
        return 0.0
    return float(numpy.dot(left, right))


def query_text_from_example(example: dict[str, Any]) -> str:
    query = example.get("query", {}) if isinstance(example.get("query"), dict) else {}
    return "\n".join(
        [
            str(query.get("task_title", "")),
            str(query.get("goal", "")),
            str(query.get("constraints", "")),
            str(query.get("done", "")),
        ]
    ).strip()


def _document_text(row: dict[str, Any]) -> str:
    return str(row.get("text", ""))


def encode_query(
    text: str,
    *,
    runtime: dict[str, Any],
) -> numpy.ndarray:
    return encode_text(
        text,
        hash_dim=int(runtime["hash_dim"]),
        feature_weights=runtime["feature_weights"],
        projection=runtime["projection"],
    )


def encode_document(
    text: str,
    *,
    runtime: dict[str, Any],
) -> numpy.ndarray:
    return encode_query(text, runtime=runtime)


def score_pair(
    query_text: str,
    document_text: str,
    *,
    runtime: dict[str, Any],
) -> float:
    query_embedding = encode_query(query_text, runtime=runtime)
    document_embedding = encode_document(document_text, runtime=runtime)
    score_mode = str(runtime.get("score_mode", DEFAULT_SCORE_MODE))
    if score_mode != "cosine":
        raise ValueError(f"unsupported score mode: {score_mode}")
    return cosine_similarity(query_embedding, document_embedding)


def _feature_vectors_for_documents(
    documents_by_run_id: dict[str, dict[str, Any]],
    *,
    hash_dim: int,
) -> dict[str, numpy.ndarray]:
    return {
        run_id: hashed_feature_vector(_document_text(row), hash_dim=hash_dim)
        for run_id, row in documents_by_run_id.items()
    }


def train_dense_feature_weights(
    examples: list[dict[str, Any]],
    documents_by_run_id: dict[str, dict[str, Any]],
    *,
    hash_dim: int = DEFAULT_HASH_DIM,
    embedding_dim: int = DEFAULT_EMBEDDING_DIM,
    projection_seed: int = DEFAULT_PROJECTION_SEED,
    epochs: int,
    learning_rate: float,
    margin: float,
) -> tuple[numpy.ndarray, dict[str, int]]:
    projection = projection_matrix(
        hash_dim=hash_dim,
        embedding_dim=embedding_dim,
        seed=projection_seed,
    )
    weights = numpy.ones(hash_dim, dtype=numpy.float32)
    doc_vectors = _feature_vectors_for_documents(documents_by_run_id, hash_dim=hash_dim)
    query_cache: dict[str, numpy.ndarray] = {}
    stats = {
        "pair_count": 0,
        "update_count": 0,
        "positive_examples": 0,
        "abstain_examples": 0,
        "dense_document_count": len(doc_vectors),
    }

    def query_vector(example: dict[str, Any]) -> numpy.ndarray:
        example_id = str(example.get("example_id", ""))
        if example_id not in query_cache:
            query_cache[example_id] = hashed_feature_vector(
                query_text_from_example(example),
                hash_dim=hash_dim,
            )
        return query_cache[example_id]

    for example in examples:
        if example.get("abstention_label") is True:
            stats["abstain_examples"] += 1
        if any(str(run_id) in doc_vectors for run_id in example.get("gold_source_run_ids", [])):
            stats["positive_examples"] += 1

    fallback_update: tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray] | None = None
    for _ in range(max(1, epochs)):
        for example in examples:
            q_features = query_vector(example)
            positive_ids = [
                str(run_id)
                for run_id in example.get("gold_source_run_ids", [])
                if str(run_id) in doc_vectors
            ]
            if not positive_ids:
                continue
            ordered_negative_ids = [
                str(run_id)
                for run_id in example.get("hard_negative_run_ids", [])
                if str(run_id) in doc_vectors and str(run_id) not in positive_ids
            ]
            for candidate in example.get("candidate_set", []):
                run_id = str(candidate.get("run_id", ""))
                if run_id and run_id not in positive_ids and run_id in doc_vectors:
                    ordered_negative_ids.append(run_id)
            negative_ids = list(dict.fromkeys(ordered_negative_ids))
            if not negative_ids:
                continue
            if fallback_update is None:
                fallback_update = (
                    q_features,
                    doc_vectors[positive_ids[0]],
                    doc_vectors[negative_ids[0]],
                )

            q_embedding = encode_feature_vector(
                q_features,
                feature_weights=weights,
                projection=projection,
            )
            for positive_id in positive_ids:
                pos_features = doc_vectors[positive_id]
                pos_embedding = encode_feature_vector(
                    pos_features,
                    feature_weights=weights,
                    projection=projection,
                )
                pos_score = cosine_similarity(q_embedding, pos_embedding)
                for negative_id in negative_ids:
                    neg_features = doc_vectors[negative_id]
                    neg_embedding = encode_feature_vector(
                        neg_features,
                        feature_weights=weights,
                        projection=projection,
                    )
                    neg_score = cosine_similarity(q_embedding, neg_embedding)
                    stats["pair_count"] += 1
                    if pos_score > neg_score + margin:
                        continue
                    weights += (
                        float(learning_rate) * q_features * (pos_features - neg_features)
                    ).astype(numpy.float32)
                    numpy.clip(weights, 0.05, 8.0, out=weights)
                    q_embedding = encode_feature_vector(
                        q_features,
                        feature_weights=weights,
                        projection=projection,
                    )
                    pos_embedding = encode_feature_vector(
                        pos_features,
                        feature_weights=weights,
                        projection=projection,
                    )
                    pos_score = cosine_similarity(q_embedding, pos_embedding)
                    stats["update_count"] += 1
    if stats["update_count"] == 0 and fallback_update is not None:
        q_features, pos_features, neg_features = fallback_update
        weights += (float(learning_rate) * q_features * (pos_features - neg_features)).astype(
            numpy.float32
        )
        numpy.clip(weights, 0.05, 8.0, out=weights)
        stats["update_count"] = 1
    return weights, stats


def write_dense_retriever_artifacts(
    artifacts_dir: pathlib.Path,
    *,
    feature_weights: numpy.ndarray,
    hash_dim: int,
    embedding_dim: int,
    projection_seed: int,
    document_fingerprint: str,
) -> dict[str, Any]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    feature_weights_path = artifacts_dir / "feature_weights.npy"
    numpy.save(feature_weights_path, feature_weights.astype(numpy.float32))
    config_payload = {
        "retriever_type": "dense-v1",
        "encoder_type": "local-hashed-v2",
        "hash_dim": hash_dim,
        "embedding_dim": embedding_dim,
        "projection_seed": projection_seed,
        "document_fingerprint": document_fingerprint,
        "score_mode": DEFAULT_SCORE_MODE,
    }
    config_path = artifacts_dir / "encoder-config.json"
    config_path.write_text(
        json.dumps(config_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_fingerprint = sha256_text(
        json.dumps(
            {
                "config": config_payload,
                "feature_weights_sha256": sha256_file(feature_weights_path) or "",
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return {
        "retriever_type": "dense-v1",
        "encoder_type": "local-hashed-v2",
        "hash_dim": hash_dim,
        "embedding_dim": embedding_dim,
        "projection_seed": projection_seed,
        "document_fingerprint": document_fingerprint,
        "artifact_fingerprint": artifact_fingerprint,
        "feature_weights_path": str(feature_weights_path.resolve()),
        "config_path": str(config_path.resolve()),
        "score_mode": DEFAULT_SCORE_MODE,
    }


def load_dense_retriever_runtime(retriever_payload: dict[str, Any]) -> dict[str, Any]:
    feature_weights_path = pathlib.Path(str(retriever_payload.get("feature_weights_path", "")))
    if not feature_weights_path.is_file():
        raise ValueError(f"dense feature weights missing: {feature_weights_path}")
    hash_dim = int(retriever_payload.get("hash_dim", DEFAULT_HASH_DIM))
    embedding_dim = int(retriever_payload.get("embedding_dim", DEFAULT_EMBEDDING_DIM))
    projection_seed = int(retriever_payload.get("projection_seed", DEFAULT_PROJECTION_SEED))
    feature_weights = numpy.load(feature_weights_path).astype(numpy.float32)
    if feature_weights.shape != (hash_dim,):
        raise ValueError(
            "dense feature weights shape mismatch: "
            f"expected {(hash_dim,)}, got {feature_weights.shape}"
        )
    projection = projection_matrix(
        hash_dim=hash_dim,
        embedding_dim=embedding_dim,
        seed=projection_seed,
    )
    return {
        "retriever_type": str(retriever_payload.get("retriever_type", "dense-v1")),
        "encoder_type": str(retriever_payload.get("encoder_type", "local-hashed-v2")),
        "hash_dim": hash_dim,
        "embedding_dim": embedding_dim,
        "projection_seed": projection_seed,
        "artifact_fingerprint": str(retriever_payload.get("artifact_fingerprint", "")),
        "feature_weights_path": str(feature_weights_path.resolve()),
        "feature_weights": feature_weights,
        "projection": projection,
        "score_mode": str(retriever_payload.get("score_mode", DEFAULT_SCORE_MODE)),
    }


def load_text_encoder_runtime(artifact_payload: dict[str, Any]) -> dict[str, Any]:
    return load_dense_retriever_runtime(artifact_payload)
