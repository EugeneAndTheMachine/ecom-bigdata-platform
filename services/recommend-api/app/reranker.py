"""Online re-ranking = batch candidates (ALS) x live context (session, trending, intent).

score(item) = w_als * als_norm
            + w_cat * category_match * (0.5 + intent)     # exploit what the user is browsing
            + w_trend * trending_norm * (1.5 - intent)     # explore what is hot when intent is low
Cold start (no ALS candidates) -> trending of the session category (or global top categories).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Weights:
    als: float = 0.6
    category: float = 0.3
    trending: float = 0.1


def _normalize(pairs: list[tuple[int, float]]) -> dict[int, float]:
    if not pairs:
        return {}
    hi = max(s for _, s in pairs)
    lo = min(s for _, s in pairs)
    span = (hi - lo) or 1.0
    return {pid: (s - lo) / span for pid, s in pairs}


def rerank(candidates: list[tuple[int, float]], trending: list[tuple[int, float]],
           session_category: str | None, product_category: dict[int, str],
           intent: float, k: int, weights: Weights = Weights(),
           exclude: set[int] | None = None) -> list[dict]:
    exclude = exclude or set()
    als = _normalize(candidates)
    trend = _normalize(trending)
    pool = list(dict.fromkeys([p for p, _ in candidates] + [p for p, _ in trending]))
    scored = []
    for pid in pool:
        if pid in exclude:
            continue
        cat_match = 1.0 if session_category and product_category.get(pid) == session_category else 0.0
        score = (weights.als * als.get(pid, 0.0)
                 + weights.category * cat_match * (0.5 + intent)
                 + weights.trending * trend.get(pid, 0.0) * (1.5 - intent))
        reason = "als" if pid in als else "trending"
        scored.append({"product_id": pid, "score": round(score, 6), "source": reason,
                       "category_l1": product_category.get(pid)})
    scored.sort(key=lambda d: d["score"], reverse=True)
    return scored[:k]
