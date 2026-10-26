from datetime import UTC, datetime

from app.model import INTENT_FEATURES, session_features
from app.reranker import rerank


def test_cold_start_returns_trending():
    out = rerank([], [(1, 9.0), (2, 5.0)], None, {}, intent=0.5, k=5)
    assert [i["product_id"] for i in out] == [1, 2]
    assert all(i["source"] == "trending" for i in out)


def test_category_boost_beats_slightly_better_als_score():
    candidates = [(10, 1.00), (20, 0.95), (30, 0.10)]
    cats = {10: "apparel", 20: "electronics", 30: "electronics"}
    out = rerank(candidates, [], "electronics", cats, intent=0.9, k=3)
    assert out[0]["product_id"] == 20


def test_excluded_items_are_removed_and_k_respected():
    out = rerank([(1, 1.0), (2, 0.5), (3, 0.1)], [], None, {}, 0.5, k=2, exclude={1})
    assert [i["product_id"] for i in out] == [2, 3]


def test_session_features_shape_matches_training_contract():
    session = {"n_views": "4", "n_carts": "2", "cart_items": "1", "cart_value": "120.5",
               "recent_categories": "electronics|apparel", "duration_s": "300",
               "first_event_time": "2026-10-26 10:15:00"}
    feats = session_features(session, now=datetime(2026, 10, 26, tzinfo=UTC))
    assert list(feats) == INTENT_FEATURES
    assert feats["n_categories"] == 2.0 and feats["hour_of_day"] == 10.0
