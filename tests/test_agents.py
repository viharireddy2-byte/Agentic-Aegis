import polars as pl
import pytest

from src.agents import HealerAgent, OracleAgent, ScoutAgent, SentinelAgent


@pytest.fixture
def dirty_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 4],
        "name": ["  Alice", "bob", "BOB", "Carol", "Carol"],
        "amount": [10.0, -5.0, 1000.0, None, None],
        "email": ["a@x.com", "bad_email", "c@x.com", "d@x.com", "d@x.com"],
    })


def test_scout_detects_known_issue_categories(dirty_df):
    profile = ScoutAgent().profile_dataset(dirty_df, "test_dataset")
    categories = set(profile["issue_categories"])
    assert "duplicates" in categories
    assert "nulls" in categories
    assert "whitespace" in categories
    assert "negative_values" in categories
    assert profile["issue_count"] > 0


def test_sentinel_scores_between_0_and_100(dirty_df):
    profile = ScoutAgent().profile_dataset(dirty_df, "test_dataset")
    score = SentinelAgent().calculate_quality_score(profile)
    assert 0 <= score["overall_score"] <= 100
    assert score["status"] in {"excellent", "good", "needs_attention", "critical"}


def test_healer_removes_duplicates_and_negatives(dirty_df):
    profile = ScoutAgent().profile_dataset(dirty_df, "test_dataset")
    clean_df, actions = HealerAgent().auto_remediate(dirty_df, profile["issues_detected"])
    assert clean_df.height < dirty_df.height  # duplicates removed
    assert (clean_df["amount"].drop_nulls() >= 0).all()
    assert len(actions) > 0


def test_oracle_flags_anomalies_on_larger_dataset():
    import random
    random.seed(1)
    values = [random.gauss(50, 5) for _ in range(200)] + [500, 520, 480]
    df = pl.DataFrame({"amount": values, "qty": [random.randint(1, 5) for _ in values]})
    result = OracleAgent(contamination=0.02).detect_anomalies(df)
    assert result["anomaly_count"] >= 1


def test_oracle_detects_drift():
    base_profile = {"column_stats": {"amount": {"mean": 50.0, "null_pct": 1.0}}}
    current_profile = {"column_stats": {"amount": {"mean": 90.0, "null_pct": 1.0}}}
    result = OracleAgent().detect_drift(base_profile, current_profile, threshold_pct=15.0)
    assert result["drift_detected"] is True
    assert result["drifted_metrics"][0]["column"] == "amount"
