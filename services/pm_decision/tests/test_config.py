from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from pm_decision.config import Settings


def test_target_notionals_must_be_explicit_in_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("PM_DECISION_BEARER_TOKEN", "x" * 32)
    monkeypatch.setenv("PM_DECISION_DATABASE_PATH", str(tmp_path / "state.sqlite3"))
    monkeypatch.delenv("PM_DECISION_BEARER_TOKEN_FILE", raising=False)
    monkeypatch.delenv("PM_DECISION_V1_TARGET_NOTIONAL", raising=False)
    monkeypatch.delenv("PM_DECISION_V2_TARGET_NOTIONAL", raising=False)
    with pytest.raises(ValueError, match="PM_DECISION_V1_TARGET_NOTIONAL"):
        Settings.from_env()

    monkeypatch.setenv("PM_DECISION_V1_TARGET_NOTIONAL", "5")
    monkeypatch.setenv("PM_DECISION_V2_TARGET_NOTIONAL", "10")
    settings = Settings.from_env()
    assert settings.v1_target_notional == Decimal("5")
    assert settings.v2_target_notional == Decimal("10")


@pytest.mark.parametrize(
    "name",
    ["PM_DECISION_V1_TARGET_NOTIONAL", "PM_DECISION_V2_TARGET_NOTIONAL"],
)
@pytest.mark.parametrize(
    "value",
    ["", "0", "-1", "1e1", "NaN", "Infinity", "+1", ".5", "01", "5.", "1_0"],
)
def test_invalid_target_notional_is_rejected(monkeypatch, tmp_path, name, value):
    monkeypatch.setenv("PM_DECISION_BEARER_TOKEN", "x" * 32)
    monkeypatch.setenv("PM_DECISION_DATABASE_PATH", str(tmp_path / "state.sqlite3"))
    monkeypatch.delenv("PM_DECISION_BEARER_TOKEN_FILE", raising=False)
    monkeypatch.setenv("PM_DECISION_V1_TARGET_NOTIONAL", "5")
    monkeypatch.setenv("PM_DECISION_V2_TARGET_NOTIONAL", "10")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        Settings.from_env()


def test_direct_settings_enforce_token_and_absolute_database_path(tmp_path):
    with pytest.raises(ValueError, match="at least 32"):
        Settings(
            bearer_token="short",
            database_path=tmp_path / "state.sqlite3",
            v1_target_notional=Decimal("5"),
            v2_target_notional=Decimal("10"),
        )
    with pytest.raises(ValueError, match="absolute"):
        Settings(
            bearer_token="x" * 32,
            database_path=Path("relative.sqlite3"),
            v1_target_notional=Decimal("5"),
            v2_target_notional=Decimal("10"),
        )
