import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def sample_payload() -> dict:
    return json.loads((FIXTURES / "sample_recentchange.json").read_text())


@pytest.fixture(scope="session")
def sample_event_time(sample_payload) -> float:
    return float(sample_payload["timestamp"])
