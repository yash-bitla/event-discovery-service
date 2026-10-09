from __future__ import annotations

import pytest

from event_discovery.clock import ManualClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer


def test_try_spend_stops_at_the_reserve(clock: ManualClock) -> None:
    budget = QuotaBudget(clock, daily_limit=10, reserve=7)
    assert [budget.try_spend() for _ in range(4)] == [True, True, True, False]
    assert budget.spendable == 0


def test_observe_replaces_the_local_count(clock: ManualClock) -> None:
    budget = QuotaBudget(clock, daily_limit=100, reserve=10)
    budget.observe(available=40, reset_at=clock.now() + 3600)
    assert budget.spendable == 30


def test_the_budget_is_full_again_after_the_reset(clock: ManualClock) -> None:
    budget = QuotaBudget(clock, daily_limit=100, reserve=10)
    budget.observe(available=0, reset_at=clock.now() + 3600)
    assert not budget.try_spend()
    clock.sleep(3600)
    assert budget.spendable == 90


def test_allowance_spreads_the_calls_across_the_time_to_the_reset(clock: ManualClock) -> None:
    budget = QuotaBudget(clock, daily_limit=5000, reserve=500)
    assert budget.allowance(3600) == 187  # 4500 * 3600 / 86400 = 187.5

    budget.observe(available=1500, reset_at=clock.now() + 4 * 3600)
    assert budget.allowance(3600) == 250  # 1000 * 3600 / 14400
    assert budget.allowance(8 * 3600) == 1000  # the reset comes before the next cycle


def test_the_reserve_must_fit_in_the_limit(clock: ManualClock) -> None:
    with pytest.raises(ValueError):
        QuotaBudget(clock, daily_limit=10, reserve=11)


def test_pacer_spaces_the_calls(clock: ManualClock) -> None:
    pacer = RatePacer(clock, per_second=4)
    start = clock.now()
    for _ in range(9):
        pacer.wait()
    assert clock.now() - start == pytest.approx(2.0)
