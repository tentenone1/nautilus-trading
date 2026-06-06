"""
Focused behavioral tests for strategies.capital_pool.

Observed-behavior only. No assertions about desired behavior.
If current behavior is ambiguous, test name documents it.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.capital_pool import CapitalPool, CategoryState, DEFAULT_ALLOCATIONS


class TestCapitalPoolInitialization:
    """1. initialization behavior"""

    def test_default_allocations_from_total_bankroll(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_category_allocation("sports") == 5_000.0
        assert pool.get_category_allocation("crypto") == 3_500.0
        assert pool.get_category_allocation("geopolitics") == 1_500.0

    def test_custom_allocations(self):
        pool = CapitalPool(
            total_bankroll=10_000.0,
            allocations={"sports": 0.40, "crypto": 0.40, "geopolitics": 0.20},
        )
        assert pool.get_category_allocation("sports") == 4_000.0
        assert pool.get_category_allocation("crypto") == 4_000.0
        assert pool.get_category_allocation("geopolitics") == 2_000.0

    def test_zero_bankroll_raises(self):
        try:
            CapitalPool(total_bankroll=0.0)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "total_bankroll must be positive" in str(e)

    def test_negative_bankroll_raises(self):
        try:
            CapitalPool(total_bankroll=-100.0)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "total_bankroll must be positive" in str(e)

    def test_allocations_must_sum_to_one(self):
        try:
            CapitalPool(
                total_bankroll=10_000.0,
                allocations={"sports": 0.50, "crypto": 0.30, "geopolitics": 0.10},
            )
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "Allocations must sum to 1.0" in str(e)

    def test_initial_state_zero_exposure(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_total_exposure() == 0.0
        assert pool.get_category_exposure("sports") == 0.0
        assert pool.get_category_exposure("crypto") == 0.0
        assert pool.get_category_exposure("geopolitics") == 0.0

    def test_initial_available_equals_allocation(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_category_available("sports") == 5_000.0
        assert pool.get_category_available("crypto") == 3_500.0
        assert pool.get_category_available("geopolitics") == 1_500.0


class TestCapitalPoolSuccessfulAllocation:
    """2. successful allocation within capacity"""

    def test_full_request_granted(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_capital("crypto", desired_size=500.0)
        assert granted == 500.0
        assert pool.get_category_exposure("crypto") == 500.0
        assert pool.get_category_available("crypto") == 3_000.0

    def test_partial_request_granted_when_less_than_capacity(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_capital("crypto", desired_size=100.0)
        assert granted == 100.0

    def test_request_increments_open_position_count(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=100.0)
        snap = pool.get_state_snapshot()
        assert snap["categories"]["crypto"]["open_positions"] == 1


class TestCapitalPoolOverCapacityRejection:
    """3. over-capacity rejection"""

    def test_request_exceeding_category_allocation_gets_partial(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_capital("geopolitics", desired_size=2_000.0)
        assert granted == 1_500.0  # capped at allocation

    def test_request_after_exhaustion_returns_zero(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("geopolitics", desired_size=1_500.0)
        granted = pool.request_capital("geopolitics", desired_size=100.0)
        assert granted == 0.0

    def test_max_positions_rejection(self):
        pool = CapitalPool(total_bankroll=10_000.0, max_positions={"geopolitics": 2})
        pool.request_capital("geopolitics", desired_size=100.0)
        pool.request_capital("geopolitics", desired_size=100.0)
        granted = pool.request_capital("geopolitics", desired_size=100.0)
        assert granted == 0.0

    def test_global_exposure_cap_rejection(self):
        pool = CapitalPool(total_bankroll=10_000.0, global_exposure_cap=0.10)
        # 10% of 10k = 1k global cap
        pool.request_capital("sports", desired_size=600.0)
        pool.request_capital("crypto", desired_size=400.0)
        # global exposure now at 1k
        granted = pool.request_capital("geopolitics", desired_size=100.0)
        assert granted == 0.0


class TestCapitalPoolFailedAllocationNoMutation:
    """4. failed allocation does not mutate state"""

    def test_rejected_request_does_not_increment_count(self):
        pool = CapitalPool(total_bankroll=10_000.0, max_positions={"crypto": 1})
        pool.request_capital("crypto", desired_size=100.0)
        snap_before = pool.get_state_snapshot()
        pool.request_capital("crypto", desired_size=100.0)  # rejected
        snap_after = pool.get_state_snapshot()
        assert snap_after["categories"]["crypto"]["open_positions"] == snap_before["categories"]["crypto"]["open_positions"]

    def test_rejected_request_does_not_change_exposure(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        exposure_before = pool.get_category_exposure("crypto")
        pool.request_capital("crypto", desired_size=5_000.0)  # will get partial then next rejected
        # First gets 3_000 (remaining), second gets 0
        # Wait, let's just exhaust and then check
        pool2 = CapitalPool(total_bankroll=10_000.0)
        pool2.request_capital("crypto", desired_size=3_500.0)
        exposure_before = pool2.get_category_exposure("crypto")
        pool2.request_capital("crypto", desired_size=100.0)  # rejected
        assert pool2.get_category_exposure("crypto") == exposure_before


class TestCapitalPoolReleaseRestoresCapacity:
    """5. release of known allocation restores capacity"""

    def test_release_restores_available(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        pool.release_capital("crypto", pnl=50.0, position_size=500.0)
        assert pool.get_category_available("crypto") == 3_500.0

    def test_release_reduces_exposure(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        pool.release_capital("crypto", pnl=50.0, position_size=500.0)
        assert pool.get_category_exposure("crypto") == 0.0

    def test_release_reduces_open_position_count(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        snap_before = pool.get_state_snapshot()
        assert snap_before["categories"]["crypto"]["open_positions"] == 1
        pool.release_capital("crypto", pnl=50.0, position_size=500.0)
        snap_after = pool.get_state_snapshot()
        assert snap_after["categories"]["crypto"]["open_positions"] == 0


class TestCapitalPoolReleaseIsolation:
    """6. releasing one position does not release another"""

    def test_release_one_of_two_positions(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=300.0)
        pool.request_capital("crypto", desired_size=200.0)
        pool.release_capital("crypto", pnl=10.0, position_size=300.0)
        assert pool.get_category_exposure("crypto") == 200.0
        assert pool.get_category_available("crypto") == 3_300.0


class TestCapitalPoolRepeatedRelease:
    """7. repeated release does not create excess capacity"""

    def test_double_release_does_not_go_negative(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        pool.release_capital("crypto", pnl=0.0, position_size=500.0)
        pool.release_capital("crypto", pnl=0.0, position_size=500.0)
        assert pool.get_category_exposure("crypto") == 0.0
        assert pool.get_category_available("crypto") == 3_500.0


class TestCapitalPoolAccumulation:
    """8. multiple allocations accumulate exposure correctly"""

    def test_multiple_allocations_sum(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("sports", desired_size=1_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        pool.request_capital("geopolitics", desired_size=300.0)
        assert pool.get_total_exposure() == 1_800.0
        assert pool.get_category_exposure("sports") == 1_000.0
        assert pool.get_category_exposure("crypto") == 500.0
        assert pool.get_category_exposure("geopolitics") == 300.0

    def test_multiple_same_category_allocations_sum(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=500.0)
        pool.request_capital("crypto", desired_size=300.0)
        assert pool.get_category_exposure("crypto") == 800.0
        assert pool.get_category_available("crypto") == 2_700.0


class TestCapitalPoolUnknownCategoryBehavior:
    """9. unknown/invalid category behavior is documented as current behavior"""

    def test_request_capital_raises_keyerror_for_unknown(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        try:
            pool.request_capital("finance", desired_size=100.0)
            assert False, "Expected KeyError"
        except KeyError as e:
            assert "Unknown category" in str(e)

    def test_request_fade_capital_falls_back_to_first_known_category(self):
        """
        OBSERVED: request_fade_capital falls back to CATEGORIES[0] ("sports")
        for unknown categories instead of raising or returning 0.
        This is the current implementation behavior.
        """
        pool = CapitalPool(total_bankroll=10_000.0)

        granted = pool.request_fade_capital("finance", desired_size=100.0)

        assert granted == 100.0
        snap = pool.get_state_snapshot()
        assert snap["categories"]["sports"]["fade_exposure"] == 100.0
        assert snap["categories"]["sports"]["current_exposure"] == 100.0
        assert "finance" not in snap["categories"]

    def test_get_category_allocation_returns_zero_for_unknown(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_category_allocation("finance") == 0.0

    def test_get_category_available_returns_zero_for_unknown(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_category_available("finance") == 0.0

    def test_get_category_exposure_returns_zero_for_unknown(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_category_exposure("finance") == 0.0

    def test_release_capital_silently_ignores_unknown_category(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        # Should not raise
        pool.release_capital("finance", pnl=0.0, position_size=100.0)


class TestCapitalPoolFadeBucket:
    """10. fade bucket behavior"""

    def test_fade_allocation_is_20_percent_of_category_by_default(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        snap = pool.get_state_snapshot()
        # sports: 5000 * 0.20 = 1000 fade capacity
        assert abs(snap["categories"]["sports"]["fade_available"] - 1_000.0) < 0.01
        # crypto: 3500 * 0.20 = 700 fade capacity
        assert abs(snap["categories"]["crypto"]["fade_available"] - 700.0) < 0.01

    def test_fade_request_grants_within_fade_bucket(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_fade_capital("crypto", desired_size=200.0)
        assert granted == 200.0
        assert pool.get_category_exposure("crypto") == 200.0
        snap = pool.get_state_snapshot()
        assert snap["categories"]["crypto"]["fade_exposure"] == 200.0

    def test_fade_request_capped_at_fade_bucket(self):
        """
        OBSERVED: request_fade_capital applies a hard 2% bankroll cap
        BEFORE checking fade bucket capacity. For a 10k bankroll, any
        desired_size > 200 is first capped to 200, then compared to
        fade_available (700 for crypto). The 2% cap is binding.
        """
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_fade_capital("crypto", desired_size=1_000.0)
        # Hard cap: 2% of 10k = 200, applied before fade bucket check
        assert granted == 200.0

    def test_fade_request_does_not_increment_open_position_count(self):
        """
        OBSERVED: request_fade_capital does NOT increment open_position_count.
        It increments fade_open_count instead. This means fade requests
        do NOT count toward the max_positions limit checked at the top
        of request_fade_capital(). Only normal positions count.
        When no normal positions exist, fade requests can exceed max_positions.
        """
        pool = CapitalPool(total_bankroll=10_000.0, max_positions={"crypto": 2})
        snap_before = pool.get_state_snapshot()
        assert snap_before["categories"]["crypto"]["open_positions"] == 0
        pool.request_fade_capital("crypto", desired_size=100.0)
        snap_after = pool.get_state_snapshot()
        assert snap_after["categories"]["crypto"]["open_positions"] == 0
        assert snap_after["categories"]["crypto"]["fade_open_count"] == 1

    def test_fade_request_blocked_when_normal_positions_at_max(self):
        """
        OBSERVED: request_fade_capital checks open_position_count >= max_positions.
        Since fade requests do not increment open_position_count, they only
        get blocked when normal positions have already filled the slots.
        """
        pool = CapitalPool(total_bankroll=10_000.0, max_positions={"crypto": 2})
        pool.request_capital("crypto", desired_size=100.0)
        pool.request_capital("crypto", desired_size=100.0)
        # Now open_position_count == 2 == max_positions
        granted = pool.request_fade_capital("crypto", desired_size=100.0)
        assert granted == 0.0

    def test_fade_release_reduces_fade_exposure(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_fade_capital("crypto", desired_size=200.0)
        pool.release_capital("crypto", pnl=10.0, position_size=200.0, is_fade=True)
        snap = pool.get_state_snapshot()
        assert snap["categories"]["crypto"]["fade_exposure"] == 0.0

    def test_fade_pnl_tracked_separately(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_fade_capital("crypto", desired_size=200.0)
        pool.release_capital("crypto", pnl=-50.0, position_size=200.0, is_fade=True)
        snap = pool.get_state_snapshot()
        assert snap["categories"]["crypto"]["fade_pnl"] == -50.0
        assert snap["categories"]["crypto"]["total_pnl"] == -50.0

    def test_fade_request_hard_capped_at_2_percent_bankroll(self):
        """
        OBSERVED: request_fade_capital hard-caps desired_size at 2% of bankroll
        before checking fade bucket. For default fade_allocation_pct=0.20,
        the fade bucket is typically larger than 2%, so the 2% cap is binding.
        """
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_fade_capital("sports", desired_size=5_000.0)
        # 2% of 10k = 200, sports fade_available = 1000
        assert granted == 200.0


class TestCapitalPoolRequestReleaseCycles:
    """11. repeated request/release cycles return pool to original state"""

    def test_single_cycle_restores_original_state(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        before = pool.get_state_snapshot()
        pool.request_capital("crypto", desired_size=500.0)
        pool.release_capital("crypto", pnl=25.0, position_size=500.0)
        after = pool.get_state_snapshot()
        assert after["categories"]["crypto"]["current_exposure"] == before["categories"]["crypto"]["current_exposure"]
        assert after["categories"]["crypto"]["open_positions"] == before["categories"]["crypto"]["open_positions"]

    def test_many_cycles_accumulate_pnl_only(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        for i in range(5):
            pool.request_capital("crypto", desired_size=100.0)
            pool.release_capital("crypto", pnl=10.0, position_size=100.0)
        assert pool.get_category_exposure("crypto") == 0.0
        assert pool.get_category_available("crypto") == 3_500.0
        assert pool.get_category_pnl("crypto") == 50.0


class TestCapitalPoolNoNegativeCapacity:
    """12. no negative available capacity"""

    def test_available_capacity_never_negative(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=3_500.0)
        assert pool.get_category_available("crypto") == 0.0
        # Try to over-allocate by releasing then double-releasing
        pool.release_capital("crypto", pnl=0.0, position_size=3_500.0)
        pool.release_capital("crypto", pnl=0.0, position_size=3_500.0)
        assert pool.get_category_available("crypto") == 3_500.0
        assert pool.get_category_available("crypto") >= 0.0

    def test_category_state_available_capacity_clamps_to_zero(self):
        state = CategoryState(allocation_pct=0.35, max_positions=8)
        state.current_exposure = 4_000.0  # exceeds 3500 cap
        available = state.available_capacity(10_000.0)
        assert available == 0.0


class TestCapitalPoolMaxCapacityInvariant:
    """13. no available capacity above configured max after valid operations"""

    def test_available_never_exceeds_allocation(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        for _ in range(3):
            pool.request_capital("sports", desired_size=1_000.0)
            pool.release_capital("sports", pnl=0.0, position_size=1_000.0)
        assert pool.get_category_available("sports") <= 5_000.0

    def test_exposure_never_exceeds_allocation_plus_granted(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        # Over-request: should be capped at allocation
        pool.request_capital("geopolitics", desired_size=2_000.0)
        assert pool.get_category_exposure("geopolitics") <= 1_500.0


class TestCapitalPoolObservedAmbiguities:
    """
    Document ambiguous or suspicious behaviors observed in the current code.
    These tests describe what the code actually does, not what it should do.
    """

    def test_position_size_parameter_in_request_capital_is_ignored(self):
        """
        OBSERVED: request_capital accepts a `position_size` parameter
        documented as "used instead of desired_size for exposure tracking",
        but the implementation ignores it entirely. Only `desired_size` is used.
        This is a documentation/code mismatch in the current code.
        """
        pool = CapitalPool(total_bankroll=10_000.0)
        granted = pool.request_capital("crypto", desired_size=100.0, position_size=999.0)
        assert granted == 100.0
        assert pool.get_category_exposure("crypto") == 100.0

    def test_release_capital_does_not_validate_position_size_matches_exposure(self):
        """
        OBSERVED: release_capital accepts any position_size and subtracts it
        from current_exposure, which can result in negative exposure if
        position_size > current_exposure. The max(0, ...) in CategoryState.release()
        prevents negative exposure, but allows releasing more than was allocated.
        """
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("crypto", desired_size=100.0)
        pool.release_capital("crypto", pnl=0.0, position_size=500.0)
        # Exposure clamped to 0, not -400
        assert pool.get_category_exposure("crypto") == 0.0

    def test_can_open_position_false_when_no_capacity(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        pool.request_capital("geopolitics", desired_size=1_500.0)
        assert pool.can_open_position("geopolitics") is False

    def test_update_bankroll_rejects_non_positive(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        try:
            pool.update_bankroll(0.0)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "new_total must be positive" in str(e)

    def test_get_total_exposure_empty_pool_is_zero(self):
        pool = CapitalPool(total_bankroll=10_000.0)
        assert pool.get_total_exposure() == 0.0
