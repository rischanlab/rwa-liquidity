"""The Beyond TVL variables, checked against the paper's own Tables 2 and 4."""

from __future__ import annotations

import polars as pl
import pytest

from rwa_liquidity.risk import build_risk

# Table 2 of Mafrur & Khadijah (arXiv:2605.29689): asset value, holders,
# active addresses, transfer volume (USD), transfer count. Expressed here as
# tokens at a price of 1, so dollar and token amounts coincide.
TABLE_2 = {
    "BUIDL": (2_487_654_577, 108, 24, 1_092_239_594, 88),
    "BENJI": (823_165_231, 1_106, 17, 10_063_830, 19),
    "USTB": (721_054_020, 99, 21, 285_750_617, 709),
    "STAC": (101_323_883, 4, 1, 3_549_447, 1),
}

# Table 4: turnover, active ratio, transfer intensity, AVH.
TABLE_4 = {
    "BUIDL": (0.4391, 0.2222, 0.8148, 23_033_839),
    "BENJI": (0.0122, 0.0154, 0.0172, 744_273),
    "USTB": (0.3963, 0.2121, 7.1616, 7_283_374),
    "STAC": (0.0350, 0.2500, 0.2500, 25_330_971),
}


def _panel() -> pl.DataFrame:
    rows = []
    for symbol, (value, holders, active, volume, count) in TABLE_2.items():
        rows.append(
            {
                "symbol": symbol,
                "window_start": "2026-05-01T00:00:00Z",
                "window_end": "2026-06-01T00:00:00Z",
                "missing": None,
                "reconciled": True,
                "supply": float(value),
                "holders": holders,
                "active_addresses": active,
                "n_all": count,
                "v_all": float(volume),
                "h_all": 2_500.0,
                "t_res": 0.0,
                "n_residual": 0,
            }
        )
    return pl.DataFrame(rows)


def test_derived_variables_match_table_4() -> None:
    prices = {(symbol, None): 1.0 for symbol in TABLE_2}
    risk = {row["symbol"]: row for row in build_risk(_panel(), prices).iter_rows(named=True)}
    for symbol, (turnover, ratio, intensity, avh) in TABLE_4.items():
        row = risk[symbol]
        assert row["turnover"] == pytest.approx(turnover, abs=5e-5)
        assert row["active_ratio"] == pytest.approx(ratio, abs=5e-5)
        assert row["transfer_intensity"] == pytest.approx(intensity, abs=5e-5)
        assert row["avh_usd"] == pytest.approx(avh, abs=1)


def test_scores_are_on_a_0_to_100_scale_and_missing_prices_degrade_gracefully() -> None:
    risk = build_risk(_panel(), {})  # no prices: ATS and AVH are undefined
    assert risk["ats_usd"].is_null().all()
    assert risk["l_components"].to_list() == [3, 3, 3, 3]
    for column in ("L", "C", "composite_equal"):
        values = risk[column].drop_nulls().to_list()
        assert values
        assert all(0 <= value <= 100 for value in values)
    # The holder HHI is scaled directly, not min-max: 2,500 on the 0-10,000 scale.
    assert risk["risk_holder_hhi"].to_list() == [25.0] * 4
