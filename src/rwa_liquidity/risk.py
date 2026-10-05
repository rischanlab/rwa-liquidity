"""Risk scores in the style of Mafrur & Khadijah, "Beyond TVL" (arXiv:2605.29689).

The paper scores tokenized RWAs on three dimensions, higher meaning riskier:

* **Liquidity risk L**, the mean of four min-max risk scores: turnover, active
  ratio and transfer intensity (protective: higher is safer) and average
  transfer size (risk-increasing).
* **Concentration risk C**, the mean of holders (protective), average value per
  holder (risk-increasing) and a concentration index scaled directly to 0-100.
* **Market-quality risk M**, built only from how activity is split across
  chains.

This module reproduces the variables of the paper's Tables 2-4 from Ethereum
data only, computed per calendar month from `panel.csv`, with two departures
forced by measuring a single chain:

* **M is not computed.** It depends entirely on cross-chain shares, which are
  identically 1 when only Ethereum is observed.
* **NHHI (holders by network) is replaced by the holder HHI** of the Ethereum
  holder distribution, the "Holder HHI" row of the paper's Table 3. Like NHHI it
  lies in [0, 1] and is scaled to 0-100 directly rather than min-max scaled.
  `C_two` reports C from holders and AVH alone, for readers who prefer to drop
  the third component instead of substituting it.

Composites therefore combine L and C only. The paper's weights are kept in
proportion with M removed: equal (1/2, 1/2), liquidity-heavy (0.50, 0.25 ->
2/3, 1/3) and concentration-heavy (0.25, 0.50 -> 1/3, 2/3).

Dollar variables (asset value, transfer volume, average transfer size, average
value per holder) need a price per token. A node does not know prices, so they
come from a CSV the user supplies (`symbol,price_usd`, optionally with a
`month` column in `YYYY-MM` for month-specific NAVs). Without a price those
variables are left empty and the scores average the components that remain;
`l_components` and `c_components` say how many were used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = ["RISK_WEIGHTS", "build_risk", "load_prices"]

#: Composite weights on (L, C), the paper's weights with M removed and the rest
#: kept in proportion.
RISK_WEIGHTS: Final[Mapping[str, tuple[float, float]]] = {
    "composite_equal": (0.5, 0.5),
    "composite_liquidity_heavy": (2 / 3, 1 / 3),
    "composite_concentration_heavy": (1 / 3, 2 / 3),
}

#: (variable, protective). Protective variables lower risk as they rise.
_LIQUIDITY: Final = (
    ("turnover", True),
    ("active_ratio", True),
    ("transfer_intensity", True),
    ("ats_usd", False),
)
_CONCENTRATION_MINMAX: Final = (("holders", True), ("avh_usd", False))


def load_prices(path: str) -> dict[tuple[str, str | None], float]:
    """Read `symbol,price_usd[,month]` into a lookup keyed by (symbol, month or None)."""
    frame = pl.read_csv(path, infer_schema_length=0)
    if "symbol" not in frame.columns or "price_usd" not in frame.columns:
        raise ValueError(f"{path} needs the columns symbol and price_usd")
    prices: dict[tuple[str, str | None], float] = {}
    for row in frame.iter_rows(named=True):
        raw = (row.get("price_usd") or "").strip()
        if not raw:
            continue
        month = (row.get("month") or "").strip() or None
        prices[(row["symbol"].strip(), month)] = float(raw)
    return prices


def _div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _times(value: float | None, price: float | None) -> float | None:
    return None if value is None or price is None else value * price


def _riskmin(values: Sequence[float | None], *, protective: bool) -> list[float | None]:
    """Min-max scale to 0-100 risk; protective variables are reversed.

    When every defined value is equal the scale has no spread and each score is
    set to 0, the lowest risk, since no asset is worse than another.
    """
    defined = [value for value in values if value is not None]
    if not defined:
        return [None] * len(values)
    low, high = min(defined), max(defined)
    scores: list[float | None] = []
    for value in values:
        if value is None:
            scores.append(None)
        elif high == low:
            scores.append(0.0)
        else:
            scores.append(100 * ((high - value) if protective else (value - low)) / (high - low))
    return scores


def _mean(values: Sequence[float | None]) -> tuple[float | None, int]:
    defined = [value for value in values if value is not None]
    return (sum(defined) / len(defined) if defined else None), len(defined)


def build_risk(
    panel: pl.DataFrame, prices: Mapping[tuple[str, str | None], float] | None = None
) -> pl.DataFrame:
    """Return one row per asset and month with the Beyond TVL variables and scores.

    Normalisation is cross-sectional, as in the paper: within each month, over
    the assets measured that month.
    """
    prices = prices or {}
    measured = panel.filter(pl.col("missing").is_null())
    rows: list[dict[str, object]] = []
    for record in measured.iter_rows(named=True):
        start = record["window_start"]
        month = start[:7] if isinstance(start, str) else f"{start:%Y-%m}"
        symbol = record["symbol"]
        price = prices.get((symbol, month), prices.get((symbol, None)))
        supply, holders = record["supply"], record["holders"]
        count, volume = record["n_all"], record["v_all"]
        active = record.get("active_addresses")
        asset_value = _times(supply, price)
        volume_usd = _times(volume, price)
        h_all = record["h_all"]
        rows.append(
            {
                "symbol": symbol,
                "month": month,
                "window_start": record["window_start"],
                "window_end": record["window_end"],
                "reconciled": record["reconciled"],
                "price_usd": price,
                # Table 2: raw variables.
                "asset_value_usd": asset_value,
                "supply_tokens": supply,
                "holders": holders,
                "active_addresses": active,
                "transfer_volume_usd": volume_usd,
                "transfer_volume_tokens": volume,
                "transfer_count": count,
                # Table 4: derived variables. Turnover is a ratio of token
                # amounts, so it needs no price.
                "turnover": _div(volume, supply),
                "active_ratio": _div(active, holders),
                "transfer_intensity": _div(count, holders),
                "ats_usd": _div(volume_usd, count),
                "avh_usd": _div(asset_value, holders),
                "holder_hhi": None if h_all is None else h_all / 10_000,
                # The same activity restricted to residual transfers, for
                # comparison with the event-classified measures.
                "turnover_residual": record["t_res"],
                "transfer_count_residual": record["n_residual"],
            }
        )

    frame = pl.DataFrame(rows, infer_schema_length=None)
    if frame.is_empty():
        return frame

    out: list[pl.DataFrame] = []
    for (_,), group in frame.group_by("month", maintain_order=True):
        data = group.to_dicts()
        for variable, protective in (*_LIQUIDITY, *_CONCENTRATION_MINMAX):
            scores = _riskmin([row[variable] for row in data], protective=protective)
            for row, score in zip(data, scores, strict=True):
                row[f"risk_{variable}"] = score
        for row in data:
            hhi = row["holder_hhi"]
            row["risk_holder_hhi"] = None if hhi is None else 100 * hhi
            row["L"], row["l_components"] = _mean([row[f"risk_{v}"] for v, _ in _LIQUIDITY])
            row["C"], row["c_components"] = _mean(
                [row["risk_holders"], row["risk_avh_usd"], row["risk_holder_hhi"]]
            )
            row["C_two"], _ = _mean([row["risk_holders"], row["risk_avh_usd"]])
            for name, (w_l, w_c) in RISK_WEIGHTS.items():
                both = row["L"] is not None and row["C"] is not None
                row[name] = w_l * row["L"] + w_c * row["C"] if both else None
        out.append(pl.DataFrame(data, infer_schema_length=None))
    return pl.concat(out, how="diagonal_relaxed")
