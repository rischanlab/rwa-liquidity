r"""The working paper's quantities, computed from one pinned run.

Everything the paper reports is built here, from the frames of a single run
read at a single block, so the numbers in the paper and the numbers in this
repository cannot drift apart. `rwa-liquidity paper` writes the results into
`paper/`: the tables as `.tex` fragments for `\\input`, and the asset-window
panel they are drawn from as CSV.

The definitions follow the paper's Section 4 and are stated again where each is
computed. Two differ from the rest of this package on purpose:

* **Five event categories instead of four transfer kinds.** The paper separates
  issuer-linked movements from creation and destruction, where `classify.py`
  counts both as primary. Creation and destruction take precedence over the
  issuer label, as the paper's Table 1 requires.
* **Participation is bounded.** `P` counts only end-of-window holders who took
  part in a positive-value residual transfer that was not a self-transfer
  (the paper's equations 6 and 7), so it cannot exceed one.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, cast

import polars as pl

from rwa_liquidity.metrics.base import Window
from rwa_liquidity.schema.types import BURN_ADDRESSES

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence

__all__ = [
    "COVERAGE_FLOOR",
    "EVENT_CATEGORIES",
    "PANEL_SCHEMA",
    "TABLE_ASSETS",
    "build_panel",
    "categorize",
    "months",
    "months_table",
    "paper_numbers",
    "turnover_table",
]

#: The paper's event categories, in its order: creation, destruction,
#: issuer-linked, residual, unresolved.
EVENT_CATEGORIES: Final = ("creation", "destruction", "issuer_linked", "residual", "unresolved")

#: How many of the largest balances the top-k shares sum.
TOP_K: Final = 10

#: Retained coverage below which conditional concentration is not reported. A
#: conditional figure describes the retained holders among themselves, and
#: when they hold less than half the supply that says little about the asset.
COVERAGE_FLOOR: Final = 0.5

#: The HHI scale the paper uses.
_HHI_SCALE: Final = 10_000

#: What an undefined value becomes in a table. An empty cell would look like an
#: oversight; this says the quantity is undefined, which is a result.
_UNDEFINED: Final = "--"


def months(first: str, last: str) -> list[Window]:
    """Return calendar-month windows from `first` to `last`, both `YYYY-MM`, inclusive."""
    start = datetime.strptime(first, "%Y-%m").replace(tzinfo=UTC)
    stop = datetime.strptime(last, "%Y-%m").replace(tzinfo=UTC)
    windows: list[Window] = []
    while start <= stop:
        end = start.replace(year=start.year + start.month // 12, month=start.month % 12 + 1)
        windows.append(Window(start=start, end=end))
        start = end
    if not windows:
        raise ValueError(f"{last} is before {first}")
    return windows


def categorize(transfers: pl.DataFrame, *, issuers: Collection[str] = ()) -> pl.DataFrame:
    """Add the paper's `category` column to a frame of transfers.

    Creation is a transfer out of the zero address, destruction one into the
    zero or a burn address, and a transfer that is both is unresolved. Of the
    rest, a transfer to or from a documented issuer address is issuer-linked and
    everything else is residual.
    """
    burn = list(BURN_ADDRESSES)
    known = [address.lower() for address in issuers]
    sender = pl.col("from_address").str.to_lowercase()
    recipient = pl.col("to_address").str.to_lowercase()
    from_burn = sender.is_in(burn)
    to_burn = recipient.is_in(burn)
    issuer = (sender.is_in(known) | recipient.is_in(known)) if known else pl.lit(value=False)
    return transfers.with_columns(
        pl.when(from_burn & to_burn)
        .then(pl.lit("unresolved"))
        .when(from_burn)
        .then(pl.lit("creation"))
        .when(to_burn)
        .then(pl.lit("destruction"))
        .when(issuer)
        .then(pl.lit("issuer_linked"))
        .otherwise(pl.lit("residual"))
        .alias("category")
    )


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


def _activity(transfers: pl.DataFrame, supply: float | None) -> dict[str, object]:
    """Return category counts and volumes, and the turnover decomposition (eqs 1-5)."""
    row: dict[str, object] = {}
    volumes: dict[str, float] = {}
    for category in EVENT_CATEGORIES:
        chosen = transfers.filter(pl.col("category") == category)
        volumes[category] = float(chosen["amount"].sum()) if chosen.height else 0.0
        row[f"n_{category}"] = chosen.height
        row[f"v_{category}"] = volumes[category]
    v_all = sum(volumes.values())
    v_res = volumes["residual"]
    row["n_all"] = transfers.height
    # Active addresses: every distinct address that sent or received the token in
    # the window, of any category, burn addresses aside. This is the "active
    # addresses (30 days)" count of the Beyond TVL risk framework.
    if transfers.height:
        senders = transfers["from_address"].str.to_lowercase().to_list()
        recipients = transfers["to_address"].str.to_lowercase().to_list()
        row["active_addresses"] = len((set(senders) | set(recipients)) - BURN_ADDRESSES)
    else:
        row["active_addresses"] = 0
    row["v_all"] = v_all
    row["t_all"] = _ratio(v_all, supply) if supply is not None else None
    row["t_res"] = _ratio(v_res, supply) if supply is not None else None
    # F needs positive residual volume and X positive total volume. When both
    # volumes are zero neither is defined: there is nothing to decompose.
    row["f"] = _ratio(v_all, v_res)
    row["x"] = 1 - v_res / v_all if v_all > 0 else None
    return row


def _ownership(
    balances: Mapping[str, float],
    transfers: pl.DataFrame,
    *,
    supply: float,
    exclude: Collection[str],
) -> dict[str, object]:
    """Return participation, dormancy and the coverage decomposition (eqs 6-15)."""
    residual = transfers.filter(
        (pl.col("category") == "residual")
        & (pl.col("amount") > 0)
        & (pl.col("from_address").str.to_lowercase() != pl.col("to_address").str.to_lowercase())
    )
    participants = {
        address.lower()
        for address in residual["from_address"].to_list() + residual["to_address"].to_list()
    } - BURN_ADDRESSES

    holders = {address.lower(): balance for address, balance in balances.items()}
    excluded = {address.lower() for address in exclude}
    shares = {address: balance / supply for address, balance in holders.items()}
    retained = {address: share for address, share in shares.items() if address not in excluded}

    coverage = sum(retained.values())
    assessable = coverage >= COVERAGE_FLOOR
    h_ret = _HHI_SCALE * sum(share**2 for share in retained.values())
    k_ret = sum(sorted(retained.values(), reverse=True)[:TOP_K])
    return {
        "retained_holders": len(retained),
        "participation": len(participants & holders.keys()) / len(holders) if holders else None,
        "dormancy": sum(b for a, b in holders.items() if a not in participants) / supply,
        "h_all": _HHI_SCALE * sum(share**2 for share in shares.values()),
        "coverage": coverage,
        "h_ret": h_ret,
        "h_cond": h_ret / coverage**2 if assessable else None,
        "k_all": sum(sorted(shares.values(), reverse=True)[:TOP_K]),
        "k_ret": k_ret,
        "k_cond": k_ret / coverage if assessable else None,
        "assessable": assessable,
    }


#: Columns filled only from a validated holder distribution.
_OWNERSHIP_COLUMNS: Final = (
    "retained_holders",
    "participation",
    "dormancy",
    "h_all",
    "coverage",
    "h_ret",
    "h_cond",
    "k_all",
    "k_ret",
    "k_cond",
)

_TEXT: Final = pl.String()
_COUNT: Final = pl.Int64()
_REAL: Final = pl.Float64()
_TIME: Final = pl.Datetime("us", "UTC")

#: The panel's columns and types, so an all-missing run still has its schema.

PANEL_SCHEMA: Final = pl.Schema(
    [
        ("asset_uid", _TEXT),
        ("symbol", _TEXT),
        ("window_start", _TIME),
        ("window_end", _TIME),
        ("start_block", _COUNT),
        ("end_block", _COUNT),
        ("head_block", _COUNT),
        ("missing", _TEXT),
        ("reconciled", pl.Boolean()),
        ("dropped_transfers", _COUNT),
        ("supply", _REAL),
        ("holders", _COUNT),
        *((f"n_{category}", _COUNT) for category in EVENT_CATEGORIES),
        ("n_all", _COUNT),
        ("active_addresses", _COUNT),
        *((f"v_{category}", _REAL) for category in EVENT_CATEGORIES),
        ("v_all", _REAL),
        ("t_all", _REAL),
        ("t_res", _REAL),
        ("f", _REAL),
        ("x", _REAL),
        ("retained_holders", _COUNT),
        *((name, _REAL) for name in _OWNERSHIP_COLUMNS if name != "retained_holders"),
        ("assessable", pl.Boolean()),
    ]
)


def build_panel(  # noqa: PLR0913 -- the three frames, what to measure, and what is known
    # about the run's quality; each is needed for one of the panel's columns.
    snapshots: pl.DataFrame,
    transfers: pl.DataFrame,
    holders: pl.DataFrame,
    *,
    assets: Sequence[tuple[str, str]],
    windows: Sequence[Window],
    issuers: Mapping[str, Collection[str]] | None = None,
    exclude: Mapping[str, Collection[str]] | None = None,
    missing: Mapping[str, Sequence[str]] | None = None,
    reconciliation: Mapping[str, bool | None] | None = None,
    dropped_transfers: Mapping[str, int] | None = None,
    blocks: Mapping[Window, tuple[int, int, int]] | None = None,
) -> pl.DataFrame:
    """Return one row per asset and window with every quantity the paper uses.

    Supply `S` is the ledger's supply at the window's end, and balances are
    taken at the same instant, so the two describe one moment. Ownership
    quantities are left empty for an asset whose ledger did not reconcile with
    the contract's `totalSupply()`, and every quantity is empty for an asset the
    run could not fetch; `missing` says which data was absent.

    Args:
        snapshots: Supply rows, one per asset and window end.
        transfers: Transfer events covering every window.
        holders: Balances at each window end.
        assets: `(asset_uid, symbol)` pairs, in the order rows should appear.
        windows: The observation windows, oldest first.
        issuers: Documented issuer addresses per asset uid.
        exclude: Intermediary contracts left out of the retained population,
            per asset uid.
        missing: Per asset, the kinds of data that could not be fetched.
        reconciliation: Per asset, whether the ledger matched `totalSupply()`.
        dropped_transfers: Per asset, transfers dropped as larger than supply.
        blocks: Per window, `(start_block, end_block, head_block)`.

    Returns:
        A frame with the columns of `PANEL_SCHEMA`.
    """
    issuers = issuers or {}
    exclude = exclude or {}
    missing = missing or {}
    reconciliation = reconciliation or {}
    dropped_transfers = dropped_transfers or {}
    blocks = blocks or {}

    rows: list[dict[str, object]] = []
    for uid, symbol in assets:
        asset_transfers = categorize(
            transfers.filter(pl.col("asset_uid") == uid), issuers=issuers.get(uid, ())
        )
        asset_holders = holders.filter(pl.col("asset_uid") == uid)
        asset_supply = snapshots.filter(pl.col("asset_uid") == uid)
        for window in windows:
            first, last, head = blocks.get(window, (None, None, None))
            row: dict[str, object] = {
                "asset_uid": uid,
                "symbol": symbol,
                "window_start": window.start,
                "window_end": window.end,
                "start_block": first,
                "end_block": last,
                "head_block": head,
                "missing": ", ".join(missing[uid]) if uid in missing else None,
                "reconciled": reconciliation.get(uid),
                "dropped_transfers": dropped_transfers.get(uid),
            }
            rows.append(row)
            if uid in missing:
                continue

            at_end = asset_supply.filter(pl.col("as_of") == window.end)["total_supply"]
            supply = float(at_end[0]) if at_end.len() and at_end[0] is not None else None
            balances = asset_holders.filter(pl.col("as_of") == window.end)
            in_window = window.clip(asset_transfers, column="block_time")
            row["supply"] = supply
            row["holders"] = balances.height
            row.update(_activity(in_window, supply))
            if reconciliation.get(uid) is False or supply is None or supply <= 0:
                continue
            row.update(
                _ownership(
                    dict(zip(balances["address"], balances["balance"], strict=True)),
                    in_window,
                    supply=supply,
                    exclude=exclude.get(uid, ()),
                )
            )
    return pl.DataFrame(rows, schema=PANEL_SCHEMA)


def _cell(value: float | None, *, digits: int, scale: float = 1.0, thousands: bool = False) -> str:
    if value is None:
        return _UNDEFINED
    return f"{value * scale:,.{digits}f}" if thousands else f"{value * scale:.{digits}f}"


def _provenance(panel: pl.DataFrame, window: Window) -> str:
    """Return one LaTeX comment naming the window and the blocks it was read from.

    A comment does not appear in the compiled document; it records where the
    figures came from for whoever opens the source.
    """
    note = f"% rwa-liquidity paper: window [{window.start:%Y-%m-%d}, {window.end:%Y-%m-%d}) UTC"
    known = panel.filter(pl.col("window_end") == window.end).drop_nulls("start_block")
    if known.height:
        first = known.row(0, named=True)
        note += (
            f", blocks {first['start_block']} to {first['end_block']},"
            f" read at block {first['head_block']}"
        )
    return note + "."


def _row(panel: pl.DataFrame, window: Window, symbol: str) -> dict[str, Any]:
    """Return the measured row for `symbol` in `window`, or an empty one if there is none."""
    rows = panel.filter(
        (pl.col("window_end") == window.end)
        & (pl.col("symbol") == symbol)
        & pl.col("missing").is_null()
    )
    return rows.row(0, named=True) if rows.height else {}


#: The assets in the paper's Table 2, in its order.
TABLE_ASSETS: Final = ("BUIDL", "USYC", "OUSG", "FDIT")


def turnover_table(
    panel: pl.DataFrame, window: Window, *, assets: Sequence[str] = TABLE_ASSETS
) -> str:
    """Return the paper's Table 2 as a `tabular`: T_all, T_res, F and X.

    Only the `tabular` is written, so the caption and notes, which name the
    common window, stay in the manuscript. An asset the run could not measure
    keeps its row, with every figure undefined.
    """
    body = []
    for symbol in assets:
        row = _row(panel, window, symbol)
        cells = [
            symbol,
            _cell(row.get("t_all"), digits=4),
            _cell(row.get("t_res"), digits=4),
            _cell(row.get("f"), digits=1),
            _cell(row.get("x"), digits=1, scale=100),
        ]
        body.append(" & ".join(cells) + r" \\")
    return "\n".join(
        [
            _provenance(panel, window),
            r"\begin{tabular}{@{}lrrrr@{}}",
            r"\toprule",
            r"Asset & $T^{\mathrm{all}}$ & $T^{\mathrm{res}}$ & $F$ & $X$ (\%) \\",
            r"\midrule",
            *body,
            r"\bottomrule",
            r"\end{tabular}",
            "",
        ]
    )


def _month(window: Window) -> str:
    """Return the calendar month a window covers, as `December 2025`."""
    return f"{window.start:%B %Y}"


def _span_note(panel: pl.DataFrame) -> str:
    """Return one LaTeX comment naming the panel's months and the read block."""
    first = cast("datetime", panel["window_start"].min())
    last = cast("datetime", panel["window_end"].max())
    note = f"% rwa-liquidity paper: monthly windows [{first:%Y-%m-%d}, {last:%Y-%m-%d}) UTC"
    heads = panel["head_block"].drop_nulls()
    if heads.len():
        note += f", read at block {heads[0]}"
    return note + "."


def _spread(values: pl.Series, *, digits: int, scale: float = 1.0) -> tuple[str, str]:
    """Return a series' median and its range as cells, or undefined for an empty one."""
    values = values.drop_nulls()
    if not values.len():
        return _UNDEFINED, _UNDEFINED
    low, high = values.min(), values.max()
    median = _cell(values.median(), digits=digits, scale=scale)  # type: ignore[arg-type]
    span = (
        f"{_cell(low, digits=digits, scale=scale)}--"  # type: ignore[arg-type]
        f"{_cell(high, digits=digits, scale=scale)}"  # type: ignore[arg-type]
    )
    return median, span


def months_table(panel: pl.DataFrame) -> str:
    """Return X and F across every month of the panel, one row per measured asset.

    One month's factor can mislead: BUIDL's runs from 1.8 to 12.6 over the
    panel. The median and range over months show how stable the decomposition
    is. X comes first because it is bounded; F is unbounded when residual
    volume is small. Each is summarised over the months where it is defined,
    and "months" counts the months with any movement.
    """
    body = []
    for (symbol,), rows in panel.filter(pl.col("missing").is_null()).group_by(
        "symbol", maintain_order=True
    ):
        x_median, x_range = _spread(rows["x"], digits=1, scale=100)
        f_median, f_range = _spread(rows["f"], digits=1)
        moved = rows.filter(pl.col("v_all") > 0).height
        cells = [str(symbol), str(moved), x_median, x_range, f_median, f_range]
        body.append(" & ".join(cells) + r" \\")
    return "\n".join(
        [
            _span_note(panel),
            r"\begin{tabular}{lrrrrr}",
            r"\toprule",
            r"Asset & Months & Median $X$ (\%) & Range $X$ (\%) & Median $F$ & Range $F$ \\",
            r"\midrule",
            *body,
            r"\bottomrule",
            r"\end{tabular}",
            "",
        ]
    )


def _ownership_macros(row: Mapping[str, Any], prefix: str, suffix: str) -> list[str]:
    figures = {
        "Coverage": _cell(row.get("coverage"), digits=1, scale=100),
        "TopTenAll": _cell(row.get("k_all"), digits=1, scale=100),
        "TopTenRet": _cell(row.get("k_ret"), digits=1, scale=100),
        "TopTenCond": _cell(row.get("k_cond"), digits=1, scale=100),
        "HHIAll": _cell(row.get("h_all"), digits=0, thousands=True),
        "HHIRet": _cell(row.get("h_ret"), digits=0, thousands=True),
        "HHICond": _cell(row.get("h_cond"), digits=0, thousands=True),
    }
    return [
        rf"\newcommand{{\{prefix}{name}{suffix}}}{{{value}}}" for name, value in figures.items()
    ]


def paper_numbers(
    panel: pl.DataFrame,
    windows: Sequence[Window],
    *,
    ownership: str = "OUSG",
    factor: str = "BUIDL",
) -> str:
    r"""Return the figures the manuscript quotes in its text, as LaTeX macros.

    After `\input{numbers}`, `\OUSGTopTenCond\%` prints OUSG's top-ten share
    among retained holders in the last month and `\OUSGTopTenCondFirst\%` the
    same in the first. Section 5.2 reports both concentration conventions and
    the coverage between them; `\BUIDLFMedian` and its range report the
    classification factor across months rather than in one. Percentages carry
    no `%` sign, so the text keeps control of its own punctuation.
    """
    first, last = windows[0], windows[-1]
    lines = [
        _provenance(panel, last),
        rf"\newcommand{{\PanelFirstMonth}}{{{_month(first)}}}",
        rf"\newcommand{{\PanelLastMonth}}{{{_month(last)}}}",
        *_ownership_macros(_row(panel, last, ownership), ownership, ""),
        *_ownership_macros(_row(panel, first, ownership), ownership, "First"),
    ]
    factors = panel.filter((pl.col("symbol") == factor) & pl.col("missing").is_null())["f"]
    factors = factors.drop_nulls()
    median = _cell(factors.median() if factors.len() else None, digits=1)  # type: ignore[arg-type]
    low = _cell(factors.min() if factors.len() else None, digits=1)  # type: ignore[arg-type]
    high = _cell(factors.max() if factors.len() else None, digits=1)  # type: ignore[arg-type]
    lines += [
        rf"\newcommand{{\{factor}FMedian}}{{{median}}}",
        rf"\newcommand{{\{factor}FMin}}{{{low}}}",
        rf"\newcommand{{\{factor}FMax}}{{{high}}}",
    ]
    return "\n".join([*lines, ""])
