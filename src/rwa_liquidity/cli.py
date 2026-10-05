"""Command-line interface.

The CLI is presentation only. Every number it shows comes from the same
functions the Python API exposes, so nothing can be computed one way on the
terminal and another way in a notebook.

Two things it goes out of its way to show, because a table of six numbers is
easy to misread:

* **The mode is in the header, always.** A turnover ratio means something
  different under `secondary_only` than under `all`, and a figure copied out of
  a terminal loses that context unless it was printed with it.
* **Caveats are printed, not swallowed.** Metrics carry warnings about truncated
  holder lists, unclassified transfers, and undefined denominators. They appear
  under the table rather than being dropped to keep the output tidy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Final

import polars as pl
import typer
from rich.console import Console
from rich.table import Table

from rwa_liquidity import __version__
from rwa_liquidity.demo import DEMO_LABEL, load_demo_dataset
from rwa_liquidity.metrics.base import DEFAULT_WINDOW_DAYS, Window
from rwa_liquidity.metrics.report import METRIC_COLUMNS, build_report, report_frame
from rwa_liquidity.reconcile import reconcile_snapshots
from rwa_liquidity.schema.types import Denomination, VolumeMode

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from rwa_liquidity.metrics.report import AssetReport
    from rwa_liquidity.sources import EvmRpcSource
    from rwa_liquidity.sources.base import Source

console = Console()

app = typer.Typer(
    name="rwa-liquidity",
    no_args_is_help=True,
    add_completion=False,
)

#: Cap on how many per-source failures to list before summarising the rest.
_MAX_FAILURES_SHOWN: Final = 5

#: How each metric is rendered. Ratios that are conceptually shares are shown as
#: percentages; turnover is left as a ratio because that is how it is quoted.
_FORMATS: dict[str, str] = {
    "turnover_ratio": "ratio",
    "active_holder_ratio": "percent",
    "volume_per_active_address": "quantity",
    "top_10_holder_share": "percent",
    "holder_hhi": "index",
    "dormancy": "percent",
}


def _version_callback(value: bool) -> None:
    """Print the version and exit, before any other option is processed."""
    if value:
        console.print(f"rwa-liquidity {__version__}")
        raise typer.Exit


@app.callback()
def main(
    # `version` is never read in the body: typer's eager callback fires during
    # parsing and exits before this function runs. The parameter exists so that
    # typer knows to register the option at all.
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the installed version and exit.",
        ),
    ] = False,
) -> None:
    """Measure liquidity in tokenized real-world asset (RWA) markets."""


def _cell(name: str, value: float | None) -> str:
    """Render one metric value, marking an undefined one as such."""
    if value is None:
        # Not "0", and not blank. The metric could not be computed, which is
        # different from it being zero and different from it being missing.
        return "[dim]n/a[/dim]"
    match _FORMATS.get(name, "ratio"):
        case "percent":
            return f"{value:.1%}"
        case "index":
            return f"{value:,.0f}"
        case "quantity":
            return f"{value:,.0f}"
        case _:
            return f"{value:.4f}"


def _render_table(reports: Sequence[AssetReport], *, mode: VolumeMode) -> None:
    if reports:
        # Printed above the table rather than as a title: a long title wraps and
        # becomes unreadable in an 80-column terminal, and the mode is the one
        # piece of context a copied figure must not lose.
        console.print(f"[bold]mode[/bold] = {mode}    [bold]window[/bold] = {reports[0].window}")
    table = Table(header_style="bold", expand=False)
    table.add_column("Asset", no_wrap=True)
    for _, label in METRIC_COLUMNS:
        table.add_column(label, justify="right")

    for report in reports:
        label = report.symbol or report.asset_uid
        table.add_row(label, *(_cell(name, report.value(name)) for name, _ in METRIC_COLUMNS))
    console.print(table)


def _render_caveats(reports: Sequence[AssetReport]) -> None:
    flagged = [(r, r.warnings) for r in reports if r.warnings]
    if not flagged:
        return
    console.print("\n[bold]Caveats[/bold]")
    for report, warnings in flagged:
        name = report.symbol or report.asset_uid
        for warning in warnings:
            console.print(f"  [yellow]•[/yellow] [bold]{name}[/bold]: {warning}")


def _render_reconciliation(snapshots: pl.DataFrame) -> None:
    report = reconcile_snapshots(snapshots)
    if report.compared == 0:
        return
    if report.agrees:
        console.print(
            f"\n[green]Sources agree[/green] across {report.compared} comparison(s) "
            f"within {report.tolerance:.0%}."
        )
        return
    console.print(
        f"\n[bold]Cross-source disagreement[/bold] "
        f"({len(report.disagreements)} of {report.compared} comparisons)"
    )
    for item in report.disagreements:
        console.print(f"  [yellow]•[/yellow] {item.describe()}")
    console.print(
        "  [dim]A disagreement is not automatically an error in either source; "
        "see docs/data-sources.md.[/dim]"
    )


@dataclass(frozen=True, slots=True)
class _Collected:
    """Frames gathered for a command, with what is known about their quality."""

    snapshots: pl.DataFrame
    transfers: pl.DataFrame
    holders: pl.DataFrame
    missing: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    reconciliation: Mapping[str, bool | None] = field(default_factory=dict)
    dropped_transfers: Mapping[str, int] = field(default_factory=dict)
    blocks: Mapping[Window, tuple[int, int, int]] = field(default_factory=dict)

    @property
    def unmeasured(self) -> frozenset[str]:
        """Return the assets missing transfers, holders, or both."""
        return frozenset(self.missing)


def _pinned_evm_source(head: int | None = None, **kwargs: Any) -> tuple[EvmRpcSource, int | None]:
    """Return an on-chain source pinned to `head`, or to the current head, and that block.

    Every asset in one run is then read at the same block, which is what makes a
    recorded block number mean anything. If the head cannot be read at all the
    source is returned unpinned, and the fetches that follow report the outage.
    """
    from rwa_liquidity.sources import EvmRpcSource, SourceError  # noqa: PLC0415

    if head is not None:
        return EvmRpcSource(head=head, **kwargs), head
    probe = EvmRpcSource(**kwargs)
    try:
        head = probe.head_block()
    except SourceError:
        return probe, None
    probe.close()
    return EvmRpcSource(head=head, **kwargs), head


def _window_blocks(
    source: EvmRpcSource, periods: Sequence[Window], head: int | None
) -> dict[Window, tuple[int, int, int]]:
    """Return `(first_block, last_block, head)` per window, or nothing if unknown."""
    from rwa_liquidity.sources import SourceError  # noqa: PLC0415

    if head is None:
        return {}
    try:
        return {period: (*source.block_range(period.start, period.end), head) for period in periods}
    except SourceError as error:
        console.print(f"  [yellow]-[/yellow] block numbers unavailable: {str(error)[:90]}")
        return {}


def _collect_live(period: Window, *, refresh: bool) -> _Collected:
    """Measure the registry's assets against live, keyless sources.

    The on-chain adapter comes first because its figures are derived from chain
    state and checked against the contract's own `totalSupply()`, so they are the
    ones to trust when a provider disagrees.
    """
    from rwa_liquidity.pipeline import collect  # noqa: PLC0415 -- keeps `--version` fast
    from rwa_liquidity.sources import (  # noqa: PLC0415
        DeFiLlamaPricesSource,
        issuer_addresses,
        load_defillama_registry,
        load_known_addresses,
    )

    assets = [entry.ref for entry in load_defillama_registry()]
    console.print(
        f"Measuring [bold]{len(assets)}[/bold] assets against a public Ethereum node. "
        f"The first run replays each token's full transfer history -- a cold run of the "
        f"whole registry can take a while, and every window is cached as it arrives, so "
        f"re-running after an interruption resumes rather than restarts. See "
        f"[bold]--demo[/bold] for an instant run, or docs/methodology.md for why this "
        f"one is slow."
    )

    evm, head = _pinned_evm_source(issuer_addresses=issuer_addresses(load_known_addresses()))
    sources: list[Source] = [evm, DeFiLlamaPricesSource()]
    try:
        result = collect(sources, assets, window=period, refresh=refresh)
        blocks = _window_blocks(evm, [period], head)
    finally:
        for source in sources:
            source.close()

    if result.failures:
        console.print(f"\n[yellow]{len(result.failures)} fetch(es) failed:[/yellow]")
        for name, message in result.failures[:_MAX_FAILURES_SHOWN]:
            console.print(f"  [yellow]-[/yellow] {name}: {message}")
        remaining = len(result.failures) - _MAX_FAILURES_SHOWN
        if remaining > 0:
            console.print(f"  [dim]... and {remaining} more[/dim]")

    console.print(
        f"\nsources: [bold]{', '.join(result.sources_used) or 'none'}[/bold]    "
        f"transfers: {result.transfers.height:,}    holders: {result.holders.height:,}\n"
    )
    return _Collected(
        snapshots=result.snapshots,
        transfers=result.transfers,
        holders=result.holders,
        missing=result.missing,
        reconciliation=result.reconciliation,
        dropped_transfers=result.dropped_transfers,
        blocks=blocks,
    )


def _collect_history(
    periods: Sequence[Window], *, refresh: bool, head: int | None = None
) -> _Collected:
    """Gather transfers plus a supply and holder history for every registry asset.

    One scan per asset serves every window: the on-chain adapter walks the full
    history anyway, so a window a year old costs no extra requests.
    """
    from rwa_liquidity.pipeline import _concat  # noqa: PLC0415
    from rwa_liquidity.schema.frames import (  # noqa: PLC0415
        AssetSnapshot,
        HolderBalance,
        TransferEvent,
    )
    from rwa_liquidity.sources import (  # noqa: PLC0415
        SourceError,
        issuer_addresses,
        load_defillama_registry,
        load_known_addresses,
    )

    ends = [period.end for period in periods]
    entries = load_defillama_registry()
    console.print(
        f"Reconstructing [bold]{len(entries)}[/bold] assets over "
        f"{len(periods)} windows from on-chain history."
    )

    snapshots: list[pl.DataFrame] = []
    transfers: list[pl.DataFrame] = []
    holders: list[pl.DataFrame] = []
    missing: dict[str, tuple[str, ...]] = {}

    source, head = _pinned_evm_source(
        head, issuer_addresses=issuer_addresses(load_known_addresses())
    )
    try:
        # Read every contract's supply at the pinned block first. A node keeps
        # recent state for a limited number of blocks, and a long run would
        # otherwise ask for the later assets' supply after it had been pruned.
        try:
            source.fetch_asset_snapshots([entry.ref for entry in entries], refresh=refresh)
        except SourceError as error:
            console.print(f"  [yellow]-[/yellow] supply at the read block: {str(error)[:90]}")
        for entry in entries:
            # Deliberately one try/except around all three calls, not one per
            # call. Splitting them would let an asset keep a snapshot or a
            # holder frame while its transfers failed -- and an empty transfer
            # frame reads as "did not trade" to every metric downstream, which
            # is the exact conflation this package exists to prevent (see
            # pipeline.py's own docstring). All-or-nothing costs a discarded
            # fetch on a partial failure; the alternative risks manufacturing
            # a liquidity finding from an outage. The safer failure direction
            # is worth the waste.
            try:
                asset_snapshots = source.supply_snapshots(entry.ref, ends, refresh=refresh)
                asset_holders = source.holder_snapshots(entry.ref, ends, refresh=refresh)
                asset_transfers = source.fetch_transfers(
                    entry.ref, start=periods[0].start, end=periods[-1].end, refresh=refresh
                )
            except SourceError as error:
                missing[entry.ref.uid] = ("transfers", "holders")
                console.print(f"  [yellow]-[/yellow] {entry.symbol}: {str(error)[:90]}")
                continue
            snapshots.append(asset_snapshots)
            holders.append(asset_holders)
            transfers.append(asset_transfers)
        blocks = _window_blocks(source, periods, head)
        reconciliation = source.reconciliation_status()
        dropped = source.dropped_transfer_counts()
    finally:
        source.close()

    console.print()
    return _Collected(
        snapshots=_concat(snapshots, AssetSnapshot),
        transfers=_concat(transfers, TransferEvent),
        holders=_concat(holders, HolderBalance),
        missing=missing,
        reconciliation=reconciliation,
        dropped_transfers=dropped,
        blocks=blocks,
    )


@app.command()
def report(  # noqa: PLR0913 -- each option changes what the numbers mean and
    # belongs on the command line rather than hidden in a config file.
    *,
    demo: Annotated[
        bool,
        typer.Option("--demo", help="Use the committed synthetic sample dataset. No API keys."),
    ] = False,
    mode: Annotated[
        VolumeMode,
        typer.Option("--mode", help="Which transfer kinds to count."),
    ] = VolumeMode.SECONDARY_ONLY,
    denomination: Annotated[
        Denomination,
        typer.Option("--denomination", help="Units for volume-based metrics."),
    ] = Denomination.NATIVE,
    days: Annotated[
        int,
        typer.Option("--days", "-d", min=1, help="Length of the observation window."),
    ] = DEFAULT_WINDOW_DAYS,
    top_n: Annotated[
        int,
        typer.Option("--top-n", min=1, help="How many holders the concentration share covers."),
    ] = 10,
    refresh: Annotated[
        bool,
        typer.Option("--refresh", help="Bypass the response cache and refetch."),
    ] = False,
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Write the table to a file (.csv, .parquet, .tex)."),
    ] = None,
) -> None:
    """Compute liquidity metrics and print them as a table.

    Without `--demo` this measures real assets against a public Ethereum node,
    which also needs no API keys but takes a minute on a cold cache. `--demo`
    reads the committed sample dataset instead and is instant.
    """
    if demo:
        dataset = load_demo_dataset()
        console.print(f"[bold yellow]{DEMO_LABEL}[/bold yellow]\n")
        window = (
            dataset.window
            if days == DEFAULT_WINDOW_DAYS
            else Window.ending(dataset.window.end, days=days)
        )
        collected = _Collected(dataset.snapshots, dataset.transfers, dataset.holders)
    else:
        window = Window.ending(datetime.now(UTC), days=days)
        collected = _collect_live(window, refresh=refresh)
    snapshots = collected.snapshots

    from rwa_liquidity.sources import (  # noqa: PLC0415 -- keeps `--version` fast
        excluded_contracts,
        load_known_addresses,
    )

    reports = build_report(
        snapshots,
        collected.transfers,
        collected.holders,
        window=window,
        mode=mode,
        denomination=denomination,
        top_n=top_n,
        exclude=excluded_contracts(load_known_addresses()),
        missing=collected.missing,
        reconciliation=collected.reconciliation,
        dropped_transfers=collected.dropped_transfers,
        blocks=collected.blocks.get(window),
    )

    _render_table(reports, mode=mode)
    _render_caveats(reports)
    _render_reconciliation(snapshots)

    if out is not None:
        from rwa_liquidity.export import write_frame  # noqa: PLC0415 -- optional path

        written = write_frame(
            report_frame(reports),
            out,
            caption=f"Liquidity metrics ({mode}, {days}-day window). Synthetic sample data.",
            column_labels=dict(METRIC_COLUMNS),
        )
        console.print(f"\nWrote [bold]{written}[/bold]")


@app.command()
def trend(  # noqa: PLR0913 -- window geometry, which metric, which mode, cache
    # policy and destination. Each changes the output and belongs on the command
    # line rather than in a config file.
    *,
    days: Annotated[
        int, typer.Option("--days", "-d", min=1, help="Length of each window.")
    ] = DEFAULT_WINDOW_DAYS,
    periods: Annotated[
        int, typer.Option("--periods", "-p", min=2, max=24, help="How many windows.")
    ] = 6,
    metric: Annotated[
        str, typer.Option("--metric", help="Which metric to track.")
    ] = "turnover_ratio",
    mode: Annotated[VolumeMode, typer.Option("--mode")] = VolumeMode.SECONDARY_ONLY,
    refresh: Annotated[bool, typer.Option("--refresh")] = False,
    out: Annotated[Path | None, typer.Option("--out", "-o")] = None,
) -> None:
    """Track one metric across consecutive windows, to see whether it is moving.

    Supply and holder distributions are reconstructed from the ledger at each
    window's end rather than taken from today, so a fund that has grown does not
    show a falsely collapsing turnover because its denominator moved.
    """
    from rwa_liquidity.metrics.trend import (  # noqa: PLC0415 -- keeps `--version` fast
        build_trend,
        trend_frame,
        windows_ending,
    )

    known = {name for name, _ in METRIC_COLUMNS}
    if metric not in known:
        console.print(
            f"[red]Unknown metric {metric!r}.[/red] Choose one of: {', '.join(sorted(known))}"
        )
        raise typer.Exit(code=2)

    from rwa_liquidity.sources import (  # noqa: PLC0415 -- keeps `--version` fast
        excluded_contracts,
        load_known_addresses,
    )

    periods_of = windows_ending(datetime.now(UTC), days=days, periods=periods)
    collected = _collect_history(periods_of, refresh=refresh)
    trends = build_trend(
        collected.snapshots,
        collected.transfers,
        collected.holders,
        windows=periods_of,
        metric=metric,
        mode=mode,
        exclude=excluded_contracts(load_known_addresses()),
        missing=collected.missing,
        reconciliation=collected.reconciliation,
        dropped_transfers=collected.dropped_transfers,
        blocks=collected.blocks,
    )

    console.print(
        f"[bold]{metric}[/bold]  mode = {mode}  {periods} windows of {days} days, oldest first"
    )
    table = Table(header_style="bold", expand=False)
    table.add_column("Asset", no_wrap=True)
    for period in periods_of:
        # Month-day only: the full date truncates in an 80-column terminal, and
        # the year is already implied by the header line above the table.
        table.add_column(f"{period.end:%m-%d}", justify="right")
    table.add_column("Direction", no_wrap=True)

    for item in trends:
        table.add_row(
            item.symbol or item.asset_uid,
            *(_cell(metric, value) for value in item.values),
            item.direction,
        )
    console.print(table)
    console.print(
        "  [dim]Direction compares the first defined point with the last, and is "
        "deliberately coarse: a handful of observations of a thin market cannot "
        "support a growth rate.[/dim]"
    )

    if out is not None:
        from rwa_liquidity.export import write_frame  # noqa: PLC0415

        written = write_frame(
            trend_frame(trends),
            out,
            caption=f"{metric} over {periods} windows of {days} days ({mode}).",
        )
        console.print(f"\nWrote [bold]{written}[/bold]")


@app.command()
def issuance(*, refresh: Annotated[bool, typer.Option("--refresh")] = False) -> None:
    """Report how each registry asset issues, from its complete history.

    The primary/secondary split rests on issuance passing through the zero
    address. Whether a given token's does is a fact about that token, checkable
    from its own history, and this is where the blanket caveat gets replaced by a
    per-asset answer.
    """
    from rwa_liquidity.sources import (  # noqa: PLC0415
        EvmRpcSource,
        SourceError,
        load_defillama_registry,
    )

    source = EvmRpcSource()
    table = Table(header_style="bold", expand=False)
    for column, justify in (
        ("Asset", "left"),
        ("Transfers", "right"),
        ("Mints", "right"),
        ("Burns", "right"),
        ("Ever minted", "right"),
        ("Issuance visible", "left"),
    ):
        table.add_column(column, justify=justify)  # type: ignore[arg-type]

    caveats: list[tuple[str, str]] = []
    try:
        for entry in load_defillama_registry():
            try:
                profile = source.describe_issuance(entry.ref, refresh=refresh)
            except SourceError as error:
                table.add_row(
                    entry.symbol, "[dim]not measurable[/dim]", "", "", "", str(error)[:40]
                )
                continue
            table.add_row(
                entry.symbol,
                f"{profile.transfers:,}",
                f"{profile.mints:,}",
                f"{profile.burns:,}",
                f"{profile.minted_supply:,.2f}",
                "[green]yes[/green]" if profile.issuance_is_visible else "[yellow]no[/yellow]",
            )
            note = profile.caveat()
            if note is not None:
                caveats.append((entry.symbol, note))
    finally:
        source.close()

    console.print(table)
    if caveats:
        console.print("\n[bold]Caveats[/bold]")
        for symbol, note in caveats:
            console.print(f"  [yellow]•[/yellow] [bold]{symbol}[/bold]: {note}")
    else:
        console.print(
            "\n[green]Issuance is visible for every asset measured.[/green] Each one "
            "mints through the zero address, so the primary/secondary split can see how "
            "it is issued and the secondary figures are measurements rather than upper "
            "bounds."
        )


@app.command()
def paper(
    *,
    first_month: Annotated[
        str, typer.Option("--first-month", help="First calendar month, YYYY-MM.")
    ],
    last_month: Annotated[str, typer.Option("--last-month", help="Last calendar month, YYYY-MM.")],
    head: Annotated[
        int | None,
        typer.Option("--head", help="Read at this block instead of the chain head."),
    ] = None,
    refresh: Annotated[bool, typer.Option("--refresh")] = False,
    out: Annotated[Path, typer.Option("--out", "-o")] = Path("paper"),
    registry: Annotated[
        str | None,
        typer.Option(
            "--registry",
            help="Asset list to measure: a file in the data package (e.g. research.toml) "
            "or a path to a TOML file. Defaults to defillama.toml.",
        ),
    ] = None,
) -> None:
    r"""Write the working paper's tables and panel from one run at one block.

    Every registry asset is measured over each calendar month from the same
    pinned block. The panel goes to `panel.csv`; the paper's Table 2, for the
    last month, to `table_turnover.tex`; X and F across all months to
    `table_months.tex`; and the figures quoted in the text to `numbers.tex`, all
    for `\input`. Passing the `head_block`
    recorded in the panel as `--head` repeats the run.
    """
    from rwa_liquidity.paper import (  # noqa: PLC0415 -- keeps `--version` fast
        TABLE_ASSETS,
        build_panel,
        months,
        months_table,
        paper_numbers,
        turnover_table,
    )
    from rwa_liquidity.sources import (  # noqa: PLC0415
        excluded_contracts,
        issuer_addresses,
        load_defillama_registry,
        load_known_addresses,
    )

    if registry:
        # Set for the whole run, so every registry read below sees the same list.
        import os  # noqa: PLC0415

        from rwa_liquidity.sources.registry import REGISTRY_ENV  # noqa: PLC0415

        os.environ[REGISTRY_ENV] = registry
    registry_entries = load_defillama_registry()
    console.print(
        f"Registry: [bold]{registry or 'defillama.toml'}[/bold] "
        f"({', '.join(entry.symbol for entry in registry_entries)})"
    )

    try:
        periods_of = months(first_month, last_month)
    except ValueError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error

    collected = _collect_history(periods_of, refresh=refresh, head=head)
    known = load_known_addresses()
    panel = build_panel(
        collected.snapshots,
        collected.transfers,
        collected.holders,
        assets=[(entry.ref.uid, entry.symbol) for entry in registry_entries],
        windows=periods_of,
        issuers=issuer_addresses(known),
        exclude=excluded_contracts(known),
        missing=collected.missing,
        reconciliation=collected.reconciliation,
        dropped_transfers=collected.dropped_transfers,
        blocks=collected.blocks,
    )

    out.mkdir(parents=True, exist_ok=True)
    panel.write_csv(out / "panel.csv", datetime_format="%Y-%m-%dT%H:%M:%SZ")
    written = {
        "table_turnover.tex": turnover_table(
            panel,
            periods_of[-1],
            # The default registry keeps the paper's four Table 2 funds; another
            # registry gets one row per asset it lists.
            assets=tuple(entry.symbol for entry in registry_entries) if registry else TABLE_ASSETS,
        ),
        "table_months.tex": months_table(panel),
        "numbers.tex": paper_numbers(panel, periods_of),
    }
    for name, text in written.items():
        (out / name).write_text(text, encoding="utf-8", newline="\n")
    for name in ("panel.csv", *written):
        console.print(f"Wrote [bold]{out / name}[/bold]")


@app.command()
def risk(
    *,
    panel_path: Annotated[
        Path, typer.Option("--panel", help="panel.csv written by the paper command.")
    ] = Path("paper/panel.csv"),
    prices_path: Annotated[
        Path | None,
        typer.Option(
            "--prices",
            help="CSV with symbol,price_usd (optional month YYYY-MM) for the dollar variables.",
        ),
    ] = None,
    out: Annotated[Path | None, typer.Option("--out", "-o")] = None,
) -> None:
    """Compute the Beyond TVL liquidity (L) and concentration (C) risk scores.

    Reads the asset-month panel, adds the paper's raw and derived variables, and
    min-max scales them across assets within each month. Market-quality risk M
    is cross-chain only and is not computed from Ethereum data; see risk.py.
    """
    from rwa_liquidity.risk import build_risk, load_prices  # noqa: PLC0415

    panel = pl.read_csv(panel_path, infer_schema_length=None)
    if "active_addresses" not in panel.columns:
        console.print(
            "[red]panel.csv has no active_addresses column. Re-run the paper command "
            "with this version first.[/red]"
        )
        raise typer.Exit(code=2)
    prices = load_prices(str(prices_path)) if prices_path else {}
    scores = build_risk(panel, prices)
    missing = sorted(
        set(scores.filter(pl.col("price_usd").is_null())["symbol"].to_list())
        if "price_usd" in scores.columns
        else []
    )
    if missing:
        console.print(
            f"[yellow]No price for {', '.join(missing)}: asset value, transfer volume in "
            f"USD, ATS and AVH are left empty for them, and L and C average the "
            f"remaining components.[/yellow]"
        )
    target = out or panel_path.with_name("risk.csv")
    scores.write_csv(target)
    console.print(f"Wrote [bold]{target}[/bold] ({scores.height} asset-months)")


@app.command()
def sources() -> None:
    """List the ingestion adapters and what each one can answer."""
    from rwa_liquidity.sources import (  # noqa: PLC0415 -- keeps `--version` fast
        DeFiLlamaPricesSource,
        DeFiLlamaProtocolTvlSource,
        DuneSource,
        RwaXyzSource,
    )

    table = Table(title="Ingestion adapters", header_style="bold")
    table.add_column("Source")
    table.add_column("Capabilities")
    table.add_column("Key")
    table.add_column("Verified live")

    # Annotated explicitly: a bare list of differing classes is joined by mypy to
    # their shared metaclass, which has none of the attributes read below.
    rows: list[tuple[type[Source], str, str]] = [
        (DeFiLlamaPricesSource, "no", "yes"),
        (DeFiLlamaProtocolTvlSource, "no", "yes"),
        (RwaXyzSource, "yes", "[yellow]no[/yellow]"),
        (DuneSource, "yes", "[yellow]no[/yellow]"),
    ]
    for source_type, key, verified in rows:
        table.add_row(
            source_type.name,
            ", ".join(sorted(capability.value for capability in source_type.capabilities)),
            key,
            verified,
        )
    console.print(table)
    console.print(
        "\n[dim]'Verified live' means the adapter has been run against the real API. "
        "The others were written against published documentation; see "
        "docs/data-sources.md.[/dim]"
    )


@app.command()
def window(days: int = DEFAULT_WINDOW_DAYS) -> None:
    """Print the observation window that `--days` would produce, ending now."""
    period = Window.ending(datetime.now(UTC), days=days)
    console.print(f"{period}  ({period.days:.0f} days)")
