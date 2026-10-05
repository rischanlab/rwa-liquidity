"""Ethereum JSON-RPC adapter: real on-chain data, no API key.

This is the only source that supplies all three normalized frames, and the only
one whose numbers this package can *verify* rather than trust.

**Why it exists.** Every liquidity metric here needs transfer-level data, and the
providers that sell it need a key. That left the package able to demonstrate its
metrics only on constructed data. Public Ethereum RPC endpoints serve
`eth_getLogs` and `eth_call` without credentials, so the same measurements can be
made directly against the chain.

**Why it is tractable.** Reconstructing holder balances means replaying a token's
entire `Transfer` history from deployment, which sounds prohibitive. It is not
hopeless, for exactly the assets this package studies: tokenized funds are thin.
BUIDL's complete history is about 15,000 logs, but reaching them costs one
request per 10,000-block window from deployment to the chain tip regardless of
how few of those blocks hold a transfer -- 641 requests for BUIDL, a few tens of
minutes against a free endpoint answering serially. A liquid retail token would
be hopeless here; a tokenized treasury fund is slow rather than impossible. See
`docs/methodology.md` for the measured cost of a cold scan and what a loaded
endpoint does to it.

**Why it is trustworthy.** After replaying the ledger, the reconstructed balances
are summed and compared against the contract's own `totalSupply()`. If the two
agree to the raw unit, the holder distribution is correct by construction --
there is no provider to take on faith and no truncated top-N list. If they
disagree, something about the token breaks the assumption (a rebasing balance, a
non-standard transfer path) and the adapter says so rather than publishing a
distribution it cannot justify.

That check is the strongest data-integrity property in the package, and it is
only possible because the data is derived rather than fetched.
"""

from __future__ import annotations

import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

import polars as pl

from rwa_liquidity.schema.frames import AssetSnapshot, HolderBalance, TransferEvent
from rwa_liquidity.schema.types import BURN_ADDRESSES, ZERO_ADDRESS
from rwa_liquidity.schema.validation import polars_schema, validate
from rwa_liquidity.sources.base import (
    Capability,
    Source,
    SourceFetchError,
    SourceTransportError,
)
from rwa_liquidity.sources.classify import classify_transfers
from rwa_liquidity.sources.http import CachedJSONClient

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping, Sequence
    from datetime import timedelta

    import httpx

    from rwa_liquidity.cache.store import ParquetCache
    from rwa_liquidity.schema.asset import AssetRef

__all__ = ["DEFAULT_RPC_URL", "TRANSFER_TOPIC", "EvmRpcSource", "IssuanceProfile"]

logger = logging.getLogger(__name__)

#: A keyless mainnet endpoint that serves `eth_getLogs`. Most public endpoints do
#: not: they answer `eth_blockNumber` and `eth_call` happily and then return 403
#: or a 50-block range cap for log queries. This one was verified on 2026-07-30;
#: override it with `rpc_url` or the `EVM_RPC_URL` environment variable.
DEFAULT_RPC_URL: Final = "https://rpc.mevblocker.io"

#: `keccak256("Transfer(address,address,uint256)")`.
TRANSFER_TOPIC: Final = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Function selectors, i.e. the first four bytes of the keccak hash of each
# signature. Hard-coded rather than computed: adding a keccak implementation to
# hash four constant strings would be a dependency for nothing.
_SELECTOR_DECIMALS: Final = "0x313ce567"  # decimals()
_SELECTOR_TOTAL_SUPPLY: Final = "0x18160ddd"  # totalSupply()
_SELECTOR_SYMBOL: Final = "0x95d89b41"  # symbol()
_SELECTOR_NAME: Final = "0x06fdde03"  # name()

#: An ERC-20 `Transfer` has two indexed parameters, so three topics. ERC-721
#: reuses the same event signature but indexes the token id as well, giving four.
#: Counting a stream of NFT transfers as fungible volume would be nonsense, so
#: four-topic logs are skipped.
_ERC20_TOPIC_COUNT: Final = 3

#: A 256-bit word is 32 bytes, i.e. 64 hex characters.
_WORD_HEX: Final = 64

#: Longest contract-supplied label kept; see `_sanitize`.
_MAX_LABEL_LENGTH: Final = 80

#: Padding byte on the older bytes32 string encoding.
NUL_BYTE: Final = b"\x00"

#: Default ceiling on how many logs a single scan will accept. A tokenized fund
#: is well inside this; a widely traded token is not, and stopping with a clear
#: message beats issuing thousands of requests against a free endpoint.
DEFAULT_MAX_LOGS: Final = 250_000

#: Retries per request. Public endpoints return transient 502/504 under load.
_RETRIES: Final = 3

#: Blocks per log query, or `None` to discover it. Public endpoints cap a query
#: by block span rather than by result count -- the default one answers "range N
#: exceeds limit of 10000" -- and the cap differs by provider, so a fixed guess
#: is either wasteful against a generous endpoint or wrong against a strict one.
#: Discovery costs about a dozen requests once per process and adapts to both.
DEFAULT_BLOCK_STEP: Final[int | None] = None

#: Floor on the adaptive step. Below this a full-history scan is hopeless
#: anyway, and shrinking further would turn one slow scan into a stuck one.
_MIN_BLOCK_STEP: Final = 500

#: JSON-RPC's code for a fault inside the server, as opposed to a complaint
#: about the request. The endpoint returns it when overloaded.
_JSONRPC_INTERNAL_ERROR: Final = -32603

#: Attempts to make when the node reports itself overloaded, and the base pause
#: between them in seconds. Longer than the transport's retry pause because this
#: is a node asking for room rather than a request that happened to fail: a scan
#: issuing hundreds of queries is the reason it needs the room.
_OVERLOAD_RETRIES: Final = 4
_OVERLOAD_PAUSE: Final = 3.0

#: How a node states its own block-span limit, as in "range 24999999 exceeds
#: limit of 10000". Not every endpoint says so, hence the fallback search, but
#: the ones that do give an exact answer for one request.
_SPAN_LIMIT: Final = re.compile(r"limit of (\d+)")

#: Default ceiling on how many `eth_getLogs` calls one scan may issue. A walk
#: from a token's deployment block in 10,000-block windows is in the hundreds,
#: so this is a backstop against a fault rather than a limit real work meets,
#: such as a range that keeps splitting for a reason other than its size.
DEFAULT_MAX_LOG_REQUESTS: Final = 20_000

#: Minimum seconds between requests. A full-history scan issues its requests in
#: a burst, which is exactly what trips a free endpoint's rate limiter; spacing
#: them costs a few seconds and avoids being throttled for minutes.
DEFAULT_MIN_INTERVAL: Final = 0.15

#: An instant after every transfer, for replaying the whole ledger.
_END_OF_TIME: Final = datetime.max.replace(tzinfo=UTC)

#: Balances below this many raw units are treated as zero. Integer arithmetic
#: makes this exact, so the only reason to have it at all is to drop the dust a
#: rounding-based token can leave behind.
_DUST: Final = 0


@dataclass(frozen=True, slots=True)
class IssuanceProfile:
    """What a token's complete history says about how it issues.

    The point of this is to replace a blanket caveat with a per-asset fact. The
    zero-address rule can only see issuance that goes through the zero address;
    whether a given token's issuance does is answerable from its own history.

    Attributes:
        asset_uid: The asset described.
        transfers: Every classifiable transfer in its history.
        mints: Transfers out of the zero or a burn address.
        burns: Transfers into one.
        minted_supply: Total ever minted, human-scaled.
        first_mint_block: Where issuance began, if it is visible at all.
        largest_recipient: The address that received the most minted supply. A
            candidate treasury when issuance is not visible, offered for review
            rather than applied: guessing an issuer address wrong would move real
            trading into the primary bucket.
        largest_recipient_share: That address's share of everything minted.
    """

    asset_uid: str
    transfers: int
    mints: int
    burns: int
    minted_supply: float
    first_mint_block: int | None
    largest_recipient: str | None
    largest_recipient_share: float | None

    @property
    def issuance_is_visible(self) -> bool:
        """Return whether any issuance passes through the zero address."""
        return self.mints > 0

    def caveat(self) -> str | None:
        """Return what this profile means for the asset's secondary figures."""
        if self.issuance_is_visible:
            return None
        share = self.largest_recipient_share
        suffix = ""
        if self.largest_recipient is not None and share is not None:
            suffix = (
                f" The largest recipient of supply is {self.largest_recipient}, holding "
                f"{share:.1%} of everything issued; if that is the issuer's own address, "
                f"passing it as an issuer address would reclassify its distributions."
            )
        return (
            "no issuance passes through the zero address anywhere in this token's "
            "history, so the zero-address rule cannot see how it is issued. Some of "
            "what is counted as secondary trading may be distribution from an issuer, "
            "which makes every secondary figure for this asset an upper bound." + suffix
        )


def _to_address(topic: str) -> str:
    """Extract an address from a 32-byte indexed topic.

    An indexed address is left-padded to a full word, so the address is the last
    20 bytes. Returned lowercase, which is the canonical form this package uses.
    """
    return "0x" + topic[-40:].lower()


def _decode_uint(raw: str | None) -> int | None:
    """Decode a single `uint256` return value."""
    if not raw or raw == "0x":
        return None
    try:
        return int(raw, 16)
    except ValueError:
        return None


def _has_code(raw: str | None) -> bool:
    """Whether an `eth_getCode` result describes a deployed contract.

    An address with no code answers `0x`, which is also what a node returns for
    a block before the contract was deployed. That second reading is the useful
    one here.
    """
    return bool(raw) and raw not in {"0x", "0x0"}


def _sanitize(text: str) -> str | None:
    """Make a contract-supplied string safe to print and to put in a table.

    A token's `symbol()` and `name()` are whatever its deployer chose to write.
    They travel unaltered into a terminal, a CSV and a LaTeX table, so they are
    treated as untrusted input rather than as labels:

    * Control characters are dropped. A newline breaks table alignment -- one
      registry asset really does return a symbol containing whitespace that
      wrecked the rendered output -- and an ANSI escape sequence inside a
      `name()` would be acted on by the terminal it is printed to.
    * Runs of whitespace collapse to one space, and the result is trimmed.
    * An over-long value is truncated. A symbol is a ticker; a contract
      returning a paragraph is not describing one, and a very long string is a
      cheap way to disrupt any table it lands in.
    """
    kept = "".join(character for character in text if character.isprintable())
    collapsed = " ".join(kept.split())
    if not collapsed:
        return None
    if len(collapsed) > _MAX_LABEL_LENGTH:
        return collapsed[:_MAX_LABEL_LENGTH].rstrip() + "..."
    return collapsed


def _decode_string(raw: str | None) -> str | None:
    """Decode a returned string, handling both ABI encodings in the wild.

    A conformant token returns a dynamic `string`: an offset word, a length word,
    then the bytes. Several older tokens return a fixed `bytes32` instead, with
    the text left-aligned and null-padded. Both appear among real RWA tokens, so
    both are handled. The result is sanitized before it is returned; see
    `_sanitize` for why that is not paranoia.
    """
    if not raw or raw == "0x":
        return None
    body = bytes.fromhex(raw[2:])
    if len(body) < 2 * _WORD_HEX // 2:
        # Too short to carry offset and length words: treat as bytes32.
        return _sanitize(body.rstrip(NUL_BYTE).decode("utf-8", "replace"))
    length = int.from_bytes(body[32:64], "big")
    if 0 < length <= len(body) - 64:
        return _sanitize(body[64 : 64 + length].decode("utf-8", "replace"))
    return _sanitize(body[:32].rstrip(NUL_BYTE).decode("utf-8", "replace"))


class EvmRpcSource(Source):
    """On-chain transfers, holder balances and supply, straight from a node."""

    name = "evm_rpc"
    capabilities = frozenset(
        {Capability.ASSET_SNAPSHOT, Capability.TRANSFER_EVENT, Capability.HOLDER_BALANCE}
    )

    def __init__(  # noqa: PLR0913 -- keyword-only seams for cache, transport,
        # endpoint, issuer map, scan budget and ttl; none are positional.
        self,
        *,
        cache: ParquetCache | None = None,
        client: httpx.Client | None = None,
        rpc_url: str | None = None,
        issuer_addresses: Mapping[str, Collection[str]] | None = None,
        max_logs: int = DEFAULT_MAX_LOGS,
        max_log_requests: int = DEFAULT_MAX_LOG_REQUESTS,
        block_step: int | None = DEFAULT_BLOCK_STEP,
        min_interval: float = DEFAULT_MIN_INTERVAL,
        ttl: timedelta | None = None,
        head: int | None = None,
    ) -> None:
        """Create the adapter.

        Args:
            cache: Cache to read and write through.
            client: An httpx client; tests inject one with a mock transport.
            rpc_url: JSON-RPC endpoint. Defaults to `EVM_RPC_URL` in the
                environment, then to `DEFAULT_RPC_URL`.
            issuer_addresses: Per-asset treasury addresses whose transfers are
                primary rather than secondary, keyed by asset uid.
            max_logs: Refuse a scan that would exceed this many logs.
            max_log_requests: Refuse a scan that issues this many `eth_getLogs`
                calls. A backstop against a range that splits without ever
                converging, which is a bug rather than a large token.
            block_step: Blocks per log query. Defaults to `EVM_RPC_BLOCK_STEP`
                in the environment; if that is unset too, the adapter discovers
                what the endpoint accepts.
            min_interval: Minimum seconds between requests, to stay under a free
                endpoint's rate limit.
            ttl: Freshness window for cached responses. On-chain history is
                immutable, so a long or unbounded ttl is safe for old blocks.
            head: Pin every read to this block instead of the chain head, so a
                run can be repeated later and give the same numbers.
        """
        from rwa_liquidity.config import optional_setting  # noqa: PLC0415 -- avoids a cycle

        self._rpc_url = (rpc_url or optional_setting("EVM_RPC_URL") or DEFAULT_RPC_URL).rstrip("/")
        self._issuers = {uid.lower(): tuple(v) for uid, v in (issuer_addresses or {}).items()}
        # EVM_RPC_MAX_LOGS / EVM_RPC_MAX_LOG_REQUESTS raise the scan ceilings for
        # a token too active for the defaults (e.g. USDY), at the cost of a
        # longer first run.
        configured_logs = optional_setting("EVM_RPC_MAX_LOGS")
        configured_requests = optional_setting("EVM_RPC_MAX_LOG_REQUESTS")
        self._max_logs = int(configured_logs) if configured_logs else max_logs
        self._max_log_requests = (
            int(configured_requests) if configured_requests else max_log_requests
        )
        if block_step is None:
            # Fixing the step fixes the block ranges every scan asks for, and so
            # the cache keys its answers are stored under. A discovered step can
            # differ between runs -- an endpoint that answers an empty
            # whole-chain probe one day and throttles it the next -- and every
            # range then has to be fetched again.
            configured = optional_setting("EVM_RPC_BLOCK_STEP")
            block_step = int(configured) if configured is not None else None
        #: Blocks per log query, learned on first use and kept for the rest of
        #: the process: the cap is a property of the endpoint, so paying to
        #: discover it once per source rather than once per asset is the point.
        self._step = None if block_step is None else max(_MIN_BLOCK_STEP, block_step)
        self._min_interval = min_interval
        self._last_request = 0.0
        self._ttl = ttl
        self._http = CachedJSONClient(source=self.name, cache=cache, client=client)
        self._request_id = 0
        #: eth_getLogs calls issued by the scan in progress. Reset per scan
        #: rather than per instance: the CLI reuses one source across every
        #: asset in the registry, and a running total would fail the later ones
        #: for work the earlier ones did.
        self._requests = 0
        self._pinned_head = head
        self._reconciled: dict[str, bool | None] = {}
        self._dropped: dict[str, int] = {}

    # -- plumbing -----------------------------------------------------------

    def _rpc(
        self,
        method: str,
        params: Sequence[Any],
        *,
        dataset: str,
        cache_key: str,
        refresh: bool = False,
    ) -> Any:
        """Issue one JSON-RPC call and return its `result`.

        Raises:
            SourceFetchError: On transport failure or a JSON-RPC error object.
        """
        for attempt in range(_OVERLOAD_RETRIES + 1):
            self._request_id += 1
            # Pace only real calls: a cache hit costs the endpoint nothing, and
            # sleeping before one would make a cached run needlessly slow. Only a
            # call that reached the node starts the interval, so a run of cache
            # hits never waits.
            elapsed = time.monotonic() - self._last_request
            if self._last_request and elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            response = self._http.post_json(
                self._rpc_url,
                dataset=dataset,
                body={
                    "jsonrpc": "2.0",
                    "id": self._request_id,
                    "method": method,
                    "params": list(params),
                },
                cache_params={"call": cache_key},
                ttl=self._ttl,
                refresh=refresh,
                retries=_RETRIES,
            )
            if not response.from_cache:
                self._last_request = time.monotonic()
            payload = response.payload
            if not isinstance(payload, dict):
                raise SourceFetchError(f"{self.name}: {method} did not return a JSON object")
            if "error" not in payload:
                return payload.get("result")

            error = payload["error"]
            code = error.get("code") if isinstance(error, dict) else None
            if code != _JSONRPC_INTERNAL_ERROR:
                raise SourceFetchError(f"{self.name}: {method} failed: {error}")

            # JSON-RPC defines -32603 as a fault inside the server, so it is a
            # report about the node rather than a verdict on the request. This
            # endpoint uses it to throttle, over HTTP 200, which puts it out of
            # reach of the transport's own retry budget -- that only sees status
            # codes, and this arrives as a success. Backing off and asking again
            # is what a 429 would have got.
            if attempt < _OVERLOAD_RETRIES:
                logger.info(
                    "%s: %s is throttling; waiting %.0fs",
                    self.name,
                    self._rpc_url,
                    _OVERLOAD_PAUSE * (attempt + 1),
                )
                time.sleep(_OVERLOAD_PAUSE * (attempt + 1))
                continue

            # Out of patience. Raised as a transport failure so the log scanner
            # lets it out rather than splitting the range: a node that is
            # struggling answers two narrower queries no more happily than one,
            # and the scan would drive the overload it is reacting to.
            raise SourceTransportError(
                f"{self.name}: {self._rpc_url} is refusing {method} as "
                f"unavailable after {_OVERLOAD_RETRIES} attempts ({error}). A free "
                f"endpoint rate-limits sustained scanning, and a full-history walk "
                f"is sustained scanning; this usually clears after a pause. To scan "
                f"now, point EVM_RPC_URL at an endpoint with more headroom."
            )

        raise SourceTransportError(f"{self.name}: {method} exhausted its retries")

    def _call(
        self,
        address: str,
        selector: str,
        *,
        block: int | None = None,
        refresh: bool = False,
        fallback: bool = True,
    ) -> str | None:
        """Read contract state, optionally at a specific block.

        Pinning matters for `totalSupply()`: the reconstruction check compares a
        ledger replayed up to some block against the supply, and reading the
        supply at "latest" instead would let activity between the two show up as
        a reconstruction failure. Not every public endpoint serves historical
        calls, so a rejection falls back to "latest" and the check becomes
        approximate rather than unavailable.
        """
        target = "latest" if block is None else hex(block)
        try:
            result = self._rpc(
                "eth_call",
                [{"to": address, "data": selector}, target],
                dataset="eth_call",
                cache_key=f"{address}:{selector}:{target}",
                refresh=refresh,
            )
        except SourceFetchError:
            if block is None or not fallback:
                raise
            logger.info(
                "%s: %s does not serve historical eth_call; reading %s at latest instead",
                self.name,
                self._rpc_url,
                selector,
            )
            return self._call(address, selector, refresh=refresh)
        return result if isinstance(result, str) else None

    def head_block(self, *, refresh: bool = True) -> int:
        """Return the block every read is taken at: the pinned one, or the chain head.

        Defaults to refreshing: a cached head block would silently pin every
        subsequent scan to an old tip. A pin set at construction is deliberate
        and is returned as is.
        """
        if self._pinned_head is not None:
            return self._pinned_head
        result = self._rpc("eth_blockNumber", [], dataset="head", cache_key="head", refresh=refresh)
        block = _decode_uint(result if isinstance(result, str) else None)
        if block is None:
            raise SourceFetchError(f"{self.name}: eth_blockNumber returned {result!r}")
        return block

    def block_range(
        self, start: datetime, end: datetime, *, refresh: bool = False
    ) -> tuple[int, int]:
        """Return the first and last block whose timestamps fall in `[start, end)`.

        Recorded next to an exported window so the figure can be recomputed
        later from exactly the same blocks. The last block is capped at the read
        block, since nothing after it was observed.

        Args:
            start: Inclusive start of the window.
            end: Exclusive end of the window.
            refresh: Bypass the cache.

        Returns:
            `(first_block, last_block)`.
        """
        head = self.head_block()
        first = self._first_block_at_or_after(start, head, refresh=refresh)
        after = self._first_block_at_or_after(end, head, refresh=refresh)
        return first, after - 1

    def _first_block_at_or_after(self, moment: datetime, head: int, *, refresh: bool) -> int:
        """Return the lowest block stamped at or after `moment`, or `head + 1` if none is.

        A binary search over block timestamps, which never decrease along the
        chain. Each probe is one cached `eth_getBlockByNumber`.
        """
        low, high = 0, head + 1
        while low < high:
            middle = (low + high) // 2
            if self._block_times([middle], refresh=refresh)[middle] >= moment:
                high = middle
            else:
                low = middle + 1
        return low

    def _deployment_block(self, address: str, head: int, *, refresh: bool) -> int:
        """Return the first block at which `address` holds code.

        A token cannot have emitted a log before it existed, so everything below
        this block is provably empty. Finding it costs about 25 `eth_getCode`
        calls and saves thousands: these tokens were deployed well past block 18
        million, so a walk from genesis spends most of its requests proving that
        the early chain has nothing to say about a contract that did not exist.

        Falls back to genesis if the endpoint cannot answer historically. That is
        slow rather than wrong, which is the right way round -- and a node that
        wrongly reported a contract as absent would be caught downstream, where
        the reconstructed ledger is checked against the token's own supply.
        """
        try:
            if _has_code(self._code(address, 0, refresh=refresh)):
                return 0
            if not _has_code(self._code(address, head, refresh=refresh)):
                # No code even at the tip: nothing to scan, and a range starting
                # at the head is the cheapest way to say so.
                return head
        except SourceFetchError:
            logger.info(
                "%s: %s does not serve historical eth_getCode; scanning from genesis",
                self.name,
                self._rpc_url,
            )
            return 0

        low, high = 0, head
        while low < high:
            middle = (low + high) // 2
            if _has_code(self._code(address, middle, refresh=refresh)):
                high = middle
            else:
                low = middle + 1
        return low

    def _code(self, address: str, block: int, *, refresh: bool) -> str | None:
        result = self._rpc(
            "eth_getCode",
            [address, hex(block)],
            dataset="eth_getCode",
            cache_key=f"{address}:{block}",
            refresh=refresh,
        )
        return result if isinstance(result, str) else None

    def _discover_step(self, head: int, *, refresh: bool) -> int:
        """Find the widest block span this endpoint will answer in one query.

        Asks for the whole chain and halves until the answer stops being a
        rejection. An endpoint with no span cap settles on the first request, so
        nothing is paid for the generous case; a capped one costs about a dozen.

        Probes against the zero address, which owns no token and so has no
        Transfer logs anywhere. That leaves the span as the only thing the node
        can object to. Probing against the token being scanned would conflate the
        endpoint's span limit with that token's log density -- and the densest
        stretch of a fund's history is usually issuance, right where the probe
        would start -- so the limit would come back understated and stay that way
        for every asset this source goes on to scan.

        Deliberately optimistic to begin with. Starting from a conservative guess
        would be safe but permanently slow against an endpoint that would have
        served the lot, and there is no way to grow a guess that is too small: an
        accepted query looks the same whether or not a wider one would also have
        worked.
        """

        def probe(span: int) -> str | None:
            """Return `None` if the span was served, or the refusal otherwise."""
            self._requests += 1
            try:
                self._rpc(
                    "eth_getLogs",
                    [
                        {
                            "address": ZERO_ADDRESS,
                            "topics": [TRANSFER_TOPIC],
                            "fromBlock": hex(max(0, head - span + 1)),
                            "toBlock": hex(head),
                        }
                    ],
                    dataset="eth_getLogs",
                    cache_key=f"span-probe:{head}:{span}",
                    refresh=refresh,
                )
            except SourceTransportError:
                raise
            except SourceFetchError as error:
                return str(error)
            return None

        refusal = probe(head + 1)
        if refusal is None:
            return head + 1

        # Prefer the node's own answer to anything inferred. It states the limit
        # outright -- "range 24999999 exceeds limit of 10000" -- and reading it
        # is one request against roughly twenty, exact rather than approximate,
        # and immune to the failure that made the search unreliable: a refusal
        # for any *other* reason, a rate limit above all, is indistinguishable
        # from "too wide" and drags the estimate down. That is the same
        # conflation `SourceTransportError` exists to prevent, one level up.
        stated = _SPAN_LIMIT.search(refusal)
        if stated is not None:
            return max(_MIN_BLOCK_STEP, int(stated.group(1)))

        # No stated limit, so fall back to bisecting for it. Approximate, and
        # understates the limit if a probe is refused for an unrelated reason,
        # but a scan that asks for less than it could is merely slow.
        step = (head + 1) // 2
        while step > _MIN_BLOCK_STEP and probe(step) is not None:
            step //= 2
        return max(_MIN_BLOCK_STEP, step)

    def _scan_logs(self, address: str, head: int, *, refresh: bool) -> list[dict[str, Any]]:
        """Walk the complete Transfer history of `address` up to `head`.

        The single entry point for a full scan, so the per-scan request budget
        has exactly one place to be reset, and the five methods that need history
        cannot drift apart in how they ask for it.

        The walk steps through fixed, aligned windows rather than recursively
        halving the whole chain. Public endpoints cap a log query by **block
        span** -- 10,000 blocks on the default endpoint -- and not by how many
        results it would return, so the request count of a top-down split is set
        by the length of the chain rather than by how active the token is. That
        is why a token with twelve logs in its entire history cost as many
        requests as a busy one. Stepping in windows removes the split's interior
        nodes, and aligning them to a multiple of the step keeps the cache keys
        stable from run to run: only the final, partial window moves when the
        chain advances.
        """
        self._requests = 0
        collected: list[dict[str, Any]] = []
        low = self._deployment_block(address, head, refresh=refresh)
        if self._step is None:
            self._step = self._discover_step(head, refresh=refresh)
        low -= low % self._step
        step = self._step
        while low <= head:
            self._logs(
                address, low, min(low + step - 1, head), collected=collected, refresh=refresh
            )
            low += step
        return collected

    def _logs(
        self,
        address: str,
        low: int,
        high: int,
        *,
        collected: list[dict[str, Any]],
        refresh: bool,
    ) -> None:
        """Fetch Transfer logs for `[low, high]`, splitting when the node balks.

        `_scan_logs` sizes its windows to the span the endpoint accepts; this
        handles the other reason a query is refused, which is that the span it
        accepts holds more logs than it will return at once. Halving resolves
        that locally, for the dense stretch that caused it.

        Only a rejection splits. A request that never reached the node -- DNS
        failure, dropped connection, a 5xx that outlived its retries -- carries
        no verdict on the block span, and halving on one of those converts a
        network outage into an exponential fan-out where every branch fails the
        same way and splits again. `SourceTransportError` is what distinguishes
        the two.
        """
        self._requests += 1
        if self._requests > self._max_log_requests:
            raise SourceFetchError(
                f"{self.name}: the scan issued {self._requests:,} eth_getLogs calls, past "
                f"the {self._max_log_requests:,} limit. A legitimate scan resolves in far "
                f"fewer; this many means the range is being split for a reason other than "
                f"the node capping results."
            )
        self._check_budget(collected)
        try:
            result = self._rpc(
                "eth_getLogs",
                [
                    {
                        "address": address,
                        "topics": [TRANSFER_TOPIC],
                        "fromBlock": hex(low),
                        "toBlock": hex(high),
                    }
                ],
                dataset="eth_getLogs",
                cache_key=f"{address}:{low}:{high}",
                refresh=refresh,
            )
        except SourceTransportError:
            # The node never judged this request, so there is nothing to learn
            # from it about the span. Let it out: the pipeline records a failure
            # for this asset, and the cache means a re-run resumes rather than
            # restarts.
            raise
        except SourceFetchError:
            if low >= high:
                raise
            # Split this window and only this window. A rejection here is
            # usually about how many logs the span contains rather than how wide
            # it is, and log density is a property of one stretch of one token's
            # history: BUIDL's mints crowd a few million blocks, while other
            # tokens in the registry are nearly empty there. Narrowing the shared
            # step instead would let one dense region slow every later window and
            # every later asset. The step tracks the endpoint's span limit, which
            # `_discover_step` learns.
            middle = (low + high) // 2
            self._logs(address, low, middle, collected=collected, refresh=refresh)
            self._logs(address, middle + 1, high, collected=collected, refresh=refresh)
            return

        if not isinstance(result, list):
            raise SourceFetchError(f"{self.name}: eth_getLogs returned {type(result).__name__}")
        collected.extend(log for log in result if isinstance(log, dict))
        # Checked again after extending, not only before the request: a node that
        # answers the whole range in one call would otherwise blow the budget
        # without it ever being consulted.
        self._check_budget(collected)

    def _check_budget(self, collected: Sequence[Any]) -> None:
        if len(collected) > self._max_logs:
            raise SourceFetchError(
                f"{self.name}: the scan reached {len(collected):,} logs, past the "
                f"{self._max_logs:,} limit. This token is too active for full-history "
                f"reconstruction against a public endpoint; raise max_logs, or use a "
                f"source that provides holder balances directly."
            )

    def _block_times(self, blocks: Iterable[int], *, refresh: bool) -> dict[int, datetime]:
        """Look up timestamps for blocks whose logs did not carry one.

        Most endpoints now include `blockTimestamp` on each log, which avoids a
        request per block. This is the fallback for those that do not, and it is
        deliberately per-block and cached: an incorrect timestamp would move a
        transfer into or out of its observation window.
        """
        times: dict[int, datetime] = {}
        for number in sorted(set(blocks)):
            result = self._rpc(
                "eth_getBlockByNumber",
                [hex(number), False],
                dataset="eth_getBlockByNumber",
                cache_key=f"block:{number}",
                refresh=refresh,
            )
            stamp = _decode_uint(result.get("timestamp") if isinstance(result, dict) else None)
            if stamp is None:
                raise SourceFetchError(f"{self.name}: block {number} has no timestamp")
            times[number] = datetime.fromtimestamp(stamp, tz=UTC)
        return times

    # -- decoding -----------------------------------------------------------

    def _decode_logs(
        self, logs: Sequence[Mapping[str, Any]], *, refresh: bool
    ) -> list[dict[str, Any]]:
        """Decode raw logs into records, resolving each one's block time.

        Amounts stay Python integers here rather than going straight into a
        frame. A real token emits values that do not fit any fixed-width integer
        type -- CACHE Gold has a `Transfer` of 1.1e40 raw units against a supply
        of 100,771 -- and building a frame from those raises rather than
        producing a wrong number. Python's unbounded integers carry them through
        to the plausibility check below, which is where such a value belongs.
        """
        records: list[dict[str, Any]] = []
        missing_times: list[int] = []
        skipped_non_erc20 = 0

        for log in logs:
            topics = log.get("topics")
            if not isinstance(topics, list) or len(topics) != _ERC20_TOPIC_COUNT:
                skipped_non_erc20 += 1
                continue
            block = _decode_uint(log.get("blockNumber"))
            index = _decode_uint(log.get("logIndex"))
            value = _decode_uint(log.get("data")) or 0
            tx_hash = log.get("transactionHash")
            if block is None or index is None or not isinstance(tx_hash, str):
                skipped_non_erc20 += 1
                continue

            stamp = _decode_uint(log.get("blockTimestamp"))
            if stamp is None:
                missing_times.append(block)
            records.append(
                {
                    "block": block,
                    "log_index": index,
                    "tx_hash": tx_hash,
                    "from_address": _to_address(topics[1]),
                    "to_address": _to_address(topics[2]),
                    "raw_amount": value,
                    "block_time": datetime.fromtimestamp(stamp, tz=UTC) if stamp else None,
                }
            )

        if skipped_non_erc20:
            logger.warning(
                "%s skipped %d log(s) that are not two-parameter ERC-20 transfers "
                "(most likely ERC-721, which reuses the same event signature)",
                self.name,
                skipped_non_erc20,
            )

        if missing_times:
            resolved = self._block_times(missing_times, refresh=refresh)
            for record in records:
                if record["block_time"] is None:
                    record["block_time"] = resolved[record["block"]]

        records.sort(key=lambda record: (record["block"], record["log_index"]))
        return records

    def _drop_implausible(
        self, records: list[dict[str, Any]], asset: AssetRef
    ) -> list[dict[str, Any]]:
        """Remove transfers that move more than the supply in existence.

        This is an invariant of ERC-20 rather than a threshold: outside of a
        mint, a transfer cannot move more tokens than exist at that moment. The
        running supply is tracked chronologically through the same log stream, so
        the bound is exact at every point rather than compared against today's
        figure -- a fund that has since shrunk legitimately has historical
        transfers larger than its current supply, and those must not be touched.

        Contracts do emit logs that violate this. CACHE Gold has three, the
        largest 1.1e40 raw units against a supply of about 1e13. Left in, a
        single such value would dominate every volume metric and make the output
        meaningless; the sum is not robust to one absurd term.
        """
        kept: list[dict[str, Any]] = []
        supply = 0
        dropped: list[int] = []
        for record in records:
            amount = int(record["raw_amount"])
            minting = record["from_address"] in BURN_ADDRESSES
            if minting:
                supply += amount
            elif amount > supply:
                dropped.append(amount)
                continue
            elif record["to_address"] in BURN_ADDRESSES:
                supply -= amount
            kept.append(record)

        self._dropped[asset.uid] = len(dropped)
        if dropped:
            logger.warning(
                "%s dropped %d transfer(s) for %s that move more than the supply in "
                "existence, the largest %.3g raw units. A transfer cannot move tokens "
                "that do not exist, so these are contract artifacts rather than "
                "activity; one such value left in the sum would dominate every volume "
                "metric.",
                self.name,
                len(dropped),
                asset.uid,
                max(dropped),
            )
        return kept

    @staticmethod
    def _to_frame(records: Sequence[Mapping[str, Any]], *, scale: int) -> pl.DataFrame:
        """Build a frame of human-scaled amounts from decoded records."""
        return pl.DataFrame(
            {
                "block_time": [record["block_time"] for record in records],
                "tx_hash": [record["tx_hash"] for record in records],
                "log_index": [record["log_index"] for record in records],
                "from_address": [record["from_address"] for record in records],
                "to_address": [record["to_address"] for record in records],
                # Scaled to a float here, after the plausibility check has run on
                # the exact integers.
                "amount": [int(record["raw_amount"]) / scale for record in records],
            },
            schema={
                "block_time": pl.Datetime("us", "UTC"),
                "tx_hash": pl.String(),
                "log_index": pl.Int64(),
                "from_address": pl.String(),
                "to_address": pl.String(),
                "amount": pl.Float64(),
            },
        )

    def _token_facts(
        self, asset: AssetRef, *, refresh: bool, block: int | None = None
    ) -> dict[str, Any]:
        """Read decimals, supply, symbol and name straight off the contract."""
        decimals = _decode_uint(self._call(asset.address, _SELECTOR_DECIMALS, refresh=refresh))
        if decimals is None:
            raise SourceFetchError(
                f"{self.name}: {asset.uid} did not answer decimals(); it may not be an "
                f"ERC-20 contract"
            )
        # Whether the supply was read at the block the ledger is replayed to. When
        # the endpoint will not serve that block, the figure falls back to
        # "latest", and a reconciliation against it would compare two different
        # moments; the check is then reported as not run rather than as failed.
        supply_at_block = True
        try:
            raw_supply = _decode_uint(
                self._call(
                    asset.address,
                    _SELECTOR_TOTAL_SUPPLY,
                    block=block,
                    refresh=refresh,
                    fallback=False,
                )
            )
        except SourceFetchError:
            if block is None:
                raise
            supply_at_block = False
            raw_supply = _decode_uint(
                self._call(asset.address, _SELECTOR_TOTAL_SUPPLY, refresh=refresh)
            )
        return {
            "decimals": decimals,
            "raw_total_supply": raw_supply,
            "supply_at_block": supply_at_block,
            "symbol": _decode_string(self._call(asset.address, _SELECTOR_SYMBOL, refresh=refresh)),
            "name": _decode_string(self._call(asset.address, _SELECTOR_NAME, refresh=refresh)),
        }

    # -- public interface ---------------------------------------------------

    def fetch_asset_snapshots(
        self,
        assets: Sequence[AssetRef],
        *,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return supply, decimals, symbol and name read from each contract.

        These figures are exact rather than reported: they come from the token's
        own state, not from a provider's index of it. No price is included --
        a node does not know prices, and inventing one is not this adapter's job.
        """
        schema = dict(polars_schema(AssetSnapshot))
        rows: list[dict[str, Any]] = []
        head = self.head_block()
        described = self._read_time()
        for asset in dict.fromkeys(assets):
            facts = self._token_facts(asset, refresh=refresh, block=head)
            scale = 10 ** facts["decimals"]
            raw_supply = facts["raw_total_supply"]
            rows.append(
                {
                    "asset_uid": asset.uid,
                    "source": self.name,
                    "retrieved_at": datetime.now(UTC),
                    # The supply is read at the read block, so the snapshot
                    # describes that block: now, unless a block was pinned.
                    "as_of": described,
                    "symbol": facts["symbol"],
                    "name": facts["name"],
                    "decimals": facts["decimals"],
                    "total_supply": None if raw_supply is None else raw_supply / scale,
                    "market_value_usd": None,
                    "price_usd": None,
                    "holder_count": None,
                }
            )
        frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
        return validate(AssetSnapshot, frame, origin=self.name)

    def fetch_transfers(
        self,
        asset: AssetRef,
        *,
        start: datetime,
        end: datetime,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return classified transfers for `asset` over `[start, end)`.

        The scan runs over blocks and the window is applied to the resulting
        timestamps, because block numbers and wall-clock time are only loosely
        related and guessing a block from a date would silently clip the window.
        """
        head = self.head_block()
        facts = self._token_facts(asset, refresh=refresh, block=head)
        scale = 10 ** facts["decimals"]

        collected = self._scan_logs(asset.address, head, refresh=refresh)
        records = self._drop_implausible(self._decode_logs(collected, refresh=refresh), asset)

        schema = dict(polars_schema(TransferEvent))
        if not records:
            return validate(TransferEvent, pl.DataFrame(schema=schema), origin=self.name)

        decoded = self._to_frame(records, scale=scale)
        windowed = decoded.filter((pl.col("block_time") >= start) & (pl.col("block_time") < end))
        classified = classify_transfers(
            windowed,
            issuer_addresses=self._issuers.get(asset.uid.lower(), ()),
            asset_uid=asset.uid,
        )
        frame = classified.with_columns(
            pl.lit(asset.uid).alias("asset_uid"),
            pl.lit(self.name).alias("source"),
            pl.lit(datetime.now(UTC)).alias("retrieved_at").cast(pl.Datetime("us", "UTC")),
            pl.lit(None, dtype=pl.Float64).alias("amount_usd"),
        ).select(list(schema))
        return validate(TransferEvent, frame, origin=self.name)

    def _read_time(self) -> datetime:
        """Return the moment the read block describes: the pinned block's time, or now."""
        if self._pinned_head is None:
            return datetime.now(UTC)
        return self._block_times([self._pinned_head], refresh=False)[self._pinned_head]

    def _history(
        self, asset: AssetRef, *, refresh: bool
    ) -> tuple[dict[str, Any], int, list[dict[str, Any]]]:
        """Return the token's facts at the read block, its scale, and its cleaned history.

        The one walk every ledger-derived method shares, so they cannot disagree
        about which block they read or which transfers they kept.
        """
        head = self.head_block()
        facts = self._token_facts(asset, refresh=refresh, block=head)
        collected = self._scan_logs(asset.address, head, refresh=refresh)
        records = self._drop_implausible(self._decode_logs(collected, refresh=refresh), asset)
        return facts, 10 ** facts["decimals"], records

    @staticmethod
    def _balances_before(
        records: Sequence[Mapping[str, Any]], instants: Sequence[datetime]
    ) -> dict[datetime, dict[str, int]]:
        """Replay the ledger and capture every positive balance just before each instant.

        An observation window is half-open, `[start, end)`, so a transfer stamped
        exactly at `end` belongs to the next window. The balance at `end` must
        therefore leave it out, or the holder distribution and the window's
        transfers would describe different moments.
        """
        wanted = sorted(set(instants))
        ledger: dict[str, int] = defaultdict(int)
        captured: dict[datetime, dict[str, int]] = {}
        position = 0

        def capture() -> dict[str, int]:
            return {
                address: value
                for address, value in ledger.items()
                if value > _DUST and address not in BURN_ADDRESSES
            }

        for record in records:
            moment = record["block_time"]
            while position < len(wanted) and wanted[position] <= moment:
                captured[wanted[position]] = capture()
                position += 1
            amount = int(record["raw_amount"])
            ledger[str(record["from_address"])] -= amount
            ledger[str(record["to_address"])] += amount
        for remaining in wanted[position:]:
            captured[remaining] = capture()
        return captured

    def _reconcile(
        self,
        asset: AssetRef,
        records: Sequence[Mapping[str, Any]],
        facts: Mapping[str, Any],
    ) -> bool | None:
        """Check the full replayed ledger against the contract's supply, and record it.

        `True` means the positive balances sum exactly to `totalSupply()` at the
        read block and no address went negative. `False` means either condition
        failed: transfers are missing, or balances change by some mechanism other
        than `Transfer` events, and every holder metric derived from them is
        unreliable. `None` means the supply could not be read at the block the
        ledger was replayed to, so the two could not be compared.
        """
        ledger: dict[str, int] = defaultdict(int)
        for record in records:
            amount = int(record["raw_amount"])
            ledger[str(record["from_address"])] -= amount
            ledger[str(record["to_address"])] += amount
        for burn_address in BURN_ADDRESSES:
            ledger.pop(burn_address, None)
        holders = {address: value for address, value in ledger.items() if value > _DUST}
        below_zero = [address for address, value in ledger.items() if value < 0]

        status: bool | None
        if below_zero:
            logger.warning(
                "%s: %d address(es) ended with a negative balance for %s. The transfer "
                "history is incomplete, so the holder distribution is unreliable.",
                self.name,
                len(below_zero),
                asset.uid,
            )
            status = False
        else:
            status = True

        reported = facts.get("raw_total_supply")
        if reported is None or not facts.get("supply_at_block", True):
            logger.warning(
                "%s: the total supply of %s could not be read at the block the ledger "
                "was replayed to, so the reconstruction could not be verified",
                self.name,
                asset.uid,
            )
            if status:
                status = None
        else:
            reconstructed = sum(holders.values())
            if reconstructed != reported:
                scale = 10 ** facts["decimals"]
                logger.warning(
                    "%s: reconstructed balances for %s sum to %s but totalSupply() "
                    "reports %s (difference %s raw units). Balances change by some "
                    "mechanism other than Transfer events -- most often rebasing -- so "
                    "the holder distribution and every concentration metric derived "
                    "from it are unreliable for this token.",
                    self.name,
                    asset.uid,
                    f"{reconstructed / scale:,.6f}",
                    f"{reported / scale:,.6f}",
                    reconstructed - reported,
                )
                status = False
            elif status:
                logger.info(
                    "%s: holder reconstruction for %s matches totalSupply() exactly "
                    "across %d holders",
                    self.name,
                    asset.uid,
                    len(holders),
                )

        self._reconciled[asset.uid] = status
        return status

    def reconciliation_status(self) -> Mapping[str, bool | None]:
        """Return the reconciliation outcome of every asset reconstructed so far."""
        return dict(self._reconciled)

    def dropped_transfer_counts(self) -> Mapping[str, int]:
        """Return how many transfers were dropped as impossible, per asset scanned."""
        return dict(self._dropped)

    def _holder_frame(
        self, asset: AssetRef, balances: Mapping[datetime, Mapping[str, int]], *, scale: int
    ) -> pl.DataFrame:
        """Build a validated `HolderBalance` frame from balances keyed by instant."""
        retrieved = datetime.now(UTC)
        rows = [
            {
                "asset_uid": asset.uid,
                "source": self.name,
                "retrieved_at": retrieved,
                "as_of": moment,
                "address": address,
                "balance": value / scale,
                "balance_usd": None,
            }
            for moment in sorted(balances)
            for address, value in balances[moment].items()
        ]
        schema = dict(polars_schema(HolderBalance))
        frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
        return validate(HolderBalance, frame, origin=self.name)

    def fetch_holders(
        self,
        asset: AssetRef,
        *,
        as_of: datetime | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return holder balances reconstructed from the full transfer history.

        Every `Transfer` since deployment is replayed as a ledger. The full
        ledger is checked against `totalSupply()` (see `reconciliation_status`),
        and the balances returned are those standing just before `as_of`.

        Args:
            asset: The asset to reconstruct balances for.
            as_of: The instant to report balances at. Transfers stamped at or
                after it are not applied, matching a half-open observation
                window that ends there. `None` reports the balances at the
                block every read is taken at.
            refresh: Bypass the cache and rescan.

        Returns:
            A validated `HolderBalance` frame, one row per address with a
            positive balance.
        """
        facts, scale, records = self._history(asset, refresh=refresh)
        self._reconcile(asset, records, facts)
        if as_of is None:
            # Every transfer up to and including the read block, labelled with
            # the moment that block describes.
            full = self._balances_before(records, [_END_OF_TIME])[_END_OF_TIME]
            balances = {self._read_time(): full}
        else:
            balances = self._balances_before(records, [as_of])
        return self._holder_frame(asset, balances, scale=scale)

    def holder_snapshots(
        self,
        asset: AssetRef,
        instants: Sequence[datetime],
        *,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return `HolderBalance` rows giving the distribution at each instant.

        Concentration and dormancy for a past window need the holders as they
        stood then. Using the present distribution is not an approximation but an
        error: balances that sum to more than the supply of an earlier window
        produce a share above 1, which the metrics correctly refuse, leaving the
        series full of holes.

        Replaying the ledger to each instant removes the problem rather than
        working around it. One walk serves every instant, and the full ledger is
        reconciled against `totalSupply()` on the way (see
        `reconciliation_status`).

        Args:
            asset: The asset to reconstruct distributions for.
            instants: Moments to snapshot, each exclusive of transfers stamped at
                or after it. Order does not matter.
            refresh: Bypass the cache and rescan.

        Returns:
            A validated `HolderBalance` frame, one row per address with a positive
            balance at each instant.
        """
        facts, scale, records = self._history(asset, refresh=refresh)
        self._reconcile(asset, records, facts)
        return self._holder_frame(asset, self._balances_before(records, instants), scale=scale)

    def supply_snapshots(
        self,
        asset: AssetRef,
        instants: Sequence[datetime],
        *,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return `AssetSnapshot` rows giving supply at each of `instants`.

        Supply is accumulated from the transfer ledger -- mints less burns, in
        block order -- so each figure is the supply as it actually stood at that
        moment rather than today's applied backwards. That distinction matters:
        BUIDL has minted roughly ten times its current supply over its life, so a
        turnover ratio for a window a year ago divided by today's supply would be
        wrong by that factor.

        The walk is the same one `fetch_holders` uses and shares its cache, and
        the full ledger is reconciled against `totalSupply()` on the way past.

        Args:
            asset: The asset to build a supply history for.
            instants: Moments to report supply at, each exclusive of transfers
                stamped at or after it. Order does not matter.
            refresh: Bypass the cache and rescan.

        Returns:
            A validated `AssetSnapshot` frame, one row per instant, carrying only
            the fields a ledger can justify: supply, decimals, symbol and name.
        """
        facts, scale, records = self._history(asset, refresh=refresh)
        self._reconcile(asset, records, facts)

        # One pass over the ledger, emitting the running supply as each requested
        # instant is reached. Records are already in block order.
        wanted = sorted(set(instants))
        supply = 0
        at_instant: dict[datetime, int] = {}
        position = 0
        for record in records:
            moment = record["block_time"]
            while position < len(wanted) and wanted[position] <= moment:
                at_instant[wanted[position]] = supply
                position += 1
            amount = int(record["raw_amount"])
            if record["from_address"] in BURN_ADDRESSES:
                supply += amount
            elif record["to_address"] in BURN_ADDRESSES:
                supply -= amount
        for remaining in wanted[position:]:
            at_instant[remaining] = supply

        schema = dict(polars_schema(AssetSnapshot))
        frame = pl.DataFrame(
            {
                "asset_uid": [asset.uid] * len(wanted),
                "source": [self.name] * len(wanted),
                "retrieved_at": [datetime.now(UTC)] * len(wanted),
                "as_of": list(wanted),
                "symbol": [facts["symbol"]] * len(wanted),
                "name": [facts["name"]] * len(wanted),
                "decimals": [facts["decimals"]] * len(wanted),
                "total_supply": [at_instant[moment] / scale for moment in wanted],
                "market_value_usd": [None] * len(wanted),
                "price_usd": [None] * len(wanted),
                "holder_count": [None] * len(wanted),
            },
            schema=schema,
        )
        return validate(AssetSnapshot, frame, origin=self.name)

    def describe_issuance(self, asset: AssetRef, *, refresh: bool = False) -> IssuanceProfile:
        """Summarise how `asset` issues, from its complete transfer history.

        Uses the same cached scan as `fetch_holders`, so calling both costs one
        history walk rather than two.

        Args:
            asset: The asset to profile.
            refresh: Bypass the cache and rescan.

        Returns:
            The profile. `caveat()` on the result says what it implies for the
            asset's secondary figures.
        """
        head = self.head_block()
        facts = self._token_facts(asset, refresh=refresh, block=head)
        scale = 10 ** facts["decimals"]

        collected = self._scan_logs(asset.address, head, refresh=refresh)
        records = self._drop_implausible(self._decode_logs(collected, refresh=refresh), asset)

        received: dict[str, int] = defaultdict(int)
        minted = 0
        mints = burns = 0
        first_mint: int | None = None
        for record in records:
            amount = int(record["raw_amount"])
            if record["from_address"] in BURN_ADDRESSES:
                mints += 1
                minted += amount
                received[str(record["to_address"])] += amount
                if first_mint is None:
                    first_mint = int(record["block"])
            elif record["to_address"] in BURN_ADDRESSES:
                burns += 1

        # With no visible issuance, fall back to who received the most supply
        # overall: on a token pre-minted in its constructor, that is whoever the
        # initial allocation went to.
        if not received:
            for record in records:
                received[str(record["to_address"])] += int(record["raw_amount"])

        largest = max(received.items(), key=lambda item: item[1], default=None)
        total = sum(received.values())
        return IssuanceProfile(
            asset_uid=asset.uid,
            transfers=len(records),
            mints=mints,
            burns=burns,
            minted_supply=minted / scale,
            first_mint_block=first_mint,
            largest_recipient=None if largest is None else largest[0],
            largest_recipient_share=(None if largest is None or total <= 0 else largest[1] / total),
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()
