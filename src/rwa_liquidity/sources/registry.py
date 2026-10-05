"""The hand-maintained map from canonical asset keys to provider identifiers.

Some providers do not publish a usable link between the thing they report on and
the token contract that thing is. DeFiLlama is the clearest case: most of its RWA
protocols carry no contract address, and where one is present it frequently
identifies a governance token rather than the tokenized asset. Deriving the link
automatically would produce confident metrics for the wrong asset.

So the link is stated, in a data file, with a note on each entry recording what
is imprecise about it. The notes are part of the record: an entry whose protocol
slug covers more than the asset it is mapped to is still useful, provided nobody
later mistakes its figures for a direct measurement.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from rwa_liquidity.schema.asset import AssetRef

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = ["REGISTRY_ENV", "RegistryEntry", "RegistryError", "load_defillama_registry"]

_DATA_PACKAGE: Final = "rwa_liquidity.sources.data"
_DEFILLAMA_FILE: Final = "defillama.toml"

#: Environment variable naming an alternative registry: either a file shipped in
#: the data package (e.g. `research.toml`) or a path to a TOML file on disk.
REGISTRY_ENV: Final = "RWA_LIQUIDITY_REGISTRY"

#: Bumped when the file layout changes incompatibly.
_SUPPORTED_SCHEMA_VERSION: Final = 1


class RegistryError(Exception):
    """The registry file is missing, malformed, or of an unsupported version."""


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    """One asset's link to a DeFiLlama protocol.

    Attributes:
        ref: The canonical asset key.
        symbol: Ticker, as a human would write it. Note that DeFiLlama's own
            symbol casing is inconsistent, so this may differ from what the API
            returns; adapters report what the source says, not this.
        name: Full name of the asset.
        defillama_slug: The protocol slug on api.llama.fi.
        defillama_chain: The chain's display name as it appears in the
            protocol's `chainTvls`, which is capitalised (`Ethereum`) rather
            than the lowercase slug used elsewhere.
        notes: What is imprecise about this mapping. Empty when nothing is.
    """

    ref: AssetRef
    symbol: str
    name: str
    defillama_slug: str
    defillama_chain: str
    notes: str = ""


def _require(raw: Mapping[str, Any], field: str, index: int) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(
            f"{_DEFILLAMA_FILE}: asset #{index + 1} is missing a non-empty {field!r}"
        )
    return value


def _parse(document: Mapping[str, Any]) -> tuple[RegistryEntry, ...]:
    version = document.get("schema_version")
    if version != _SUPPORTED_SCHEMA_VERSION:
        raise RegistryError(
            f"{_DEFILLAMA_FILE}: schema_version is {version!r}, expected "
            f"{_SUPPORTED_SCHEMA_VERSION}"
        )

    assets = document.get("asset")
    if not isinstance(assets, list) or not assets:
        raise RegistryError(f"{_DEFILLAMA_FILE}: no [[asset]] entries found")

    entries: list[RegistryEntry] = []
    seen: set[str] = set()
    for index, raw in enumerate(assets):
        if not isinstance(raw, dict):
            raise RegistryError(f"{_DEFILLAMA_FILE}: asset #{index + 1} is not a table")
        uid = _require(raw, "uid", index)
        if uid in seen:
            # Two entries for one asset would make the resolved slug depend on
            # file order, which is exactly the kind of quiet ambiguity this file
            # exists to remove.
            raise RegistryError(f"{_DEFILLAMA_FILE}: duplicate entry for {uid!r}")
        seen.add(uid)
        entries.append(
            RegistryEntry(
                ref=AssetRef.parse(uid),
                symbol=_require(raw, "symbol", index),
                name=_require(raw, "name", index),
                defillama_slug=_require(raw, "defillama_slug", index),
                defillama_chain=_require(raw, "defillama_chain", index),
                notes=str(raw.get("notes", "")).strip(),
            )
        )
    return tuple(entries)


def load_defillama_registry(name: str | None = None) -> Sequence[RegistryEntry]:
    """Read and validate an asset registry.

    Args:
        name: Which registry to read. A path to a TOML file on disk, or the name
            of a file in the data package. When omitted, the environment
            variable `RWA_LIQUIDITY_REGISTRY` is used, and failing that the
            default `defillama.toml`.

    Returns:
        Every entry in file order.

    Raises:
        RegistryError: If the file is absent, unparseable, of the wrong schema
            version, or contains an incomplete or duplicated entry.
    """
    chosen = name or os.environ.get(REGISTRY_ENV, "").strip() or _DEFILLAMA_FILE
    on_disk = Path(chosen).expanduser()
    try:
        if on_disk.is_file():
            text = on_disk.read_text(encoding="utf-8")
        else:
            text = resources.files(_DATA_PACKAGE).joinpath(chosen).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as error:
        raise RegistryError(
            f"{chosen} is neither a file on disk nor present in the installed package"
        ) from error

    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise RegistryError(f"{chosen} is not valid TOML: {error}") from error

    return _parse(document)
