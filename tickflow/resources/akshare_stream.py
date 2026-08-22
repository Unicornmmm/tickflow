"""AKShare-backed quote streaming with the TickFlow stream interface.

AKShare exposes snapshots rather than a WebSocket.  These adapters poll the
all-A-share snapshot, select subscribed symbols, and emit TickFlow-compatible
quote dictionaries through the existing ``on_quotes`` callback.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set


QuoteHandler = Callable[[List[Dict[str, Any]]], None]


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(result) else result


def _symbol_suffix(code: str) -> str:
    if code.startswith(("4", "8", "9")):
        return "BJ"
    if code.startswith("6"):
        return "SH"
    return "SZ"


def _normalise_symbol(symbol: str) -> str:
    code = symbol.split(".", 1)[0].strip()
    return f"{code}.{_symbol_suffix(code)}"


def _snapshot_to_quotes(snapshot: Any, symbols: Set[str]) -> List[Dict[str, Any]]:
    wanted = {_normalise_symbol(symbol) for symbol in symbols}
    timestamp = int(time.time() * 1000)
    quotes: List[Dict[str, Any]] = []

    for row in snapshot.to_dict("records"):
        code = str(row.get("代码", "")).strip()
        symbol = _normalise_symbol(code)
        if symbol not in wanted:
            continue
        quotes.append(
            {
                "symbol": symbol,
                "region": "CN",
                "timestamp": timestamp,
                "last_price": _number(row.get("最新价")),
                "open": _number(row.get("今开")),
                "high": _number(row.get("最高")),
                "low": _number(row.get("最低")),
                "prev_close": _number(row.get("昨收")),
                "volume": int(_number(row.get("成交量"))),
                "amount": _number(row.get("成交额")),
                "ext": {
                    "name": row.get("名称"),
                    "change_amount": _number(row.get("涨跌额")),
                    "change_pct": _number(row.get("涨跌幅")),
                    "amplitude": _number(row.get("振幅")),
                    "turnover_rate": _number(row.get("换手率")),
                },
            }
        )
    return quotes


def _fetch_snapshot() -> Any:
    try:
        import akshare as ak
    except ImportError as exc:
        raise ImportError(
            'AKShare streaming requires: pip install "tickflow[akshare]"'
        ) from exc
    return ak.stock_zh_a_spot_em()


class AkshareMarketStream:
    """Synchronous AKShare polling stream with the TickFlow stream API."""

    def __init__(self, *, poll_interval: float = 3.0) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")
        self._poll_interval = poll_interval
        self._symbols: Set[str] = set()
        self._handler: Optional[QuoteHandler] = None
        self._error_handler: Optional[Callable[[str], None]] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def on_quotes(self, fn: QuoteHandler) -> Callable:
        self._handler = fn
        return fn

    def on_depth(self, fn: QuoteHandler) -> Callable:
        raise NotImplementedError("AKShare stream supports the quotes channel only")

    def on_error(self, fn: Callable[[str], None]) -> Callable:
        self._error_handler = fn
        return fn

    def subscribe(self, channel: str, symbols: List[str]) -> None:
        if channel != "quotes":
            raise NotImplementedError("AKShare stream supports the quotes channel only")
        self._symbols.update(_normalise_symbol(symbol) for symbol in symbols)

    def unsubscribe(self, channel: str, symbols: List[str]) -> None:
        if channel != "quotes":
            raise NotImplementedError("AKShare stream supports the quotes channel only")
        self._symbols -= {_normalise_symbol(symbol) for symbol in symbols}

    def connect(self, *, block: bool = True) -> None:
        self._stop.clear()
        if block:
            self._run()
        else:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def close(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                quotes = _snapshot_to_quotes(_fetch_snapshot(), set(self._symbols))
                if quotes and self._handler:
                    self._handler(quotes)
            except Exception as exc:
                if self._error_handler:
                    self._error_handler(str(exc))
            self._stop.wait(self._poll_interval)


class AsyncAkshareMarketStream:
    """Asynchronous AKShare polling stream with the TickFlow stream API."""

    def __init__(self, *, poll_interval: float = 3.0) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")
        self._poll_interval = poll_interval
        self._symbols: Set[str] = set()
        self._handler: Optional[QuoteHandler] = None
        self._error_handler: Optional[Callable[[str], None]] = None
        self._closed = False

    def on_quotes(self, fn: QuoteHandler) -> Callable:
        self._handler = fn
        return fn

    def on_depth(self, fn: QuoteHandler) -> Callable:
        raise NotImplementedError("AKShare stream supports the quotes channel only")

    def on_error(self, fn: Callable[[str], None]) -> Callable:
        self._error_handler = fn
        return fn

    async def subscribe(self, channel: str, symbols: List[str]) -> None:
        if channel != "quotes":
            raise NotImplementedError("AKShare stream supports the quotes channel only")
        self._symbols.update(_normalise_symbol(symbol) for symbol in symbols)

    async def unsubscribe(self, channel: str, symbols: List[str]) -> None:
        if channel != "quotes":
            raise NotImplementedError("AKShare stream supports the quotes channel only")
        self._symbols -= {_normalise_symbol(symbol) for symbol in symbols}

    async def connect(self) -> None:
        self._closed = False
        while not self._closed:
            try:
                snapshot = await asyncio.to_thread(_fetch_snapshot)
                quotes = _snapshot_to_quotes(snapshot, set(self._symbols))
                if quotes and self._handler:
                    self._handler(quotes)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._error_handler:
                    self._error_handler(str(exc))
            await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        self._closed = True

