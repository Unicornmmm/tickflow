"""Market depth (order book) resources for TickFlow API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from .._batch import batched_get_async, batched_get_sync
from ._base import AsyncResource, SyncResource

if TYPE_CHECKING:
    from ..generated_model import MarketDepth

DEFAULT_DEPTH_BATCH_SIZE = 100


class Depth(SyncResource):
    """Synchronous interface for market depth endpoint.

    Examples
    --------
    >>> client = TickFlow(api_key="your-key")
    >>> depth = client.depth.get("600000.SH")
    >>> print(depth["bid_prices"], depth["ask_prices"])
    """

    def get(self, symbol: str) -> "MarketDepth":
        """Get 5-level market depth for a single symbol.

        Parameters
        ----------
        symbol : str
            Symbol code (e.g. "600000.SH").

        Returns
        -------
        MarketDepth
            Market depth data with bid/ask prices and volumes.

        Examples
        --------
        >>> depth = client.depth.get("600000.SH")
        >>> for i in range(5):
        ...     print(f"Bid {i+1}: {depth['bid_prices'][i]} x {depth['bid_volumes'][i]}")
        ...     print(f"Ask {i+1}: {depth['ask_prices'][i]} x {depth['ask_volumes'][i]}")
        """
        response = self._client.get("/v1/depth", params={"symbol": symbol})
        return response["data"]

    def batch(
        self,
        symbols: List[str],
        *,
        batch_size: int = DEFAULT_DEPTH_BATCH_SIZE,
        max_workers: int = 5,
        show_progress: bool = False,
    ) -> Dict[str, "MarketDepth"]:
        """Get 5-level market depth for multiple symbols.

        Automatically splits large symbol lists into chunks and fetches
        them concurrently. Failed chunks don't affect other chunks.

        Parameters
        ----------
        symbols : list of str
            List of symbol codes (e.g. ["600000.SH", "000001.SZ"]).
        batch_size : int, optional
            Number of symbols per request. Default 100.
        max_workers : int, optional
            Maximum number of concurrent requests. Default 5.
        show_progress : bool, optional
            If True, display a progress bar (requires tqdm). Default False.

        Returns
        -------
        dict[str, MarketDepth]
            Mapping from symbol code to market depth data.

        Examples
        --------
        >>> depths = client.depth.batch(["600000.SH", "000001.SZ", "600519.SH"])
        >>> for sym, d in depths.items():
        ...     print(f"{sym}: bid1={d['bid_prices'][0]}, ask1={d['ask_prices'][0]}")
        """
        return batched_get_sync(
            self._client,
            "/v1/depth/batch",
            symbols,
            {},
            batch_size=batch_size,
            max_workers=max_workers,
            show_progress=show_progress,
            progress_desc="Fetching market depth",
        )


class AsyncDepth(AsyncResource):
    """Asynchronous interface for market depth endpoint.

    Examples
    --------
    >>> async with AsyncTickFlow(api_key="your-key") as client:
    ...     depth = await client.depth.get("600000.SH")
    """

    async def get(self, symbol: str) -> "MarketDepth":
        """Get 5-level market depth for a single symbol.

        Parameters
        ----------
        symbol : str
            Symbol code (e.g. "600000.SH").

        Returns
        -------
        MarketDepth
            Market depth data with bid/ask prices and volumes.
        """
        response = await self._client.get("/v1/depth", params={"symbol": symbol})
        return response["data"]

    async def batch(
        self,
        symbols: List[str],
        *,
        batch_size: int = DEFAULT_DEPTH_BATCH_SIZE,
        max_concurrency: int = 5,
        show_progress: bool = False,
    ) -> Dict[str, "MarketDepth"]:
        """Get 5-level market depth for multiple symbols.

        Automatically splits large symbol lists into chunks and fetches
        them concurrently. Failed chunks don't affect other chunks.

        Parameters
        ----------
        symbols : list of str
            List of symbol codes (e.g. ["600000.SH", "000001.SZ"]).
        batch_size : int, optional
            Number of symbols per request. Default 100.
        max_concurrency : int, optional
            Maximum number of concurrent requests. Default 5.
        show_progress : bool, optional
            If True, display a progress bar (requires tqdm). Default False.

        Returns
        -------
        dict[str, MarketDepth]
            Mapping from symbol code to market depth data.
        """
        return await batched_get_async(
            self._client,
            "/v1/depth/batch",
            symbols,
            {},
            batch_size=batch_size,
            max_concurrency=max_concurrency,
            show_progress=show_progress,
            progress_desc="Fetching market depth",
        )
