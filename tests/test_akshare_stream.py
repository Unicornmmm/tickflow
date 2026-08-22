import pytest

from tickflow.resources.akshare_stream import _normalise_symbol, _snapshot_to_quotes
from tickflow import AsyncTickFlow, TickFlow
from tickflow.resources.akshare_stream import (
    AkshareMarketStream,
    AsyncAkshareMarketStream,
)


class Snapshot:
    def to_dict(self, orient):
        assert orient == "records"
        return [
            {
                "代码": "600000",
                "名称": "浦发银行",
                "最新价": 10.1,
                "今开": 10.0,
                "最高": 10.2,
                "最低": 9.9,
                "昨收": 9.95,
                "成交量": 1234,
                "成交额": 5678.0,
                "涨跌额": 0.15,
                "涨跌幅": 1.51,
                "振幅": 3.02,
                "换手率": 0.4,
            },
            {"代码": "000001", "最新价": 11.0},
        ]


def test_normalise_symbol():
    assert _normalise_symbol("600000") == "600000.SH"
    assert _normalise_symbol("000001.SZ") == "000001.SZ"
    assert _normalise_symbol("920000") == "920000.BJ"


def test_snapshot_is_filtered_and_uses_tickflow_quote_shape():
    quotes = _snapshot_to_quotes(Snapshot(), {"600000.SH"})

    assert len(quotes) == 1
    quote = quotes[0]
    assert quote["symbol"] == "600000.SH"
    assert quote["region"] == "CN"
    assert quote["last_price"] == 10.1
    assert quote["volume"] == 1234
    assert quote["ext"]["name"] == "浦发银行"


def test_sync_client_selects_akshare_stream():
    client = TickFlow(stream_provider="akshare", stream_poll_interval=1)
    try:
        assert isinstance(client.stream, AkshareMarketStream)
    finally:
        client.close()


@pytest.mark.asyncio
async def test_async_client_selects_akshare_stream():
    client = AsyncTickFlow(stream_provider="akshare", stream_poll_interval=1)
    try:
        assert isinstance(client.stream, AsyncAkshareMarketStream)
    finally:
        await client.close()

