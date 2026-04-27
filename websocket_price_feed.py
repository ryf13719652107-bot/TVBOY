# -*- coding: utf-8 -*-
"""
币安/OKX WebSocket 价格订阅模块
提供实时最新价和标记价格，替代 REST API 轮询
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

logger = logging.getLogger(__name__)


class BinanceWebSocketPriceFeed:
    """
    币安合约 WebSocket 价格订阅
    订阅：最新成交(aggTrade) + 标记价格(markPrice)
    """

    def __init__(
        self,
        symbols: list[str],
        on_price_update: Callable[[str, dict[str, Any]], None] | None = None,
        testnet: bool = False,
    ):
        """
        Args:
            symbols: 订阅的币种列表，如 ['BSBUSDT', 'BTCUSDT']
            on_price_update: 价格更新回调函数(symbol, price_data)
            testnet: 是否使用测试网
        """
        self.symbols = [s.upper() for s in symbols]
        self.on_price_update = on_price_update
        self.testnet = testnet

        # 价格数据存储
        self._prices: dict[str, dict[str, Any]] = {}
        self._prices_lock = threading.Lock()

        # WebSocket 连接
        self._ws = None
        self._connected = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # 统计信息
        self._last_update_time: dict[str, float] = {}
        self._message_count = 0
        self._reconnect_count = 0

    def _get_ws_url(self) -> str:
        """获取 WebSocket URL"""
        base_url = (
            "wss://stream.binancefuture.com"  # 测试网
            if self.testnet
            else "wss://fstream.binance.com"   # 主网
        )

        # 构建订阅流：aggTrade(最新成交) + markPrice(标记价格)
        streams = []
        for symbol in self.symbols:
            symbol_lower = symbol.lower().replace('/', '')  # 移除 /，如 BSB/USDT -> bsbusdt
            streams.append(f"{symbol_lower}@aggTrade")    # 最新成交价
            streams.append(f"{symbol_lower}@markPrice")   # 标记价格

        # 使用组合流格式 /stream?streams=stream1/stream2
        return f"{base_url}/stream?streams={'/'.join(streams)}"

    async def _connect_and_listen(self):
        """连接 WebSocket 并监听消息"""
        url = self._get_ws_url()
        logger.info(f"[WebSocket] 连接中... 订阅 {len(self.symbols)} 个币种")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                self._ws = ws
                self._connected = True
                logger.info(f"[WebSocket] 已连接，订阅流: {len(self.symbols) * 2} 个")

                async for message in ws:
                    if self._stop_event.is_set():
                        break
                    await self._handle_message(message)

        except ConnectionClosedOK:
            logger.info("[WebSocket] 连接正常关闭")
        except ConnectionClosed as e:
            logger.warning(f"[WebSocket] 连接断开: {e}")
        except Exception as e:
            logger.error(f"[WebSocket] 连接错误: {e}")
        finally:
            self._connected = False
            self._ws = None

    async def _handle_message(self, message: str):
        """处理收到的消息"""
        try:
            data = json.loads(message)
            stream = data.get("stream", "")
            payload = data.get("data", {})

            # 解析币种
            symbol = self._extract_symbol(stream)
            if not symbol:
                return

            # 解析价格数据
            price_data = self._parse_price_data(stream, payload)
            if not price_data:
                return

            # 更新价格存储
            with self._prices_lock:
                if symbol not in self._prices:
                    self._prices[symbol] = {}
                self._prices[symbol].update(price_data)
                self._prices[symbol]["received_at"] = time.time()
                self._last_update_time[symbol] = time.time()

            self._message_count += 1

            # 触发回调
            if self.on_price_update:
                try:
                    self.on_price_update(symbol, self._prices[symbol])
                except Exception as e:
                    logger.error(f"[WebSocket] 回调错误: {e}")

        except json.JSONDecodeError as e:
            logger.error(f"[WebSocket] JSON解析错误: {e}")
        except Exception as e:
            logger.error(f"[WebSocket] 消息处理错误: {e}")

    def _extract_symbol(self, stream: str) -> str | None:
        """从流名称提取币种"""
        # 格式: bsbusdt@aggTrade 或 bsbusdt@markPrice
        for symbol in self.symbols:
            if stream.lower().startswith(symbol.lower()):
                return symbol
        return None

    def _parse_price_data(self, stream: str, payload: dict) -> dict[str, Any] | None:
        """解析价格数据"""
        result: dict[str, Any] = {}

        try:
            if "@aggTrade" in stream:
                # 最新成交
                result["last"] = float(payload.get("p", 0))  # 成交价格
                result["trade_time"] = payload.get("T", 0)   # 成交时间

            elif "@markPrice" in stream:
                # 标记价格
                result["mark"] = float(payload.get("p", 0))   # 标记价格
                result["index"] = float(payload.get("i", 0))  # 指数价格
                result["funding_rate"] = float(payload.get("r", 0))  # 资金费率
                result["next_funding_time"] = payload.get("T", 0)

            return result if result else None

        except (ValueError, TypeError) as e:
            logger.error(f"[WebSocket] 价格解析错误: {e}")
            return None

    def _run_loop(self):
        """在线程中运行事件循环"""
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
            except Exception as e:
                logger.error(f"[WebSocket] 事件循环错误: {e}")

            if self._stop_event.is_set():
                break

            # 重连
            self._reconnect_count += 1
            wait_time = min(5 + self._reconnect_count * 2, 30)  # 指数退避，最大30秒
            logger.info(f"[WebSocket] {wait_time}秒后重连... (第{self._reconnect_count}次)")
            time.sleep(wait_time)

    def start(self):
        """启动 WebSocket 连接"""
        if self._thread and self._thread.is_alive():
            logger.warning("[WebSocket] 已经在运行")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="websocket-price-feed"
        )
        self._thread.start()
        logger.info("[WebSocket] 启动成功")

    def stop(self):
        """停止 WebSocket 连接"""
        self._stop_event.set()

        if self._ws:
            try:
                # 发送关闭帧
                asyncio.run(self._ws.close())
            except Exception as e:
                logger.error(f"[WebSocket] 关闭错误: {e}")

        if self._thread:
            self._thread.join(timeout=5)

        self._connected = False
        logger.info("[WebSocket] 已停止")

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """
        获取指定币种的最新价格数据

        Returns:
            {
                "last": float,      # 最新成交价
                "mark": float,      # 标记价格
                "received_at": float,  # 本地接收时间
                ...
            }
        """
        symbol = symbol.upper()
        with self._prices_lock:
            return self._prices.get(symbol, {}).copy() if symbol in self._prices else None

    def get_price_with_age(self, symbol: str, max_age_sec: float = 1.0) -> dict[str, Any] | None:
        """
        获取价格，如果数据太旧返回 None

        Args:
            symbol: 币种
            max_age_sec: 最大允许的数据年龄（秒）
        """
        data = self.get_price(symbol)
        if not data:
            return None

        received_at = data.get("received_at", 0)
        age = time.time() - received_at

        if age > max_age_sec:
            logger.warning(f"[WebSocket] {symbol} 数据过期: {age:.2f}s")
            return None

        return data

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息"""
        return {
            "connected": self._connected,
            "symbols": len(self.symbols),
            "message_count": self._message_count,
            "reconnect_count": self._reconnect_count,
            "prices_cached": len(self._prices),
        }


class OKXWebSocketPriceFeed:
    """
    OKX WebSocket 价格订阅（币安优先，OKX备用）
    """

    def __init__(
        self,
        symbols: list[str],
        on_price_update: Callable[[str, dict[str, Any]], None] | None = None,
        testnet: bool = False,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.on_price_update = on_price_update
        self.testnet = testnet

        self._prices: dict[str, dict[str, Any]] = {}
        self._prices_lock = threading.Lock()

        self._ws = None
        self._connected = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def _get_ws_url(self) -> str:
        """获取 OKX WebSocket URL"""
        if self.testnet:
            return "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
        return "wss://ws.okx.com:8443/ws/v5/business"

    async def _connect_and_listen(self):
        """连接并监听"""
        url = self._get_ws_url()
        logger.info(f"[OKX WebSocket] 连接中...")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                self._ws = ws

                # 订阅频道
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [
                        {"channel": "tickers", "instId": f"{s}-SWAP"}
                        for s in self.symbols
                    ]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"[OKX WebSocket] 订阅发送成功")

                self._connected = True

                async for message in ws:
                    if self._stop_event.is_set():
                        break
                    await self._handle_message(message)

        except Exception as e:
            logger.error(f"[OKX WebSocket] 错误: {e}")
        finally:
            self._connected = False

    async def _handle_message(self, message: str):
        """处理消息"""
        try:
            data = json.loads(message)
            event = data.get("event", "")

            if event == "subscribe":
                logger.info(f"[OKX WebSocket] 订阅成功: {data}")
                return

            if "data" in data:
                for item in data.get("data", []):
                    inst_id = item.get("instId", "")  # BSB-USDT-SWAP
                    symbol = inst_id.replace("-USDT-SWAP", "").replace("-USD-SWAP", "")

                    price_data = {
                        "last": float(item.get("last", 0)),
                        "mark": float(item.get("markPx", 0)),
                        "received_at": time.time(),
                    }

                    with self._prices_lock:
                        self._prices[symbol] = price_data

                    if self.on_price_update:
                        self.on_price_update(symbol, price_data)

        except Exception as e:
            logger.error(f"[OKX WebSocket] 处理错误: {e}")

    def _run_loop(self):
        """运行事件循环"""
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
            except Exception as e:
                logger.error(f"[OKX WebSocket] 循环错误: {e}")

            if self._stop_event.is_set():
                break

            time.sleep(5)

    def start(self):
        """启动"""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="okx-websocket-feed"
        )
        self._thread.start()

    def stop(self):
        """停止"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """获取价格"""
        symbol = symbol.upper()
        with self._prices_lock:
            return self._prices.get(symbol, {}).copy() if symbol in self._prices else None

    def is_connected(self) -> bool:
        """检查连接"""
        return self._connected


# 兼容性：为 TrailingStopWorker 提供的简单接口
def create_websocket_feed(
    exchange_id: str,
    symbols: list[str],
    on_price_update: Callable[[str, dict[str, Any]], None] | None = None,
    testnet: bool = False,
) -> BinanceWebSocketPriceFeed | OKXWebSocketPriceFeed | None:
    """
    创建对应交易所的 WebSocket 价格订阅

    Args:
        exchange_id: 'binance' 或 'okx'
        symbols: 币种列表
        on_price_update: 价格更新回调
        testnet: 是否测试网

    Returns:
        WebSocket 价格订阅实例
    """
    exchange_id = exchange_id.lower()

    if exchange_id == "binance":
        return BinanceWebSocketPriceFeed(symbols, on_price_update, testnet)
    elif exchange_id == "okx":
        return OKXWebSocketPriceFeed(symbols, on_price_update, testnet)
    else:
        logger.error(f"[WebSocket] 不支持的交易所: {exchange_id}")
        return None
