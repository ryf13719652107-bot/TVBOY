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
        self.symbols = [s.upper().replace('/USDT', 'USDT').replace('/USD', 'USD') for s in symbols]
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

    def _get_ws_url(self, symbol: str, stream_type: str) -> str:
        """获取单一 WebSocket URL"""
        base_url = (
            "wss://stream.binancefuture.com"  # 测试网
            if self.testnet
            else "wss://fstream.binance.com"   # 主网
        )
        symbol_lower = symbol.lower().replace('/', '')
        return f"{base_url}/ws/{symbol_lower}@{stream_type}"

    async def _subscribe_single(self, symbol: str, stream_type: str):
        """订阅单一币种的单一数据流"""
        url = self._get_ws_url(symbol, stream_type)
        
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                logger.info(f"[WebSocket] 已连接: {symbol}@{stream_type}")
                
                async for message in ws:
                    if self._stop_event.is_set():
                        break
                    
                    try:
                        data = json.loads(message)
                        
                        # 单一连接格式直接是数据，没有 stream 字段
                        price_data = self._parse_single_message(stream_type, data)
                        if price_data:
                            with self._prices_lock:
                                if symbol not in self._prices:
                                    self._prices[symbol] = {}
                                self._prices[symbol].update(price_data)
                                self._prices[symbol]["received_at"] = time.time()
                                self._last_update_time[symbol] = time.time()
                            
                            self._message_count += 1
                            
                            if self.on_price_update:
                                try:
                                    self.on_price_update(symbol, self._prices[symbol])
                                except Exception as e:
                                    logger.error(f"[WebSocket] 回调错误: {e}")
                                    
                    except json.JSONDecodeError as e:
                        logger.error(f"[WebSocket] JSON解析错误: {e}")
                    except Exception as e:
                        logger.error(f"[WebSocket] 消息处理错误: {e}")
                        
        except ConnectionClosedOK:
            logger.info(f"[WebSocket] {symbol}@{stream_type} 正常关闭")
        except ConnectionClosed as e:
            logger.warning(f"[WebSocket] {symbol}@{stream_type} 连接断开: {e}")
        except Exception as e:
            logger.error(f"[WebSocket] {symbol}@{stream_type} 错误: {e}")

    def _parse_single_message(self, stream_type: str, data: dict) -> dict[str, Any] | None:
        """解析单一连接的消息"""
        result: dict[str, Any] = {}
        
        try:
            if stream_type == "aggTrade":
                # 最新成交
                result["last"] = float(data.get("p", 0))  # 成交价格
                result["trade_time"] = data.get("T", 0)   # 成交时间
                
            elif stream_type == "markPrice":
                # 标记价格
                result["mark"] = float(data.get("p", 0))   # 标记价格
                result["index"] = float(data.get("i", 0))  # 指数价格
                result["funding_rate"] = float(data.get("r", 0))  # 资金费率
                result["next_funding_time"] = data.get("T", 0)
                
            return result if result else None
            
        except (ValueError, TypeError) as e:
            logger.error(f"[WebSocket] 价格解析错误: {e}")
            return None

    async def _run_all_subscriptions(self):
        """运行所有订阅"""
        tasks = []
        
        for symbol in self.symbols:
            # 为每个币种创建两个任务：aggTrade 和 markPrice
            tasks.append(self._subscribe_single(symbol, "aggTrade"))
            tasks.append(self._subscribe_single(symbol, "markPrice"))
        
        # 同时运行所有订阅
        await asyncio.gather(*tasks, return_exceptions=True)

    def _run_loop(self):
        """在线程中运行事件循环"""
        while not self._stop_event.is_set():
            try:
                self._connected = True
                asyncio.run(self._run_all_subscriptions())
            except Exception as e:
                logger.error(f"[WebSocket] 事件循环错误: {e}")

            if self._stop_event.is_set():
                break

            # 重连
            self._connected = False
            self._reconnect_count += 1
            wait_time = min(5 + self._reconnect_count * 2, 30)
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
        logger.info(f"[WebSocket] 启动成功，订阅 {len(self.symbols)} 个币种")

    def stop(self):
        """停止 WebSocket 连接"""
        self._stop_event.set()
        self._connected = False

        if self._thread:
            self._thread.join(timeout=5)

        logger.info("[WebSocket] 已停止")

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """
        获取指定币种的最新价格数据
        """
        symbol = symbol.upper().replace('/USDT', 'USDT')
        with self._prices_lock:
            return self._prices.get(symbol, {}).copy() if symbol in self._prices else None

    def get_price_with_age(self, symbol: str, max_age_sec: float = 1.0) -> dict[str, Any] | None:
        """
        获取价格，如果数据太旧返回 None
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
    """
    exchange_id = exchange_id.lower()

    if exchange_id == "binance":
        return BinanceWebSocketPriceFeed(symbols, on_price_update, testnet)
    elif exchange_id == "okx":
        return OKXWebSocketPriceFeed(symbols, on_price_update, testnet)
    else:
        logger.error(f"[WebSocket] 不支持的交易所: {exchange_id}")
        return None
