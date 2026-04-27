# -*- coding: utf-8 -*-
"""
币安/OKX WebSocket 价格订阅模块
提供实时最新价和标记价格，替代 REST API 轮询

同时支持:
    - BinanceWebSocketPriceFeed: 币安合约，订阅 aggTrade + markPrice (JSON订阅方式)
    - OKXWebSocketPriceFeed:     OKX 合约，订阅 tickers 频道
两个实现提供完全一致的公开接口，可通过 create_websocket_feed() 工厂函数统一创建
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
    币安合约 WebSocket 价格订阅 (JSON订阅方式，类似OKX)
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
        self._connected_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # 统计信息
        self._last_update_time: dict[str, float] = {}
        self._message_count = 0
        self._reconnect_count = 0

    def _get_ws_url(self) -> str:
        """获取币安 WebSocket URL (JSON订阅方式)"""
        if self.testnet:
            return "wss://stream.binancefuture.com/ws"
        return "wss://fstream.binance.com/ws"

    def _to_stream_name(self, symbol: str, stream_type: str) -> str:
        """将币种转换为币安流名称"""
        lower = symbol.lower().replace('/', '')
        return f"{lower}@{stream_type}"

    async def _send_ping(self, ws):
        """发送 ping 保持连接"""
        try:
            ping_msg = {"method": "LISTEN_KEY", "params": [], "id": int(time.time() * 1000)}
            await ws.send(json.dumps(ping_msg))
        except Exception:
            pass

    async def _connect_and_listen(self):
        """连接并监听 (类似OKX的实现方式)"""
        if not self.symbols:
            logger.warning("[Binance WebSocket] 没有订阅任何币种，等待 5 秒后重试")
            await asyncio.sleep(5)
            return

        url = self._get_ws_url()
        logger.info(f"[Binance WebSocket] 连接中... {url}")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                self._ws = ws

                # 订阅频道：aggTrade + markPrice
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": [],
                    "id": int(time.time() * 1000)
                }
                
                # 为每个币种添加两个流
                for symbol in self.symbols:
                    subscribe_msg["params"].append(self._to_stream_name(symbol, "aggTrade"))
                    subscribe_msg["params"].append(self._to_stream_name(symbol, "markPrice"))

                await ws.send(json.dumps(subscribe_msg))
                logger.info(
                    "[Binance WebSocket] 已发送订阅请求，%d 个币种，%d 个流",
                    len(self.symbols),
                    len(subscribe_msg["params"])
                )

                with self._connected_lock:
                    self._connected = True

                last_ping = time.time()

                async for message in ws:
                    if self._stop_event.is_set():
                        break

                    # 每 20 秒发送一次 ping
                    now = time.time()
                    if now - last_ping > 20:
                        await self._send_ping(ws)
                        last_ping = now

                    try:
                        data = json.loads(message)

                        # 处理订阅确认消息
                        if "id" in data and "result" in data:
                            logger.info(
                                "[Binance WebSocket] 订阅成功: %d 个流",
                                len(subscribe_msg["params"])
                            )
                            continue

                        # 处理错误消息
                        if "error" in data:
                            logger.error(
                                "[Binance WebSocket] 错误: %s",
                                data.get("msg", data.get("error", "未知错误"))
                            )
                            continue

                        # 处理推送数据
                        stream = data.get("stream", "")
                        payload = data.get("data", {})

                        if not stream or "@" not in stream:
                            continue

                        stream_symbol, stream_type = stream.split("@", 1)
                        symbol = stream_symbol.upper()

                        price_data = self._parse_stream_message(stream_type, payload)
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
                                    logger.error(f"[Binance WebSocket] 回调错误: {e}")

                    except json.JSONDecodeError as e:
                        logger.error(f"[Binance WebSocket] JSON解析错误: {e}")
                    except Exception as e:
                        logger.error(f"[Binance WebSocket] 消息处理错误: {e}")

        except ConnectionClosedOK:
            logger.info("[Binance WebSocket] 正常关闭")
        except ConnectionClosed as e:
            logger.warning(f"[Binance WebSocket] 连接断开: {e}")
        except Exception as e:
            logger.error(f"[Binance WebSocket] 错误: {e}")

    def _parse_stream_message(self, stream_type: str, data: dict) -> dict[str, Any] | None:
        """解析流消息"""
        result: dict[str, Any] = {}

        try:
            if stream_type == "aggTrade":
                result["last"] = float(data.get("p", 0))
                result["trade_time"] = data.get("T", 0)
            elif stream_type == "markPrice":
                # markPrice 每 3 秒推送一次
                result["mark"] = float(data.get("p", 0))
                result["index"] = float(data.get("i", 0))
                result["funding_rate"] = float(data.get("r", 0))
                result["next_funding_time"] = data.get("T", 0)
                # 将 mark 价同时作为 last 的备选
                result["last"] = result["mark"]
            return result if result else None
        except (ValueError, TypeError) as e:
            logger.error(f"[Binance WebSocket] 价格解析错误: {e}")
            return None

    def _run_loop(self):
        """在线程中运行事件循环"""
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
                if self._reconnect_count > 0:
                    logger.info("[Binance WebSocket] 连接恢复正常，重置重连计数器")
                    self._reconnect_count = 0
            except Exception as e:
                logger.error(f"[Binance WebSocket] 事件循环错误: {e}")

            if self._stop_event.is_set():
                break

            with self._connected_lock:
                self._connected = False
            self._reconnect_count += 1
            wait_time = min(5 + self._reconnect_count * 2, 30)
            logger.info(f"[Binance WebSocket] {wait_time}秒后重连... (第{self._reconnect_count}次)")
            time.sleep(wait_time)

    def start(self):
        """启动 WebSocket 连接"""
        if self._thread and self._thread.is_alive():
            logger.warning("[Binance WebSocket] 已经在运行")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="binance-websocket-price-feed"
        )
        self._thread.start()
        logger.info(f"[Binance WebSocket] 启动成功，订阅 {len(self.symbols)} 个币种")

    def stop(self):
        """停止 WebSocket 连接"""
        self._stop_event.set()
        self._connected = False

        if self._thread:
            self._thread.join(timeout=5)

        logger.info("[Binance WebSocket] 已停止")

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """
        获取指定币种的最新价格数据
        """
        symbol = symbol.upper().replace('/USDT', 'USDT')
        with self._prices_lock:
            data = self._prices.get(symbol, {}).copy() if symbol in self._prices else None
            # 诊断日志：检查数据是否存在及新鲜度
            if data:
                age = time.time() - data.get("received_at", 0)
                if age > 5.0:
                    logger.warning(
                        "[Binance WebSocket] %s 数据过期: %.1fs (连接=%s, 消息数=%d)",
                        symbol, age, self._connected, self._message_count
                    )
            return data

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
            logger.warning(f"[Binance WebSocket] {symbol} 数据过期: {age:.2f}s")
            return None

        return data

    def is_connected(self) -> bool:
        """检查连接状态（线程安全）"""
        with self._connected_lock:
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
    OKX 合约 WebSocket 价格订阅
    订阅：tickers 频道（包含最新价、标记价、指数价）
    与 BinanceWebSocketPriceFeed 保持相同接口和功能
    """

    def __init__(
        self,
        symbols: list[str],
        on_price_update: Callable[[str, dict[str, Any]], None] | None = None,
        testnet: bool = False,
    ):
        """
        Args:
            symbols: 订阅的币种列表，如 ['BTCUSDT', 'ETHUSDT']
                     OKX 内部会自动转换为合约格式
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
        self._connected_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # 统计信息
        self._last_update_time: dict[str, float] = {}
        self._message_count = 0
        self._reconnect_count = 0

    @staticmethod
    def _to_okx_symbol(symbol: str) -> str:
        """将统一格式(BSBUSDT)转换为 OKX 格式(BSB-USDT-SWAP)"""
        s = symbol.upper().replace('/USDT', 'USDT').replace('/USD', 'USD')
        if s.endswith('USDT'):
            base = s[:-4]
            return f"{base}-USDT-SWAP"
        if s.endswith('USD'):
            base = s[:-3]
            return f"{base}-USD-SWAP"
        return f"{s}-SWAP"

    @staticmethod
    def _from_okx_symbol(okx_sym: str) -> str:
        """将 OKX 格式(BSB-USDT-SWAP)转换为统一格式(BSBUSDT)"""
        return okx_sym.replace('-SWAP', '').replace('-', '')

    def _get_ws_url(self) -> str:
        """获取 OKX WebSocket URL"""
        if self.testnet:
            return "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
        return "wss://ws.okx.com:8443/ws/v5/public"

    async def _send_ping(self, ws):
        """发送 ping 保持连接（OKX 要求客户端发送 ping）"""
        try:
            await ws.send("ping")
        except Exception:
            pass

    async def _connect_and_listen(self):
        """连接并监听"""
        if not self.symbols:
            logger.warning("[OKX WebSocket] 没有订阅任何币种，等待 5 秒后重试")
            await asyncio.sleep(5)
            return

        url = self._get_ws_url()
        logger.info(f"[OKX WebSocket] 连接中...")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                self._ws = ws

                # 订阅频道：tickers（包含最新价、标记价、指数价、资金费率）
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [
                        {"channel": "tickers", "instId": self._to_okx_symbol(s)}
                        for s in self.symbols
                    ]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info(
                    "[OKX WebSocket] 已发送订阅请求，%d 个币种",
                    len(self.symbols)
                )

                with self._connected_lock:
                    self._connected = True

                last_ping = time.time()

                async for message in ws:
                    if self._stop_event.is_set():
                        break

                    # 每 20 秒发送一次 ping
                    now = time.time()
                    if now - last_ping > 20:
                        await self._send_ping(ws)
                        last_ping = now

                    try:
                        # OKX 可能发送纯文本 "pong"
                        if message == "pong":
                            continue

                        data = json.loads(message)

                        # 处理订阅确认消息
                        if "event" in data:
                            event = data["event"]
                            if event == "subscribe":
                                arg = data.get("arg", {})
                                logger.info(
                                    "[OKX WebSocket] 订阅成功: %s",
                                    arg.get("instId", "")
                                )
                            elif event == "error":
                                logger.error(
                                    "[OKX WebSocket] 订阅错误: %s",
                                    data.get("msg", "")
                                )
                            continue

                        # 处理推送数据
                        arg = data.get("arg", {})
                        channel = arg.get("channel", "")

                        if channel == "tickers" and "data" in data:
                            for item in data["data"]:
                                okx_inst_id = item.get("instId", "")
                                inst_id = self._from_okx_symbol(okx_inst_id)
                                if not inst_id:
                                    continue

                                price_data = self._parse_ticker(item)
                                if price_data:
                                    with self._prices_lock:
                                        self._prices[inst_id] = price_data
                                        self._last_update_time[inst_id] = time.time()

                                    self._message_count += 1

                                    if self.on_price_update:
                                        try:
                                            self.on_price_update(inst_id, self._prices[inst_id])
                                        except Exception as e:
                                            logger.error(f"[OKX WebSocket] 回调错误: {e}")

                    except json.JSONDecodeError as e:
                        logger.error(f"[OKX WebSocket] JSON解析错误: {e}")
                    except Exception as e:
                        logger.error(f"[OKX WebSocket] 消息处理错误: {e}")

        except ConnectionClosedOK:
            logger.info("[OKX WebSocket] 正常关闭")
        except ConnectionClosed as e:
            logger.warning(f"[OKX WebSocket] 连接断开: {e}")
        except Exception as e:
            logger.error(f"[OKX WebSocket] 错误: {e}")

    def _parse_ticker(self, item: dict) -> dict[str, Any] | None:
        """解析 OKX ticker 数据"""
        try:
            return {
                "last": float(item.get("last", 0)),
                "mark": float(item.get("markPx", 0)),
                "index": float(item.get("idxPx", 0)),
                "funding_rate": float(item.get("fundingRate", 0)),
                "next_funding_time": int(item.get("fundingTime", 0)),
                "open_24h": float(item.get("open24h", 0)),
                "high_24h": float(item.get("high24h", 0)),
                "low_24h": float(item.get("low24h", 0)),
                "vol_24h": float(item.get("volCcy24h", 0)),
                "received_at": time.time(),
            }
        except (ValueError, TypeError) as e:
            logger.error(f"[OKX WebSocket] 价格解析错误: {e}")
            return None

    def _run_loop(self):
        """在线程中运行事件循环。_connected 由 _connect_and_listen 在成功建立连接后设置。"""
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
                if self._reconnect_count > 0:
                    logger.info("[OKX WebSocket] 连接恢复正常，重置重连计数器")
                    self._reconnect_count = 0
            except Exception as e:
                logger.error(f"[OKX WebSocket] 事件循环错误: {e}")

            if self._stop_event.is_set():
                break

            with self._connected_lock:
                self._connected = False
            self._reconnect_count += 1
            wait_time = min(5 + self._reconnect_count * 2, 30)
            logger.info(f"[OKX WebSocket] {wait_time}秒后重连... (第{self._reconnect_count}次)")
            time.sleep(wait_time)

    def start(self):
        """启动 WebSocket 连接"""
        if self._thread and self._thread.is_alive():
            logger.warning("[OKX WebSocket] 已经在运行")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="okx-websocket-price-feed"
        )
        self._thread.start()
        logger.info(f"[OKX WebSocket] 启动成功，订阅 {len(self.symbols)} 个币种")

    def stop(self):
        """停止 WebSocket 连接"""
        self._stop_event.set()
        self._connected = False

        if self._thread:
            self._thread.join(timeout=5)

        logger.info("[OKX WebSocket] 已停止")

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
            logger.warning(f"[OKX WebSocket] {symbol} 数据过期: {age:.2f}s")
            return None

        return data

    def is_connected(self) -> bool:
        """检查连接状态（线程安全）"""
        with self._connected_lock:
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


# 兼容性：为 TrailingStopWorker 提供的工厂函数
def create_websocket_feed(
    exchange_id: str,
    symbols: list[str],
    on_price_update: Callable[[str, dict[str, Any]], None] | None = None,
    testnet: bool = False,
) -> BinanceWebSocketPriceFeed | OKXWebSocketPriceFeed | None:
    """
    创建对应交易所的 WebSocket 价格订阅

    两个实现提供完全相同的公开接口：

    公开方法:
        start()                   启动 WebSocket 连接
        stop()                    停止 WebSocket 连接
        is_connected()            检查连接状态（线程安全）
        get_price(symbol)         获取指定币种的最新价格数据
        get_price_with_age(symbol, max_age_sec=1.0)  获取价格，如果数据太旧返回 None
        get_stats()               获取统计信息

    参数:
        exchange_id: 交易所 ID，如 'binance', 'okx'
        symbols: 订阅的币种列表，如 ['BTCUSDT', 'ETHUSDT']
        on_price_update: 价格更新回调函数(symbol, price_data)
        testnet: 是否使用测试网

    返回:
        WebSocket 价格订阅实例，如果交易所不支持则返回 None
    """
    exchange_id = exchange_id.lower()

    if exchange_id == "binance":
        return BinanceWebSocketPriceFeed(symbols, on_price_update, testnet)
    elif exchange_id == "okx":
        return OKXWebSocketPriceFeed(symbols, on_price_update, testnet)
    else:
        logger.warning(f"[WebSocket] 不支持的交易所: {exchange_id}")
        return None
