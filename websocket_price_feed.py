# -*- coding: utf-8 -*-
"""
币安/OKX WebSocket 价格订阅模块
提供实时最新价和标记价格，替代 REST API 轮询

同时支持:
    - BinanceWebSocketPriceFeed: 币安合约，订阅 ticker 流 (组合流方式)
    - OKXWebSocketPriceFeed:     OKX 合约，订阅 tickers 频道
两个实现提供完全一致的公开接口，可通过 create_websocket_feed() 工厂函数统一创建
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from typing import Any, Callable

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

logger = logging.getLogger(__name__)


class BinanceWebSocketPriceFeed:
    """
    币安合约 WebSocket 价格订阅
    订阅：ticker 流（组合流方式，每1-2秒推送）
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
        self.symbols = [
            s.upper()
            .replace('/USDT', 'USDT').replace(':USDT', '')
            .replace('/USD', 'USD').replace(':USD', '')
            for s in symbols
        ]
        self.on_price_update = on_price_update
        self.testnet = testnet

        # 价格数据存储
        self._prices: dict[str, dict[str, Any]] = {}
        self._prices_lock = threading.Lock()

        # WebSocket 连接
        self._connected = False
        self._connected_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # 统计信息
        self._last_update_time: dict[str, float] = {}
        self._message_count = 0
        self._reconnect_count = 0
        self._disconnect_count = 0
        self._last_disconnect_reason: str = ""
        self._last_connected_time: float = 0.0
        self._disconnect_timestamps: list[tuple[float, str]] = []

    def _get_ws_url(self) -> str:
        """获取币安组合流 WebSocket URL

        2026-04-23 起，币安期货 WebSocket 强制使用路由路径 (/public、/market、/private)：
            - markPrice、kline、ticker、aggTrade 等常规行情 → /market
            - depth、bookTicker 等高频行情 → /public
            - 用户数据(listenKey) → /private
        老地址 wss://fstream.binance.com/stream?streams= 不带路由，markPrice 不会推送数据。
        参考：https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams

        组合订阅：
            @markPrice@1s - 标记价（1秒强制推送，不依赖交易活动）
                            注：默认 @markPrice 是 3秒，加 @1s 后变为 1秒，更低延迟
            @aggTrade     - 聚合成交（按交易触发，提供真实最新成交价）
        """
        if self.testnet:
            base_url = "wss://stream.binancefuture.com"
        else:
            base_url = "wss://fstream.binance.com"
        streams = []
        for sym in self.symbols:
            lower = sym.lower()
            streams.append(f"{lower}@markPrice@1s")
            streams.append(f"{lower}@aggTrade")
        return f"{base_url}/market/stream?streams={'/'.join(streams)}"

    async def _connect_and_listen(self):
        """连接并监听（组合流方式），手动管理连接生命周期"""
        if not self.symbols:
            logger.warning("[Binance WebSocket] 没有订阅任何币种，等待 5 秒后重试")
            await asyncio.sleep(5)
            return

        url = self._get_ws_url()
        logger.info("[Binance WebSocket] 连接中... %s", url)

        ws = None
        try:
            ws = await websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=5,
                close_timeout=3,
                max_size=2 ** 20,
            )

            logger.info(
                "[Binance WebSocket] 已连接，订阅 %d 个币种的 markPrice + aggTrade 流",
                len(self.symbols)
            )

            with self._connected_lock:
                self._connected = True
            self._last_connected_time = time.time()

            msg_count = 0
            no_data_time = time.time()
            first_data_arrived = False

            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    elapsed = time.time() - no_data_time
                    if first_data_arrived and elapsed > 15:
                        logger.error(
                            "[Binance WebSocket] %.0f 秒无新数据，即将重连",
                            elapsed
                        )
                        break
                    elif not first_data_arrived:
                        if elapsed > 30:
                            # 超过 30s 仍未收到任何数据，强制重连
                            logger.error(
                                "[Binance WebSocket] 连接后 %.0f 秒仍未收到首条数据，"
                                "强制断开重连", elapsed
                            )
                            break
                        elif elapsed > 10:
                            logger.warning(
                                "[Binance WebSocket] 连接后 %.0f 秒未收到首条数据，"
                                "TCP 正常但无数据推送，等待中...", elapsed
                            )
                    continue

                if self._stop_event.is_set():
                    break

                try:
                    msg_count += 1
                    if msg_count <= 3:
                        logger.info("[Binance WebSocket] 原始消息 #%d: %s", msg_count, message[:200])

                    data = json.loads(message)

                    stream = data.get("stream", "")
                    payload = data.get("data", {})

                    if not stream or "@" not in stream:
                        logger.debug("[Binance WebSocket] 非流消息: %s", list(data.keys()))
                        continue

                    # stream 形如 'btcusdt@markPrice@1s' 或 'btcusdt@aggTrade'
                    parts = stream.split("@")
                    if len(parts) < 2:
                        continue
                    stream_symbol = parts[0]
                    stream_type = parts[1]
                    symbol = stream_symbol.upper()

                    parsed: dict[str, Any] | None = None
                    if stream_type == "markPrice":
                        parsed = self._parse_mark_price(payload)
                        kind = "markPrice"
                    elif stream_type == "aggTrade":
                        parsed = self._parse_agg_trade(payload)
                        kind = "aggTrade"
                    else:
                        logger.debug("[Binance WebSocket] 未知流类型: %s", stream_type)
                        continue

                    if not parsed:
                        continue

                    now = time.time()
                    if not first_data_arrived:
                        first_data_arrived = True
                        self._last_connected_time = now
                        with self._connected_lock:
                            self._connected = True
                        logger.info(
                            "[Binance WebSocket] 收到首条 %s %s: %s",
                            kind, symbol, parsed
                        )

                    with self._prices_lock:
                        existing = self._prices.get(symbol, {})
                        # 合并新数据，保留另一来源的字段
                        merged = {**existing, **parsed, "received_at": now}
                        # 没收到 last 时用 mark 兜底，反之亦然
                        if "last" not in merged and "mark" in merged:
                            merged["last"] = merged["mark"]
                        if "mark" not in merged and "last" in merged:
                            merged["mark"] = merged["last"]
                        self._prices[symbol] = merged
                        self._last_update_time[symbol] = now
                        snapshot = dict(merged)

                    self._message_count += 1
                    no_data_time = now

                    if self.on_price_update:
                        try:
                            self.on_price_update(symbol, snapshot)
                        except Exception as e:
                            logger.error("[Binance WebSocket] 回调错误: %s", e)

                except json.JSONDecodeError as e:
                    logger.error("[Binance WebSocket] JSON解析错误: %s", e)
                except Exception as e:
                    logger.error("[Binance WebSocket] 消息处理错误: %s", e)

        except ConnectionClosedOK:
            logger.info("[Binance WebSocket] 正常关闭")
        except ConnectionClosed as e:
            logger.warning("[Binance WebSocket] 连接断开(code=%s): %s", e.code, e.reason)
            self._record_disconnect(f"ConnectionClosed code={e.code}")
        except OSError as e:
            logger.error("[Binance WebSocket] 网络错误: %s", e)
            self._record_disconnect(f"OSError: {e}")
        except asyncio.TimeoutError:
            logger.error("[Binance WebSocket] 连接超时")
            self._record_disconnect("Timeout")
        except Exception as e:
            logger.error("[Binance WebSocket] 连接异常: %s", e)
            self._record_disconnect(f"Exception: {type(e).__name__}")
        finally:
            with self._connected_lock:
                self._connected = False
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass

    def _parse_mark_price(self, data: dict) -> dict[str, Any] | None:
        """解析 markPrice 数据（每 1s 或 3s 强制推送，不依赖交易活动）"""
        try:
            mark = float(data.get("p", 0))
            if mark <= 0:
                return None
            return {
                "mark": mark,
                "index": float(data.get("i", 0)),
                "funding_rate": float(data.get("r", 0)),
                "next_funding_time": data.get("T", 0),
                "received_at_mark": time.time(),
            }
        except (ValueError, TypeError) as e:
            logger.error("[Binance WebSocket] markPrice 解析错误: %s", e)
            return None

    def _parse_agg_trade(self, data: dict) -> dict[str, Any] | None:
        """解析 aggTrade 数据（按聚合交易事件触发，提供真实最新成交价）"""
        try:
            last = float(data.get("p", 0))
            if last <= 0:
                return None
            return {
                "last": last,
                "qty": float(data.get("q", 0)),
                "trade_time": data.get("T", 0),
                "received_at_last": time.time(),
            }
        except (ValueError, TypeError) as e:
            logger.error("[Binance WebSocket] aggTrade 解析错误: %s", e)
            return None

    def _record_disconnect(self, reason: str):
        """记录断连信息，保留最近 20 条断连记录"""
        self._disconnect_count += 1
        self._last_disconnect_reason = reason
        ts = (time.time(), reason)
        self._disconnect_timestamps.append(ts)
        if len(self._disconnect_timestamps) > 20:
            self._disconnect_timestamps.pop(0)

    def _run_loop(self):
        """在线程中运行事件循环，指数退避重连"""
        consecutive_failures = 0
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
                if consecutive_failures > 0:
                    logger.info(
                        "[Binance WebSocket] 连接成功，重置退避计数器 (此前连续失败 %d 次)",
                        consecutive_failures
                    )
                    consecutive_failures = 0
            except Exception as e:
                logger.error("[Binance WebSocket] 事件循环错误: %s", e)

            if self._stop_event.is_set():
                break

            with self._connected_lock:
                self._connected = False
            self._reconnect_count += 1
            consecutive_failures += 1

            # 指数退避：1s, 2s, 4s, 8s, 16s → cap at 30s
            wait = min(2 ** (consecutive_failures - 1), 30)
            # 添加 ±25% 随机抖动
            jitter = random.uniform(-wait * 0.25, wait * 0.25)
            wait = max(0.5, wait + jitter)
            logger.info(
                "[Binance WebSocket] %.1f秒后重连... (第%d次重连，连续失败%d次)",
                wait, self._reconnect_count, consecutive_failures
            )
            time.sleep(wait)

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
        logger.info("[Binance WebSocket] 启动成功，订阅 %d 个币种", len(self.symbols))

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
        symbol = (
            symbol.upper()
            .replace('/USDT', 'USDT').replace(':USDT', '')
            .replace('/USD', 'USD').replace(':USD', '')
        )
        with self._prices_lock:
            data = self._prices.get(symbol, {}).copy() if symbol in self._prices else None
            if data:
                age = time.time() - data.get("received_at", 0)
                if age > 5.0:
                    logger.warning("[Binance WebSocket] %s 数据过期: %.1fs (连接=%s, 消息数=%d)",
                        symbol, age, self._connected, self._message_count)
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
            logger.warning("[Binance WebSocket] %s 数据过期: %.2fs", symbol, age)
            return None

        return data

    def is_connected(self) -> bool:
        """检查连接状态（线程安全）"""
        with self._connected_lock:
            return self._connected

    def has_received_data(self) -> bool:
        """是否已收到过实际推送数据（区别于 TCP 连接状态）"""
        return self._message_count > 0

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息"""
        with self._prices_lock:
            prices_snapshot = dict(self._prices)
        recent_disconnects = []
        for ts, reason in self._disconnect_timestamps[-5:]:
            recent_disconnects.append({
                "time_ago": round(time.time() - ts, 1),
                "reason": reason,
            })
        return {
            "connected": self._connected,
            "symbols": len(self.symbols),
            "message_count": self._message_count,
            "reconnect_count": self._reconnect_count,
            "disconnect_count": self._disconnect_count,
            "last_disconnect_reason": self._last_disconnect_reason,
            "last_connected_ago": round(time.time() - self._last_connected_time, 1) if self._last_connected_time > 0 else -1,
            "recent_disconnects": recent_disconnects,
            "prices_cached": len(prices_snapshot),
            "prices_age": {
                sym: round(time.time() - d.get("received_at", 0), 1)
                for sym, d in list(prices_snapshot.items())[:5]
            },
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
        self.symbols = [
            s.upper()
            .replace('/USDT', 'USDT').replace(':USDT', '')
            .replace('/USD', 'USD').replace(':USD', '')
            for s in symbols
        ]
        self.on_price_update = on_price_update
        self.testnet = testnet

        # 价格数据存储
        self._prices: dict[str, dict[str, Any]] = {}
        self._prices_lock = threading.Lock()

        # WebSocket 连接
        self._connected = False
        self._connected_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # 统计信息
        self._last_update_time: dict[str, float] = {}
        self._message_count = 0
        self._reconnect_count = 0
        self._disconnect_count = 0
        self._last_disconnect_reason: str = ""
        self._last_connected_time: float = 0.0
        self._disconnect_timestamps: list[tuple[float, str]] = []

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
        except Exception as e:
            logger.debug("[OKX WebSocket] ping 发送失败: %s", e)

    async def _connect_and_listen(self):
        """连接并监听，手动管理连接生命周期"""
        if not self.symbols:
            logger.warning("[OKX WebSocket] 没有订阅任何币种，等待 5 秒后重试")
            await asyncio.sleep(5)
            return

        url = self._get_ws_url()
        logger.info("[OKX WebSocket] 连接中...")

        ws = None
        try:
            ws = await websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=5,
                close_timeout=3,
                max_size=2 ** 20,
            )

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
            self._last_connected_time = time.time()

            first_data_arrived = False
            last_ping = time.time()
            no_data_time = time.time()

            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    now = time.time()
                    # 每 15 秒发送一次 ping（比 OKX 默认断线超时短）
                    if now - last_ping > 15:
                        await self._send_ping(ws)
                        last_ping = now
                    # 首条数据超时 15 秒，后续数据超时 30 秒
                    elapsed = time.time() - no_data_time
                    if (first_data_arrived and elapsed > 30) or (not first_data_arrived and elapsed > 15):
                        logger.error(
                            "[OKX WebSocket] %.0f 秒无数据，即将重连",
                            elapsed
                        )
                        break
                    continue

                if self._stop_event.is_set():
                    break

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
                                if not first_data_arrived:
                                    first_data_arrived = True
                                    self._last_connected_time = time.time()
                                    with self._connected_lock:
                                        self._connected = True
                                    logger.info(
                                        "[OKX WebSocket] 收到首条数据 %s: last=%s mark=%s",
                                        inst_id, price_data.get("last"), price_data.get("mark")
                                    )

                                with self._prices_lock:
                                    self._prices[inst_id] = price_data
                                    self._last_update_time[inst_id] = time.time()

                                self._message_count += 1
                                no_data_time = time.time()

                                if self.on_price_update:
                                    try:
                                        self.on_price_update(inst_id, self._prices[inst_id])
                                    except Exception as e:
                                        logger.error("[OKX WebSocket] 回调错误: %s", e)

                except json.JSONDecodeError as e:
                    logger.error("[OKX WebSocket] JSON解析错误: %s", e)
                except Exception as e:
                    logger.error("[OKX WebSocket] 消息处理错误: %s", e)

        except ConnectionClosedOK:
            logger.info("[OKX WebSocket] 正常关闭")
        except ConnectionClosed as e:
            logger.warning("[OKX WebSocket] 连接断开(code=%s): %s", e.code, e.reason)
            self._record_disconnect(f"ConnectionClosed code={e.code}")
        except OSError as e:
            logger.error("[OKX WebSocket] 网络错误: %s", e)
            self._record_disconnect(f"OSError: {e}")
        except asyncio.TimeoutError:
            logger.error("[OKX WebSocket] 连接超时")
            self._record_disconnect("Timeout")
        except Exception as e:
            logger.error("[OKX WebSocket] 连接异常: %s", e)
            self._record_disconnect(f"Exception: {type(e).__name__}")
        finally:
            with self._connected_lock:
                self._connected = False
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass

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
            logger.error("[OKX WebSocket] 价格解析错误: %s", e)
            return None

    def _record_disconnect(self, reason: str):
        """记录断连信息，保留最近 20 条断连记录"""
        self._disconnect_count += 1
        self._last_disconnect_reason = reason
        ts = (time.time(), reason)
        self._disconnect_timestamps.append(ts)
        if len(self._disconnect_timestamps) > 20:
            self._disconnect_timestamps.pop(0)

    def _run_loop(self):
        """在线程中运行事件循环，指数退避重连"""
        consecutive_failures = 0
        while not self._stop_event.is_set():
            try:
                asyncio.run(self._connect_and_listen())
                if consecutive_failures > 0:
                    logger.info(
                        "[OKX WebSocket] 连接成功，重置退避计数器 (此前连续失败 %d 次)",
                        consecutive_failures
                    )
                    consecutive_failures = 0
            except Exception as e:
                logger.error("[OKX WebSocket] 事件循环错误: %s", e)

            if self._stop_event.is_set():
                break

            with self._connected_lock:
                self._connected = False
            self._reconnect_count += 1
            consecutive_failures += 1

            # 指数退避：1s, 2s, 4s, 8s, 16s → cap at 30s
            wait = min(2 ** (consecutive_failures - 1), 30)
            jitter = random.uniform(-wait * 0.25, wait * 0.25)
            wait = max(0.5, wait + jitter)
            logger.info(
                "[OKX WebSocket] %.1f秒后重连... (第%d次重连，连续失败%d次)",
                wait, self._reconnect_count, consecutive_failures
            )
            time.sleep(wait)

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
        logger.info("[OKX WebSocket] 启动成功，订阅 %d 个币种", len(self.symbols))

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
        symbol = (
            symbol.upper()
            .replace('/USDT', 'USDT').replace(':USDT', '')
            .replace('/USD', 'USD').replace(':USD', '')
        )
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
            logger.warning("[OKX WebSocket] %s 数据过期: %.2fs", symbol, age)
            return None

        return data

    def is_connected(self) -> bool:
        """检查连接状态（线程安全）"""
        with self._connected_lock:
            return self._connected

    def has_received_data(self) -> bool:
        """是否已收到过实际推送数据（区别于 TCP 连接状态）"""
        return self._message_count > 0

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息"""
        with self._prices_lock:
            prices_snapshot = dict(self._prices)
        recent_disconnects = []
        for ts, reason in self._disconnect_timestamps[-5:]:
            recent_disconnects.append({
                "time_ago": round(time.time() - ts, 1),
                "reason": reason,
            })
        return {
            "connected": self._connected,
            "symbols": len(self.symbols),
            "message_count": self._message_count,
            "reconnect_count": self._reconnect_count,
            "disconnect_count": self._disconnect_count,
            "last_disconnect_reason": self._last_disconnect_reason,
            "last_connected_ago": round(time.time() - self._last_connected_time, 1) if self._last_connected_time > 0 else -1,
            "recent_disconnects": recent_disconnects,
            "prices_cached": len(prices_snapshot),
            "prices_age": {
                sym: round(time.time() - d.get("received_at", 0), 1)
                for sym, d in list(prices_snapshot.items())[:5]
            },
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
        logger.warning("[WebSocket] 不支持的交易所: %s", exchange_id)
        return None
