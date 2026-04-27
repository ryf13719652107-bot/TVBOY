# -*- coding: utf-8 -*-
"""币安 / OKX U 本位永续：多标的移动止盈 / 回撤止盈（供 TV 机器人后台线程调用）。"""
from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)


def _fe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _parse_position_row(
    position: dict[str, Any],
) -> tuple[str | None, float, float, float, str | None]:
    """
    返回 (unified_symbol, abs_qty, entry_price, mark_price, side long|short)。
    解析失败则 qty 为 0。
    """
    sym = str(position.get("symbol") or "").strip() or None
    info = position.get("info") or {}
    qty = 0.0
    c = position.get("contracts")
    if c is not None:
        try:
            qty = abs(float(c))
        except (TypeError, ValueError):
            qty = 0.0
    if qty <= 0 and info.get("positionAmt") is not None:
        try:
            qty = abs(float(info["positionAmt"]))
        except (TypeError, ValueError):
            qty = 0.0
    if qty <= 0 and info.get("pos") is not None:
        try:
            qty = abs(float(info["pos"]))
        except (TypeError, ValueError):
            qty = 0.0
    entry = _fe_float(position.get("entryPrice"), 0.0)
    if entry <= 0 and info.get("entryPrice") is not None:
        entry = _fe_float(info.get("entryPrice"), 0.0)
    if entry <= 0 and info.get("avgPx") is not None:
        entry = _fe_float(info.get("avgPx"), 0.0)
    mark = _fe_float(position.get("markPrice"), 0.0)
    if mark <= 0 and info.get("markPrice") is not None:
        mark = _fe_float(info.get("markPrice"), 0.0)
    side = str(position.get("side") or "").lower().strip() or None
    if side not in ("long", "short") and info.get("positionAmt") is not None:
        try:
            pa = float(info["positionAmt"])
            if pa > 0:
                side = "long"
            elif pa < 0:
                side = "short"
        except (TypeError, ValueError):
            pass
    return sym, qty, entry, mark, side


class TrailingStopWorker:
    """
    与独立脚本「移动止盈.py」逻辑一致；exchange 由外部注入（ccxt 币安或 OKX 实例）。
    调用方应在每次访问 exchange 时持有该账户的 trade_lock。
    """

    def __init__(
        self,
        exchange: Any,
        *,
        account_id: str,
        trade_lock: threading.Lock,
        normalize_symbol_fn: Callable[[str], str],
        stop_loss_pct: float,
        low_trail_stop_loss_pct: float,
        trail_stop_loss_pct: float,
        higher_trail_stop_loss_pct: float,
        low_trail_profit_threshold: float,
        first_trail_profit_threshold: float,
        second_trail_profit_threshold: float,
        feishu_webhook: str | None = None,
        blacklist: set[str] | None = None,
        status_hook: Callable[[str], None] | None = None,
        use_last_price: bool = False,
        use_last_price_only: bool = False,
        close_mode: str = "market",
        limit_offset_bps: float = 25.0,
        trailing_exec: str = "signal",
        exchange_algo_type: str = "stop_market",
        testnet: bool = False,
    ) -> None:
        self.exchange = exchange
        self.account_id = account_id
        self.trade_lock = trade_lock
        self.price_lock = threading.Lock()
        self._norm = normalize_symbol_fn
        self.use_last_price = bool(use_last_price)
        # 仅在「用最新价」时生效：True=分档/峰值仅用最新价浮盈；False= min(标,新) 保守
        self.use_last_price_only = bool(use_last_price_only) and self.use_last_price
        cm = (close_mode or "market").strip().lower()
        if cm == "limit":
            cm = "limit_ioc"
        self.close_mode = cm if cm in ("market", "limit_ioc") else "market"
        self.limit_offset_bps = max(0.0, float(limit_offset_bps))
        te = (trailing_exec or "signal").strip().lower()
        self.exchange_sync_stop = te in ("exchange_stop", "exchange", "stop")
        eat = (exchange_algo_type or "stop_market").strip().lower()
        self.exchange_algo_type = (
            eat if eat in ("stop_market", "stop_limit") else "stop_market"
        )
        self.testnet = testnet
        self._algo_trailing_id: dict[str, str] = {}
        self._last_trigger_tp_str: dict[str, str] = {}
        self.stop_loss_pct = float(stop_loss_pct)
        self.low_trail_stop_loss_pct = float(low_trail_stop_loss_pct)
        self.trail_stop_loss_pct = float(trail_stop_loss_pct)
        self.higher_trail_stop_loss_pct = float(higher_trail_stop_loss_pct)
        self.low_trail_profit_threshold = float(low_trail_profit_threshold)
        self.first_trail_profit_threshold = float(first_trail_profit_threshold)
        self.second_trail_profit_threshold = float(second_trail_profit_threshold)
        self.feishu_webhook = (feishu_webhook or "").strip() or None
        self.blacklist = blacklist or set()
        self.status_hook = status_hook

        self.highest_profits: dict[str, float] = {}
        self.current_tiers: dict[str, str] = {}
        self.detected_positions: set[str] = set()
        self._last_refresh_time: dict[str, float] = {}
        self._last_tier_sig: dict[str, str] = {}

        self._notify_queue: queue.Queue = queue.Queue(maxsize=200)
        self._notify_failures = 0
        self._notify_max_failures = 5
        self._notify_worker_stop = threading.Event()
        self._notify_worker = threading.Thread(
            target=self._notify_worker_loop,
            daemon=True,
            name=f"feishu-{account_id}",
        )
        self._notify_worker.start()

        self._price_feed_stop = threading.Event()
        self._price_feed_thread: threading.Thread | None = None
        self._price_feed_data: dict[str, Any] | None = None
        self._price_feed_data_ts: float = 0.0  # 修复：增加价格数据时间戳

        # WebSocket 价格订阅
        self._websocket_feed: Any | None = None
        self._use_websocket = True  # 是否使用 WebSocket
        self._subscribed_symbols: set[str] = set()

    def _norm_key(self, unified_symbol: str) -> str:
        return self._norm(unified_symbol)

    def _is_binance(self) -> bool:
        return str(getattr(self.exchange, "id", "") or "").lower() == "binance"

    def _is_okx(self) -> bool:
        return str(getattr(self.exchange, "id", "") or "").lower() == "okx"

    def _sync_algo_supported(self) -> bool:
        return self._is_binance() or self._is_okx()

    def _price_tick(self, symbol: str, market: dict[str, Any]) -> float:
        """合约最小价格变动（用于条件限价与触发价拉开距离，避免触发后无法成交）。"""
        info = market.get("info") or {}
        if self._is_binance():
            filters = info.get("filters")
            if isinstance(filters, list):
                for f in filters:
                    if isinstance(f, dict) and f.get("filterType") == "PRICE_FILTER":
                        t = _fe_float(f.get("tickSize"), 0.0)
                        if t > 0:
                            return t
        if self._is_okx():
            t = _fe_float(info.get("tickSz"), 0.0)
            if t > 0:
                return t
        prec = (market.get("precision") or {}).get("price")
        if isinstance(prec, int) and 0 < prec < 24:
            return float(10 ** (-prec))
        if isinstance(prec, float) and 0 < prec < 1:
            return float(prec)
        return 1e-6

    def _cancel_trailing_algo(self, symbol: str) -> bool:
        self._last_trigger_tp_str.pop(symbol, None)
        aid = self._algo_trailing_id.get(symbol)
        if not aid:
            return True
        try:
            with self.trade_lock:
                if self._is_binance():
                    self.exchange.request(
                        "algoOrder", "fapiPrivate", "DELETE", {"algoId": aid}
                    )
                elif self._is_okx():
                    self.exchange.cancel_order(str(aid), symbol, {"trigger": True})
                else:
                    return True
            self._algo_trailing_id.pop(symbol, None)
            logger.info("移动止盈[%s] 已撤条件单 %s id=%s", self.account_id, symbol, aid)
            return True
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 撤移动止盈条件单失败 %s: %s", self.account_id, symbol, e
            )
            return False

    def _start_websocket_feed(self, symbols: list[str]):
        """启动 WebSocket 价格订阅"""
        if not self._use_websocket:
            return

        try:
            from websocket_price_feed import create_websocket_feed

            exchange_id = getattr(self.exchange, "id", "")
            self._websocket_feed = create_websocket_feed(
                exchange_id=exchange_id,
                symbols=symbols,
                on_price_update=self._on_websocket_price_update,
                testnet=self.testnet,
            )

            if self._websocket_feed:
                self._websocket_feed.start()
                self._subscribed_symbols = set(symbols)
                logger.info(
                    "移动止盈[%s] WebSocket 价格订阅已启动，订阅 %d 个币种",
                    self.account_id,
                    len(symbols)
                )
            else:
                logger.warning(
                    "移动止盈[%s] WebSocket 不支持该交易所: %s",
                    self.account_id,
                    exchange_id
                )

        except Exception as e:
            logger.error(
                "移动止盈[%s] 启动 WebSocket 失败: %s，将使用 REST API",
                self.account_id,
                e
            )
            self._websocket_feed = None

    def _on_websocket_price_update(self, symbol: str, price_data: dict):
        """WebSocket 价格更新回调"""
        # 将 WebSocket 数据格式转换为与 fetch_tickers 一致
        ticker_data = {}

        if "last" in price_data:
            ticker_data["last"] = price_data["last"]
            ticker_data["close"] = price_data["last"]

        if "mark" in price_data:
            # 币安 WebSocket 不直接提供 mark，但我们可以存储
            pass

        if ticker_data:
            with self.price_lock:
                if self._price_feed_data is None:
                    self._price_feed_data = {}
                self._price_feed_data[symbol] = ticker_data
                self._price_feed_data_ts = time.time()

    def _stop_websocket_feed(self):
        """停止 WebSocket 价格订阅"""
        if self._websocket_feed:
            try:
                self._websocket_feed.stop()
                logger.info(
                    "移动止盈[%s] WebSocket 价格订阅已停止",
                    self.account_id
                )
            except Exception as e:
                logger.error(
                    "移动止盈[%s] 停止 WebSocket 失败: %s",
                    self.account_id,
                    e
                )
            finally:
                self._websocket_feed = None
                self._subscribed_symbols = set()

    def _update_websocket_symbols(self, symbols: list[str]):
        """更新 WebSocket 订阅的币种列表"""
        if not self._use_websocket:
            return

        current_symbols = set(symbols)

        # 首次有持仓，启动 WebSocket
        if not self._websocket_feed and current_symbols:
            logger.info(
                "移动止盈[%s] 首次检测到持仓，启动 WebSocket",
                self.account_id
            )
            self._start_websocket_feed(list(current_symbols))
            return

        # WebSocket 已启动，检查币种变化
        if self._websocket_feed and current_symbols != self._subscribed_symbols:
            # 清理不再持仓的币种数据，防止内存泄漏
            removed_symbols = self._subscribed_symbols - current_symbols
            for sym in removed_symbols:
                self.highest_profits.pop(sym, None)
                self.current_tiers.pop(sym, None)
                self.detected_positions.discard(sym)
                logger.info("移动止盈[%s] 清理已平仓币种数据: %s", self.account_id, sym)
            
            logger.info(
                "移动止盈[%s] 持仓币种变化，重新订阅 WebSocket",
                self.account_id
            )
            self._stop_websocket_feed()
            if current_symbols:  # 只有还有持仓才重新订阅
                self._start_websocket_feed(list(current_symbols))

    def _price_feed_loop(self, interval: float) -> None:
        """独立行情线程：每 interval 秒批量拉一次最新价，存入 _price_feed_data。
        WebSocket 正常时闲置（每 5s 检查一次）；WebSocket 失效时降级到 REST API，
        间隔不低于 3.0s 以避免触发交易所 API 限频。"""
        consecutive_errors = 0
        base_interval = max(3.0, interval)
        while not self._price_feed_stop.is_set():
            start_time = time.time()

            if self._websocket_feed and self._websocket_feed.is_connected():
                if self._price_feed_stop.wait(timeout=5.0):
                    break
                continue

            try:
                tickers = self.exchange.fetch_tickers()
                if isinstance(tickers, dict):
                    with self.price_lock:
                        self._price_feed_data = tickers
                        self._price_feed_data_ts = time.time()
                    consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                wait = min(consecutive_errors, 5)
                logger.warning(
                    "移动止盈[%s] 价格推送获取失败(%d次) %s，%ds后重试",
                    self.account_id, consecutive_errors, e, wait,
                )
                if self._price_feed_stop.wait(timeout=wait):
                    break
                continue

            elapsed = time.time() - start_time
            sleep_time = max(0, base_interval - elapsed)
            if self._price_feed_stop.wait(timeout=sleep_time):
                break

    def restore_existing_algos(self) -> None:
        """启动时扫描交易所已有条件单，恢复 _algo_trailing_id，避免重启后重复挂单。"""
        try:
            with self.price_lock:
                if self._is_binance():
                    raw = self.exchange.request(
                        "algoOrder", "fapiPrivate", "GET", {}
                    )
                    algo_list = []
                    if isinstance(raw, dict):
                        algo_list = raw.get("data", []) if isinstance(raw.get("data"), list) else []
                    elif isinstance(raw, list):
                        algo_list = raw
                    for item in algo_list:
                        if not isinstance(item, dict):
                            continue
                        sym = str(item.get("symbol") or "")
                        algo_id = str(item.get("algoId") or item.get("clientAlgoId") or "")
                        status = str(item.get("algoStatus") or "")
                        if not sym or not algo_id or status != "WORKING":
                            continue
                        usym = self._norm_key(sym) if hasattr(self, "_norm_key") else sym
                        self._algo_trailing_id[usym] = algo_id
                        logger.info(
                            "移动止盈[%s] 恢复已有条件单 %s algoId=%s",
                            self.account_id, usym, algo_id,
                        )
                elif self._is_okx():
                    try:
                        pos = self.exchange.fetch_positions()
                    except Exception:
                        pos = []
                    active_syms = set()
                    for p in pos:
                        sym, q, _, _, _ = _parse_position_row(p)
                        if sym and q > 0:
                            active_syms.add(sym)
                    for sym in active_syms:
                        try:
                            orders = self.exchange.fetch_open_orders(sym, {"trigger": True})
                            for o in (orders or []):
                                oid = o.get("id")
                                if oid:
                                    self._algo_trailing_id[sym] = str(oid)
                                    logger.info(
                                        "移动止盈[%s] 恢复OKX条件单 %s id=%s",
                                        self.account_id, sym, oid,
                                    )
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 恢复条件单失败（可忽略）: %s",
                self.account_id, e,
            )

    def _tier_profit_exit_threshold(
        self, current_tier: str, highest_profit: float
    ) -> float | None:
        """
        条件单 & 轮询共用的「自峰值回撤后」浮盈比例阈值(%)。
        低档保护：复用第一档的回撤比例 trail_stop_loss_pct（峰值 × (1 - 回撤比例)），
        同时叠加 low_trail_stop_loss_pct 绝对底线取 max，避免浮盈尚低时被浅回撤打出。
        未进入任一档返回 None。
        """
        if current_tier == "低档保护止盈":
            return max(
                self.low_trail_stop_loss_pct,
                highest_profit * (1.0 - self.trail_stop_loss_pct),
            )
        if current_tier == "第一档移动止盈":
            return highest_profit * (1.0 - self.trail_stop_loss_pct)
        if current_tier == "第二档移动止盈":
            return highest_profit * (1.0 - self.higher_trail_stop_loss_pct)
        return None

    def _batch_ticker_price(self, symbol: str, fallback: float) -> float:
        """从 _batch_tickers 取最新价，仅内存读取，不发起网络请求。
        若 batch 中无数据则返回 fallback。"""
        batch = getattr(self, "_batch_tickers", None)
        if batch and isinstance(batch, dict):
            tk = batch.get(symbol, {})
            if isinstance(tk, dict):
                cp = _fe_float(tk.get("last") or tk.get("close") or tk.get("markPrice"), 0.0)
                if cp > 0:
                    return cp
        return fallback

    def _sync_trailing_stop_algo(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        position_amt: float,
        current_tier: str,
        highest_profit: float,
        *,
        _profit_pct: float,
    ) -> None:
        """币安：CONDITIONAL STOP / STOP_MARKET。OKX：计划委托 ordType=trigger + limit|market（ccxt）。"""
        if not self._sync_algo_supported():
            return
        if current_tier == "无":
            self._cancel_trailing_algo(symbol)
            return
        th = self._tier_profit_exit_threshold(current_tier, highest_profit)
        if th is None:
            self._cancel_trailing_algo(symbol)
            return
        # 回撤止盈线只按「历史最高浮盈」计算，再不要用 min(当前浮盈±滑点) 去贴价。
        # 贴价会表现成：K 线附近出现「很低浮盈就有一条限价/条件线」，或达到一档后线随价格回撤而「下移」。
        profit_cutoff = th

        trigger_early_offset = 0.0002
        if side == "long":
            trigger = entry_price * (1.0 + profit_cutoff / 100.0) * (1.0 - trigger_early_offset)
            close_side = "SELL"
        else:
            trigger = entry_price * (1.0 - profit_cutoff / 100.0) * (1.0 + trigger_early_offset)
            close_side = "BUY"
        side_ccxt = close_side.lower()

        with self.trade_lock:
            market = self.exchange.market(symbol)
            symbol_id = str(market.get("id") or "")
            if not symbol_id:
                return
            trigger = float(self.exchange.price_to_precision(symbol, trigger))
            if self.exchange_algo_type == "stop_limit":
                tick = max(self._price_tick(symbol, market), 1e-12)
                bps = self.limit_offset_bps / 10000.0
                # 多头平多：卖限价须明显低于触发价；标记价触发时现价常已低于触发价，过小偏移会「已触发但不成交」
                if side == "long":
                    raw_lim = trigger * (1.0 - bps) - tick * 2
                else:
                    raw_lim = trigger * (1.0 + bps) + tick * 2
                limit_px = float(self.exchange.price_to_precision(symbol, raw_lim))
                if side == "long" and limit_px >= trigger:
                    nt = max(trigger - 2 * tick, tick)
                    limit_px = float(self.exchange.price_to_precision(symbol, nt))
                elif side == "short" and limit_px <= trigger:
                    limit_px = float(
                        self.exchange.price_to_precision(symbol, trigger + 2 * tick)
                    )
            else:
                limit_px = trigger
            qty_prec = self.exchange.amount_to_precision(symbol, position_amt)
            qty_str = str(qty_prec).strip()
            try:
                qf = float(qty_str)
            except (TypeError, ValueError):
                qf = 0.0
            if qf <= 0:
                return
            # 币安 USD-M /algoOrder 仅支持 MARK_PRICE、CONTRACT_PRICE，无 LAST_PRICE。
            # 与「用最新价算浮盈」对应：用 CONTRACT_PRICE（合约最近成交价，文档称 CONTRACT_PRICE 触发）。
            # 分档在 monitor 内已用 min(标,新) 浮盈，此处勿再误用未定义枚举致拒单/异常。
            wt = "CONTRACT_PRICE" if self.use_last_price else "MARK_PRICE"
            tp_str = str(trigger)
            lim_str = str(limit_px)

        algo_sig = f"{self.exchange_algo_type}|{tp_str}|{lim_str}"
        prev_sig = self._last_trigger_tp_str.get(symbol)
        if prev_sig == algo_sig:
            return

        if prev_sig is not None:
            parts = prev_sig.split("|")
            if len(parts) >= 2:
                try:
                    prev_tp = float(parts[1])
                    if abs(trigger - prev_tp) / max(abs(prev_tp), 1e-12) * 100.0 < 0.05:
                        return
                except (ValueError, TypeError):
                    pass

        if time.time() - self._last_refresh_time.get(symbol, 0.0) < 1.0:
            return
        self._last_refresh_time[symbol] = time.time()

        if not self._cancel_trailing_algo(symbol):
            logger.warning(
                "移动止盈[%s] %s 撤旧条件单失败，跳过本轮刷新",
                self.account_id, symbol,
            )
            return

        current_price = self._batch_ticker_price(symbol, trigger)

        if self._batch_tickers is not None and (
            (side == "long" and current_price <= trigger)
            or (side == "short" and current_price >= trigger)
        ):
            logger.info(
                "移动止盈[%s] %s 撤单后价格已越过触发线 cur=%.4f trigger=%.4f，跳过本轮重建",
                self.account_id, symbol, current_price, trigger,
            )
            return

        aid: str | None = None
        try:
            with self.trade_lock:
                if self._is_binance():
                    payload: dict[str, Any] = {
                        "algoType": "CONDITIONAL",
                        "symbol": symbol_id,
                        "side": close_side,
                        "workingType": wt,
                        "reduceOnly": "true",
                        "triggerPrice": tp_str,
                        "quantity": qty_str,
                    }
                    if self.exchange_algo_type == "stop_limit":
                        payload["type"] = "STOP"
                        payload["price"] = lim_str
                        payload["timeInForce"] = "GTC"
                    else:
                        payload["type"] = "STOP_MARKET"
                    raw = self.exchange.request(
                        "algoOrder",
                        "fapiPrivate",
                        "POST",
                        payload,
                    )
                    if isinstance(raw, dict):
                        aid = raw.get("algoId") or raw.get("clientAlgoId")
                        if aid is not None:
                            aid = str(aid)
                elif self._is_okx():
                    co_params: dict[str, Any] = {
                        "reduceOnly": True,
                        "triggerPrice": float(tp_str),
                        "triggerPxType": "last" if self.use_last_price else "mark",
                    }
                    amt = float(qty_str)
                    if self.exchange_algo_type == "stop_limit":
                        order = self.exchange.create_order(
                            symbol,
                            "limit",
                            side_ccxt,
                            amt,
                            float(lim_str),
                            {**co_params, "timeInForce": "GTC"},
                        )
                    else:
                        order = self.exchange.create_order(
                            symbol,
                            "market",
                            side_ccxt,
                            amt,
                            None,
                            co_params,
                        )
                    oid = order.get("id")
                    aid = str(oid) if oid is not None else None
            if aid:
                self._algo_trailing_id[symbol] = aid
                self._last_trigger_tp_str[symbol] = algo_sig
            ex_name = "OKX" if self._is_okx() else "币安"
            if self.exchange_algo_type == "stop_limit":
                logger.info(
                    "移动止盈[%s] %s %s 条件限价 trigger=%s limit=%s %s qty=%s",
                    self.account_id,
                    symbol,
                    ex_name,
                    tp_str,
                    lim_str,
                    close_side,
                    qty_str,
                )
            else:
                logger.info(
                    "移动止盈[%s] %s %s 条件市价 trigger=%s %s qty=%s",
                    self.account_id,
                    symbol,
                    ex_name,
                    tp_str,
                    close_side,
                    qty_str,
                )
        except Exception as e:
            logger.error(
                "移动止盈[%s] 同步交易所止损线失败 %s: %s",
                self.account_id,
                symbol,
                e,
            )

    def _blacklisted(self, unified_symbol: str) -> bool:
        u = self._norm_key(unified_symbol)
        for b in self.blacklist:
            if self._norm_key(str(b).strip()) == u:
                return True
        return False

    def _set_status(self, msg: str) -> None:
        if self.status_hook:
            try:
                self.status_hook(msg)
            except Exception:
                pass

    def _notify_worker_loop(self) -> None:
        """后台飞书通知线程：从队列消费，熔断保护。"""
        while not self._notify_worker_stop.is_set():
            try:
                msg = self._notify_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if msg is None:
                break
            if self._notify_failures >= self._notify_max_failures:
                logger.warning(
                    "移动止盈[%s] 飞书熔断已开启，丢弃通知: %s",
                    self.account_id, msg[:60],
                )
                continue
            try:
                headers = {"Content-Type": "application/json"}
                payload = {"msg_type": "text", "content": {"text": msg}}
                resp = requests.post(
                    self.feishu_webhook, json=payload, headers=headers, timeout=5
                )
                if resp.status_code == 200:
                    self._notify_failures = 0
                else:
                    self._notify_failures += 1
                    logger.error(
                        "移动止盈[%s] 飞书通知失败 HTTP %s (累计失败 %d)",
                        self.account_id, resp.status_code, self._notify_failures,
                    )
            except Exception as e:
                self._notify_failures += 1
                logger.error(
                    "移动止盈[%s] 飞书异常: %s (累计失败 %d)",
                    self.account_id, e, self._notify_failures,
                )

    def send_feishu_notification(self, message: str) -> None:
        if not self.feishu_webhook:
            return
        try:
            self._notify_queue.put_nowait(message)
        except queue.Full:
            logger.warning(
                "移动止盈[%s] 飞书通知队列满，丢弃: %s",
                self.account_id, message[:60],
            )

    def fetch_positions(self) -> list[dict[str, Any]]:
        """获取持仓，优化：移除 price_lock 依赖，减少延迟"""
        for attempt in (1, 2):
            try:
                # 优化：持仓获取不需要 price_lock，与价格获取并行
                return self.exchange.fetch_positions()
            except Exception as e:
                if attempt == 1:
                    logger.warning(
                        "移动止盈[%s] 获取持仓失败(%s)，100ms后重试",
                        self.account_id, e,
                    )
                    time.sleep(0.1)  # 优化：减少重试等待时间
                else:
                    logger.error(
                        "移动止盈[%s] 重试获取持仓仍失败: %s",
                        self.account_id, e,
                    )
                    raise

    def _resolve_current_price(self, symbol: str, mark_price: float) -> float:
        """供平仓锚点/兼容旧逻辑；分档与触发请用 _conservative_pnl_pair。"""
        if not self.use_last_price:
            return float(mark_price)
        try:
            with self.price_lock:
                tk = self.exchange.fetch_ticker(symbol)
            last = _fe_float(tk.get("last") or tk.get("close"), 0.0)
            if last > 0:
                return last
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 获取最新价失败 %s: %s", self.account_id, symbol, e
            )
        return float(mark_price)

    @staticmethod
    def _pnl_pct(side: str, entry_price: float, price: float) -> float:
        if entry_price <= 0 or price <= 0:
            return 0.0
        if side == "long":
            return (price - entry_price) / entry_price * 100.0
        return (entry_price - price) / entry_price * 100.0

    def _mark_last_pnl(
        self, symbol: str, side: str, entry_price: float, mark_price: float
    ) -> tuple[float, float, float, float, float]:
        """
        返回 (profit_conservative, p_mark, p_last, mark_px, last_px)：
        用「标记价、最新价」各算一次浮盈。分档/最高/是否触发/是否软件平仓用的「决策浮盈」：
        - use_last_price_only：仅用最新价侧浮盈 p_last；
        - 否则用 min(两浮盈) —— 减少仅「最新价」在 1s~数秒内插针、标记价未跟上时
        误抬 highest、误进档、误挂条件单或下一拍立刻软件平仓。

        若未选最新价，不额外请求 ticker，新=标（避免无谓请求）。
        """
        mark_px = float(mark_price) if mark_price and mark_price > 0 else 0.0
        p_mark = self._pnl_pct(side, entry_price, mark_px) if mark_px > 0 else 0.0
        if not self.use_last_price:
            return p_mark, p_mark, p_mark, mark_px, mark_px
        last_px = mark_px
        try:
            batch = getattr(self, "_batch_tickers", None)
            if batch and isinstance(batch, dict):
                tk = batch.get(symbol, {})
            else:
                tk = {}
            last_raw = _fe_float(tk.get("last") or tk.get("close"), 0.0)
            if last_raw > 0:
                last_px = last_raw
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 获取最新价(分档用)失败 %s: %s",
                self.account_id,
                symbol,
                e,
            )
        p_last = self._pnl_pct(side, entry_price, last_px) if last_px > 0 else 0.0
        if self.use_last_price_only:
            profit_cons = p_last
        else:
            profit_cons = min(p_mark, p_last)
        return profit_cons, p_mark, p_last, mark_px, last_px

    def close_position(
        self,
        symbol: str,
        amount: float,
        side: str,
        *,
        signal_price: float | None = None,
    ) -> bool:
        """
        平仓：market 为纯市价；limit_ioc 为单笔 IOC 限价，不补市价（未全成则返回 False 留仓下轮再试）。
        signal_price：限价锚点，建议必传；限价模式下无有效价则不平仓。
        """
        try:
            if self.close_mode == "market":
                with self.trade_lock:
                    self.exchange.create_order(
                        symbol,
                        "market",
                        side,
                        amount,
                        None,
                        {"reduceOnly": True},
                    )
                mode_txt = "市价"
            else:
                with self.trade_lock:
                    sig = float(signal_price or 0.0)
                    if sig <= 0:
                        tk = self.exchange.fetch_ticker(symbol)
                        sig = float(
                            (tk.get("bid") if side == "sell" else tk.get("ask"))
                            or tk.get("last")
                            or 0.0
                        )
                    if sig <= 0:
                        logger.warning(
                            "移动止盈[%s] %s 限价平仓：无有效参考价，已跳过（不转市价）",
                            self.account_id,
                            symbol,
                        )
                        return False
                    ob = self.exchange.fetch_order_book(symbol, limit=1)
                    bids = ob.get("bids") or []
                    asks = ob.get("asks") or []
                    bid0 = float(bids[0][0]) if bids else sig
                    ask0 = float(asks[0][0]) if asks else sig
                    bps = self.limit_offset_bps / 10000.0
                    if side == "sell":
                        ref = min(sig, bid0)
                        limit_px = ref * (1.0 - bps)
                    else:
                        ref = max(sig, ask0)
                        limit_px = ref * (1.0 + bps)
                    limit_px = float(
                        self.exchange.price_to_precision(symbol, limit_px)
                    )
                    amt = float(
                        self.exchange.amount_to_precision(symbol, amount)
                    )
                    if amt <= 0:
                        raise ValueError("平仓数量精度后为 0")
                    old_timeout = getattr(self.exchange, "timeout", 10000)
                    self.exchange.timeout = 4000
                    try:
                        order = self.exchange.create_order(
                            symbol,
                            "limit",
                            side,
                            amt,
                            limit_px,
                            {"reduceOnly": True, "timeInForce": "IOC"},
                        )
                    finally:
                        self.exchange.timeout = old_timeout
                    filled = _fe_float(order.get("filled"), 0.0)
                    eps = max(1e-12, amt * 1.0e-8)
                    if filled <= 0 or filled + eps < amt:
                        remaining = amt - filled
                        if remaining > 0:
                            logger.warning(
                                "移动止盈[%s] %s 限价IOC 未全部成交 "
                                "filled=%s amt=%s，补市价平剩余=%s",
                                self.account_id,
                                symbol,
                                filled,
                                amt,
                                remaining,
                            )
                            try:
                                self.exchange.create_order(
                                    symbol,
                                    "market",
                                    side,
                                    float(self.exchange.amount_to_precision(symbol, remaining)),
                                    None,
                                    {"reduceOnly": True},
                                )
                            except Exception as e2:
                                logger.error(
                                    "移动止盈[%s] %s 补市价平剩余失败: %s",
                                    self.account_id, symbol, e2,
                                )
                                return False
                        else:
                            return False
                    mode_txt = f"限价IOC(锚≈{sig:.8g})"

            logger.info(
                "移动止盈[%s] 已平仓 %s 数量 %s side=%s 方式=%s",
                self.account_id,
                symbol,
                amount,
                side,
                mode_txt,
            )
            self.send_feishu_notification(
                f"[移动止盈] 账户 {self.account_id} 平仓 {symbol} 数量 {amount} "
                f"side={side} {mode_txt}"
            )
            if self.exchange_sync_stop:
                self._cancel_trailing_algo(symbol)
            self.detected_positions.discard(symbol)
            self.highest_profits.pop(symbol, None)
            self.current_tiers.pop(symbol, None)
            return True
        except Exception as e:
            logger.error(
                "移动止盈[%s] 平仓失败 %s: %s", self.account_id, symbol, e
            )
            self.send_feishu_notification(
                f"[移动止盈] 平仓失败 {symbol}: {e}"
            )
            return False

    def monitor_positions_once(self) -> bool:
        """
        执行一轮监控。返回 True 表示账户上存在可识别的持仓（含黑名单持仓），
        下一轮应使用「有仓」监控间隔；否则为 False，使用空仓轮询间隔。
        """
        _monitor_start = time.time()
        try:
            positions = self.fetch_positions()
        except Exception as e:
            elapsed = time.time() - _monitor_start
            logger.warning(
                "移动止盈[%s] 获取持仓连续失败，跳过本轮(耗时%.1fs): %s",
                self.account_id, elapsed, e,
            )
            return True  # True → 保持高频重试

        active_syms: set[str] = set()
        for position in positions:
            sym, q, _, _, sd = _parse_position_row(position)
            if sym and q > 0 and sd in ("long", "short"):
                active_syms.add(sym)

        # 更新 WebSocket 订阅币种
        self._update_websocket_symbols(list(active_syms))

        if self.exchange_sync_stop:
            for sym in list(self._algo_trailing_id.keys()):
                if sym not in active_syms:
                    self._cancel_trailing_algo(sym)

        lines: list[str] = []
        has_open_for_interval = False
        if not self.use_last_price:
            px_tag = "标记"
        elif self.use_last_price_only:
            px_tag = "仅最新"
        else:
            px_tag = "混合(标+新取保守)"
        use_ex = self.exchange_sync_stop and self._sync_algo_supported()

        self._batch_tickers = None
        self._batch_tickers_ts = 0.0

        if self.use_last_price:
            # 优先使用 WebSocket 数据
            ws_data_available = False
            if self._websocket_feed and self._websocket_feed.is_connected():
                # WebSocket 已连接，直接使用其数据
                ws_prices = {}
                for sym in active_syms:
                    price_data = self._websocket_feed.get_price(sym)
                    if price_data and "last" in price_data:
                        ws_prices[sym] = {
                            "last": price_data["last"],
                            "close": price_data["last"],
                        }
                if ws_prices:
                    self._batch_tickers = ws_prices
                    self._batch_tickers_ts = time.time()
                    ws_data_available = True
                    logger.debug(
                        "移动止盈[%s] 使用 WebSocket 价格数据，%d 个币种",
                        self.account_id,
                        len(ws_prices)
                    )

            # WebSocket 不可用，使用缓存或 REST API
            if not ws_data_available:
                pf = getattr(self, "_price_feed_data", None)
                pf_ts = getattr(self, "_price_feed_data_ts", 0.0)
                current_ts = time.time()

                if pf is not None and isinstance(pf, dict) and (current_ts - pf_ts) < 2.0:
                    self._batch_tickers = pf.copy()
                    self._batch_tickers_ts = pf_ts
                else:
                    # 数据过期或不存在，实时获取
                    for _attempt_tk in (1, 2):
                        try:
                            with self.price_lock:
                                all_tickers = self.exchange.fetch_tickers()
                            if isinstance(all_tickers, dict):
                                self._batch_tickers = all_tickers.copy()
                                self._batch_tickers_ts = time.time()
                            break
                        except Exception as e:
                            self._batch_tickers = None
                            self._batch_tickers_ts = 0.0
                            log_msg = (
                                "移动止盈[%s] 批量获取最新价失败(%s)，200ms后重试"
                                if _attempt_tk == 1
                                else "移动止盈[%s] 批量获取最新价连续失败，改用标记价: %s"
                            )
                            logger.warning(log_msg, self.account_id, e)
                            if _attempt_tk == 1:
                                time.sleep(0.2)

        for position in positions:
            symbol, position_amt, entry_price, mark_price, side = _parse_position_row(
                position
            )
            if not symbol or position_amt <= 0 or entry_price <= 0:
                continue
            if side not in ("long", "short"):
                continue
            has_open_for_interval = True

            if self._blacklisted(symbol):
                if self.exchange_sync_stop:
                    self._cancel_trailing_algo(symbol)
                if symbol not in self.detected_positions:
                    self.send_feishu_notification(
                        f"[移动止盈] 黑名单跳过 {symbol}"
                    )
                    self.detected_positions.add(symbol)
                continue

            mpx = _fe_float(mark_price, 0.0)
            if mpx <= 0:
                continue

            if symbol not in self.detected_positions:
                self.detected_positions.add(symbol)
                self.highest_profits[symbol] = 0.0
                self.current_tiers[symbol] = "无"
                msg = (
                    f"首次检测仓位 {symbol} 数量={position_amt} 开仓={entry_price} "
                    f"方向={side} 计价={px_tag}价"
                )
                logger.info("移动止盈[%s] %s", self.account_id, msg)
                self.send_feishu_notification(f"[移动止盈] {msg}")

            (
                profit_pct,
                p_from_mark,
                p_from_last,
                mark_px,
                _last_px,
            ) = self._mark_last_pnl(symbol, side, entry_price, mpx)

            highest_profit = self.highest_profits.get(symbol, 0.0)
            if profit_pct > highest_profit:
                highest_profit = profit_pct
                self.highest_profits[symbol] = highest_profit

            current_tier = self.current_tiers.get(symbol, "无")
            if highest_profit >= self.second_trail_profit_threshold:
                current_tier = "第二档移动止盈"
            elif highest_profit >= self.first_trail_profit_threshold:
                current_tier = "第一档移动止盈"
            elif highest_profit >= self.low_trail_profit_threshold:
                current_tier = "低档保护止盈"
            else:
                current_tier = "无"
            self.current_tiers[symbol] = current_tier

            # 修复：增加价格数据时间戳和来源信息，便于调试
            data_age = time.time() - self._batch_tickers_ts if self._batch_tickers_ts > 0 else -1

            # 显示数据来源：WebSocket 或 REST
            ws_connected = self._websocket_feed and self._websocket_feed.is_connected()
            source_tag = "WS" if ws_connected else "REST"

            if self.use_last_price and abs(p_from_last - p_from_mark) > 0.01:
                line = (
                    f"{symbol} {side}({px_tag}) 浮盈(决)={profit_pct:.2f}% "
                    f"标={p_from_mark:.2f}% 新={p_from_last:.2f}% "
                    f"最高={highest_profit:.2f}% 档={current_tier} "
                    f"[{source_tag}延迟{data_age:.2f}s]"
                )
            else:
                line = (
                    f"{symbol} {side}({px_tag}) 浮盈={profit_pct:.2f}% "
                    f"最高={highest_profit:.2f}% 档={current_tier} "
                    f"[{source_tag}延迟{data_age:.2f}s]"
                )
            lines.append(line)
            logger.info("移动止盈[%s] %s", self.account_id, line)

            if use_ex:
                tier_ex_th = self._tier_profit_exit_threshold(
                    current_tier, highest_profit
                )
                if tier_ex_th is not None:
                    tier_sig = f"{current_tier}|{tier_ex_th:.6f}"
                    if tier_sig == self._last_tier_sig.get(symbol) and self._algo_trailing_id.get(symbol):
                        pass
                    else:
                        self._last_tier_sig[symbol] = tier_sig
                        if profit_pct <= tier_ex_th + 1.0e-9:
                            logger.info(
                                "移动止盈[%s] %s 分档(交易所) 峰值回撤触发 "
                                "profit=%.4f%% line=%.4f%% 档=%s 交由条件单执行",
                                self.account_id,
                                symbol,
                                profit_pct,
                                tier_ex_th,
                                current_tier,
                            )
                        self._sync_trailing_stop_algo(
                            symbol,
                            side,
                            entry_price,
                            position_amt,
                            current_tier,
                            highest_profit,
                            _profit_pct=profit_pct,
                        )
            elif current_tier == "低档保护止盈":
                # 与交易所路径一致：仅按「峰值×回撤」与底线取 max，不再用当前浮盈贴价
                low_eff = self._tier_profit_exit_threshold(
                    "低档保护止盈", highest_profit
                )
                if low_eff is not None and profit_pct <= low_eff + 1.0e-9:
                    logger.info(
                        "移动止盈[%s] %s 低档保护止盈触发 threshold=%.4f%%",
                        self.account_id,
                        symbol,
                        low_eff,
                    )
                    signal_price = _last_px if self.use_last_price else mark_px
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=signal_price,
                    ):
                        continue

            if not use_ex and current_tier == "第一档移动止盈":
                trail_stop_loss = highest_profit * (1.0 - self.trail_stop_loss_pct)
                if profit_pct <= trail_stop_loss:
                    logger.info(
                        "移动止盈[%s] %s 第一档回撤触发 threshold=%.2f%%",
                        self.account_id,
                        symbol,
                        trail_stop_loss,
                    )
                    signal_price = _last_px if self.use_last_price else mark_px
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=signal_price,
                    ):
                        continue

            if not use_ex and current_tier == "第二档移动止盈":
                trail_stop_loss = highest_profit * (
                    1.0 - self.higher_trail_stop_loss_pct
                )
                if profit_pct <= trail_stop_loss:
                    logger.info(
                        "移动止盈[%s] %s 第二档回撤触发 threshold=%.2f%%",
                        self.account_id,
                        symbol,
                        trail_stop_loss,
                    )
                    signal_price = _last_px if self.use_last_price else mark_px
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=signal_price,
                    ):
                        continue

            if profit_pct <= -self.stop_loss_pct:
                logger.info(
                    "移动止盈[%s] %s 止损触发 %.2f%%",
                    self.account_id,
                    symbol,
                    profit_pct,
                )
                signal_price = _last_px if self.use_last_price else mark_px
                if self.close_position(
                    symbol,
                    position_amt,
                    "sell" if side == "long" else "buy",
                    signal_price=signal_price,
                ):
                    continue

        # 清理外部平仓残留的 stale 状态；完全空仓时全清
        for sym in list(self.highest_profits.keys()):
            if sym not in active_syms:
                self.highest_profits.pop(sym, None)
                self.current_tiers.pop(sym, None)
                self.detected_positions.discard(sym)
                self._last_trigger_tp_str.pop(sym, None)
                self._last_tier_sig.pop(sym, None)

        self._batch_tickers = None
        summary = "; ".join(lines[:12]) if lines else "无持仓或无可解析仓位"
        if len(lines) > 12:
            summary += f" …共{len(lines)}个"
        _elapsed = time.time() - _monitor_start
        if _elapsed >= 0.1:
            logger.info(
                "移动止盈[%s] 监控完成 耗时=%.1fs 持仓=%d",
                self.account_id, round(_elapsed, 1), len(active_syms),
            )
        self._set_status(summary)
        return has_open_for_interval

    def run_loop(
        self,
        stop_event: threading.Event,
        monitor_interval: float,
        idle_no_position_sec: float,
    ) -> None:
        ex_txt = "市价" if self.close_mode == "market" else "限价IOC"
        if self.exchange_sync_stop:
            plat = "OKX" if self._is_okx() else "币安"
            ex_txt = (
                f"{plat}条件限价"
                if self.exchange_algo_type == "stop_limit"
                else f"{plat}条件市价"
            )
        if not self.use_last_price:
            price_mode_txt = "标记价"
        elif self.use_last_price_only:
            price_mode_txt = "仅最新价(分档)"
        else:
            price_mode_txt = "最新价(min标新)"
        logger.info(
            "移动止盈[%s] 循环启动 有仓=%ss 空仓=%ss 计价=%s 执行=%s",
            self.account_id,
            monitor_interval,
            idle_no_position_sec,
            price_mode_txt,
            ex_txt,
        )
        if self.use_last_price:
            self._price_feed_stop.clear()
            self._price_feed_data = None
            price_feed_interval = max(3.0, monitor_interval)
            self._price_feed_thread = threading.Thread(
                target=self._price_feed_loop,
                args=(price_feed_interval,),
                daemon=True,
                name=f"price-{self.account_id}",
            )
            self._price_feed_thread.start()
            logger.info(
                "移动止盈[%s] 独立行情线程已启动（%.3fs间隔）",
                self.account_id, price_feed_interval,
            )
            # WebSocket 会在首次有持仓时自动启动
        try:
            while not stop_event.is_set():
                _cycle_start = time.time()
                wait_sec = float(monitor_interval)
                try:
                    has_open = self.monitor_positions_once()
                    wait_sec = (
                        float(monitor_interval)
                        if has_open
                        else float(idle_no_position_sec)
                    )
                except Exception as e:
                    err = f"本轮异常: {e}"
                    logger.exception("移动止盈[%s] %s", self.account_id, err)
                    self._set_status(err)
                    self.send_feishu_notification(f"[移动止盈] {err}")
                _cycle_elapsed = time.time() - _cycle_start
                if _cycle_elapsed >= 0.1:
                    logger.info(
                        "移动止盈[%s] 循环耗时=%.1fs(监控+等待) 下一轮等待=%.1fs",
                        self.account_id, round(_cycle_elapsed, 1), wait_sec,
                    )
                if stop_event.wait(timeout=wait_sec):
                    break
        finally:
            self._price_feed_stop.set()
            if self._price_feed_thread and self._price_feed_thread.is_alive():
                self._price_feed_thread.join(timeout=2)
            self._notify_worker_stop.set()
            self._notify_worker.join(timeout=3)
            if self.exchange_sync_stop:
                for sym in list(self._algo_trailing_id.keys()):
                    self._cancel_trailing_algo(sym)
            logger.info("移动止盈[%s] 循环结束", self.account_id)
