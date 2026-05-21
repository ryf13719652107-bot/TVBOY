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
        third_trail_profit_threshold: float = 2.1,
        third_trail_stop_loss_pct: float = 0.5,
        feishu_webhook: str | None = None,
        blacklist: set[str] | None = None,
        status_hook: Callable[[str], None] | None = None,
        use_last_price: bool = False,
        use_last_price_only: bool = False,
        close_mode: str = "market",
        limit_offset_bps: float = 25.0,
        low_offset_bps: float | None = None,
        first_offset_bps: float | None = None,
        second_offset_bps: float | None = None,
        third_offset_bps: float | None = None,
        trailing_exec: str = "signal",
        exchange_algo_type: str = "stop_market",
        testnet: bool = False,
        close_log_hook: Callable[[dict[str, Any]], None] | None = None,
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
        # 全局基线 bps（仅在没传分档 bps 时使用）
        self.limit_offset_bps = max(0.0, float(limit_offset_bps))
        # 各档独立 limit_offset_bps（None 时回退到 limit_offset_bps）
        self.low_offset_bps = (
            max(0.0, float(low_offset_bps)) if low_offset_bps is not None
            else self.limit_offset_bps
        )
        self.first_offset_bps = (
            max(0.0, float(first_offset_bps)) if first_offset_bps is not None
            else self.limit_offset_bps
        )
        self.second_offset_bps = (
            max(0.0, float(second_offset_bps)) if second_offset_bps is not None
            else self.limit_offset_bps
        )
        self.third_offset_bps = (
            max(0.0, float(third_offset_bps)) if third_offset_bps is not None
            else self.limit_offset_bps
        )
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
        self.third_trail_stop_loss_pct = float(third_trail_stop_loss_pct)
        self.low_trail_profit_threshold = float(low_trail_profit_threshold)
        self.first_trail_profit_threshold = float(first_trail_profit_threshold)
        self.second_trail_profit_threshold = float(second_trail_profit_threshold)
        self.third_trail_profit_threshold = float(third_trail_profit_threshold)
        self.feishu_webhook = (feishu_webhook or "").strip() or None
        self.blacklist = blacklist or set()
        self.status_hook = status_hook
        self.close_log_hook = close_log_hook

        self.highest_profits: dict[str, float] = {}
        self.current_tiers: dict[str, str] = {}
        self.detected_positions: set[str] = set()
        self._last_refresh_time: dict[str, float] = {}
        self._last_tier_sig: dict[str, str] = {}
        # 条件单触发后的兜底跟踪：symbol → 首次检测到"应当触发"的时刻
        self._post_trigger_grace: dict[str, float] = {}
        # 触发未成交后的市价兜底等待秒数（IOC 失败后等待此时间再强制平仓）
        self._post_trigger_grace_sec: float = 3.0
        # 价格穿透限价多少百分比时立即市价兜底（不等 grace 秒）
        # 例如 0.1 表示现价低于限价 0.1% → 限价单不可能成交，立即市价
        self._post_trigger_breach_pct: float = 0.1

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
        self._last_direct_fetch_ts: float = 0.0  # WS直连模式下直接fetch_tickers的冷却时间戳

        # WebSocket 价格订阅
        self._websocket_feed: Any | None = None
        self._use_websocket = True  # 是否使用 WebSocket
        self._subscribed_symbols: set[str] = set()
        # WS 重订阅时 simplified key → ccxt 统一格式 反向映射（_on_websocket_price_update 用）
        self._ws_key_to_unified: dict[str, str] = {}
        # 平滑切换标志：True 表示正在双连接切换中，避免重入
        self._ws_switching: bool = False

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

    @staticmethod
    def _ws_simplified_key(symbol: str) -> str:
        """与 WebSocket 内部 normalize 一致：'OPG/USDT:USDT' → 'OPGUSDT'。
        用于把 WS 回调时收到的 simplified key 反查回 ccxt 统一格式。"""
        return (
            symbol.upper()
            .replace('/USDT', 'USDT').replace(':USDT', '')
            .replace('/USD', 'USD').replace(':USD', '')
        )

    def _pf_lookup(self, pf: dict, sym: str) -> dict | None:
        """在 _price_feed_data 中查找指定 symbol，同时兼容 ccxt 格式与 simplified key。

        WS 重订阅瞬间，旧数据可能用旧版本 simplified key，新代码用 ccxt key，
        先按 ccxt 格式查（新数据），再回退到 simplified（向后兼容）。
        """
        if not pf:
            return None
        if sym in pf:
            return pf[sym]
        simp = self._ws_simplified_key(sym)
        if simp in pf:
            return pf[simp]
        return None

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
                # 建立 simplified key → ccxt 统一格式 的反向映射
                # 让 WS 回调写入 _price_feed_data 时使用与 active_syms 一致的 key，
                # 否则 fallback 路径 `if sym in pf` 永远不命中，pf 兜底失效，
                # 导致 WS 重订阅瞬间被迫走 fetch_tickers 拉到陈旧 last 价。
                self._ws_key_to_unified = {
                    self._ws_simplified_key(s): s for s in symbols
                }
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
        """WebSocket 价格更新回调

        WS 内部传入的 symbol 是 simplified key（如 'OPGUSDT'），
        但其他模块（fallback 兜底等）使用的是 ccxt 统一格式（如 'OPG/USDT:USDT'）。
        通过 _ws_key_to_unified 反向映射，让 _price_feed_data 始终用 ccxt key，
        避免 key 不匹配导致 fallback 路径永远不命中。
        """
        unified = symbol
        mapping = getattr(self, "_ws_key_to_unified", None)
        if mapping and symbol in mapping:
            unified = mapping[symbol]

        # 将 WebSocket 数据格式转换为与 fetch_tickers 一致
        ticker_data = {}

        if "last" in price_data:
            ticker_data["last"] = price_data["last"]
            ticker_data["close"] = price_data["last"]

        if "mark" in price_data:
            # 币安 WebSocket markPrice 流提供标记价格
            # 用于计算浮盈和触发移动止盈
            ticker_data["mark"] = price_data["mark"]

        # 透传 WS 推送时间戳（per-symbol），用于精准计算 data_age
        for k in ("received_at_last", "received_at_mark", "received_at"):
            v = price_data.get(k)
            if v:
                ticker_data[k] = v

        if ticker_data:
            with self.price_lock:
                if self._price_feed_data is None:
                    self._price_feed_data = {}
                self._price_feed_data[unified] = ticker_data
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
        """更新 WebSocket 订阅的币种列表（平滑切换，避免空窗期）"""
        if not self._use_websocket:
            return

        # 正在平滑切换中，本次跳过；下一轮 monitor 会再次检测
        if getattr(self, "_ws_switching", False):
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

        # 持仓清空，关闭 WebSocket（无需平滑切换）
        if self._websocket_feed and not current_symbols:
            for sym in list(self._subscribed_symbols):
                self.highest_profits.pop(sym, None)
                self.current_tiers.pop(sym, None)
                self.detected_positions.discard(sym)
            logger.info(
                "移动止盈[%s] 持仓全部清空，关闭 WebSocket",
                self.account_id
            )
            self._stop_websocket_feed()
            return

        # WebSocket 已启动，检查币种变化 → 平滑切换
        if self._websocket_feed and current_symbols != self._subscribed_symbols:
            # 清理不再持仓的币种数据，防止内存泄漏
            removed_symbols = self._subscribed_symbols - current_symbols
            for sym in removed_symbols:
                self.highest_profits.pop(sym, None)
                self.current_tiers.pop(sym, None)
                self.detected_positions.discard(sym)
                logger.info("移动止盈[%s] 清理已平仓币种数据: %s", self.account_id, sym)

            self._smooth_switch_websocket(list(current_symbols))

    def _smooth_switch_websocket(self, new_symbols: list[str]):
        """平滑切换 WebSocket：先启动新连接、等首条数据后再关闭旧连接。

        老方案 stop()+start() 会产生 0.3~1s 的"WS 空窗期"，
        在此期间 monitor 走 fetch_tickers fallback，
        OKX REST tickers 端点对小币种存在陈旧/缓存延迟问题，
        曾导致 OPG 拉到错误 last 价 → 错误进档 → 错误平仓。

        本方案双连接 0.5~1s：
        1. 启动新 WS（旧 WS 仍主导，monitor 继续从旧 WS 取数据）
        2. 同步更新 _ws_key_to_unified 映射（覆盖新增币种）
        3. 后台线程等新 WS 收到首条数据（或 5s 超时）
        4. 原子替换 self._websocket_feed → 新 WS
        5. 关闭旧 WS
        """
        old_feed = self._websocket_feed
        new_feed = None
        try:
            from websocket_price_feed import create_websocket_feed

            exchange_id = getattr(self.exchange, "id", "")
            new_feed = create_websocket_feed(
                exchange_id=exchange_id,
                symbols=new_symbols,
                on_price_update=self._on_websocket_price_update,
                testnet=self.testnet,
            )
            if not new_feed:
                logger.warning(
                    "移动止盈[%s] 平滑切换：新 WebSocket 创建失败，降级为 stop+start",
                    self.account_id,
                )
                self._stop_websocket_feed()
                self._start_websocket_feed(new_symbols)
                return

            # 提前更新映射（旧 WS 推送的币种仍在新映射中，新增币种也在）
            # 旧 WS 不会推送 new_symbols 中独有的币种，所以双连接期间映射对两边都正确。
            self._ws_key_to_unified = {
                self._ws_simplified_key(s): s for s in new_symbols
            }
            new_feed.start()
            logger.info(
                "移动止盈[%s] 平滑切换：新 WebSocket 已启动(%d 币种)，"
                "旧 WS 继续工作，等待首条数据后切换",
                self.account_id, len(new_symbols),
            )
        except Exception as e:
            logger.error(
                "移动止盈[%s] 平滑切换：启动新 WebSocket 失败: %s，降级为 stop+start",
                self.account_id, e,
            )
            try:
                if new_feed:
                    new_feed.stop()
            except Exception:
                pass
            self._stop_websocket_feed()
            self._start_websocket_feed(new_symbols)
            return

        self._ws_switching = True

        def _switcher():
            try:
                deadline = time.time() + 5.0  # 5 秒超时强制切换
                ready = False
                while time.time() < deadline:
                    if new_feed.has_received_data():
                        ready = True
                        break
                    time.sleep(0.05)

                if ready:
                    elapsed_ms = (5.0 - max(0.0, deadline - time.time())) * 1000.0
                    logger.info(
                        "移动止盈[%s] 平滑切换：新 WS 收到首条数据(耗时%.0fms)，原子切换",
                        self.account_id, elapsed_ms,
                    )
                else:
                    logger.warning(
                        "移动止盈[%s] 平滑切换：5s 内新 WS 未收到数据，强制切换",
                        self.account_id,
                    )

                # 原子替换主引用（GIL 保证 monitor 读到的要么旧、要么新，无中间态）
                self._websocket_feed = new_feed
                self._subscribed_symbols = set(new_symbols)

                # 关闭旧 WS
                if old_feed:
                    try:
                        old_feed.stop()
                        logger.info(
                            "移动止盈[%s] 平滑切换完成，旧 WebSocket 已关闭",
                            self.account_id,
                        )
                    except Exception as e:
                        logger.warning(
                            "移动止盈[%s] 平滑切换：关闭旧 WS 失败: %s",
                            self.account_id, e,
                        )
            finally:
                self._ws_switching = False

        threading.Thread(
            target=_switcher,
            daemon=True,
            name=f"ws-smooth-switch-{self.account_id}",
        ).start()

    def _price_feed_loop(self, interval: float) -> None:
        """独立行情线程：每 interval 秒按持仓拉一次最新价，存入 _price_feed_data。
        WebSocket 正常时闲置（每 5s 检查一次）；WebSocket 失效时降级到 REST API，
        间隔不低于 3.0s 以避免触发交易所 API 限频。"""
        consecutive_errors = 0
        base_interval = max(3.0, interval)
        while not self._price_feed_stop.is_set():
            start_time = time.time()

            if self._websocket_feed and self._websocket_feed.is_connected() and self._websocket_feed.has_received_data():
                if self._price_feed_stop.wait(timeout=5.0):
                    break
                continue

            # 仅拉取当前订阅币种，减少流量与 weight
            target_syms = list(self._subscribed_symbols) if self._subscribed_symbols else []
            if not target_syms:
                if self._price_feed_stop.wait(timeout=5.0):
                    break
                continue

            try:
                tickers = self._fetch_tickers_for(target_syms)
                if tickers:
                    with self.price_lock:
                        self._price_feed_data = tickers
                        self._price_feed_data_ts = time.time()
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
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
        if current_tier == "第三档移动止盈":
            return highest_profit * (1.0 - self.third_trail_stop_loss_pct)
        return None

    def _tier_offset_bps(self, current_tier: str) -> float:
        """根据档位返回限价滑点 bps（仅 stop_limit 模式使用）。"""
        if current_tier == "低档保护止盈":
            return self.low_offset_bps
        if current_tier == "第一档移动止盈":
            return self.first_offset_bps
        if current_tier == "第二档移动止盈":
            return self.second_offset_bps
        if current_tier == "第三档移动止盈":
            return self.third_offset_bps
        return self.limit_offset_bps

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
                # 按档位选择对应的 bps（低5/一15/二20/三30）
                tier_bps_value = self._tier_offset_bps(current_tier)
                bps = tier_bps_value / 10000.0
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
                        # IOC：触发后立即按限价成交，未成交则取消（避免 GTC 卡死）
                        # 配合 _check_post_trigger_safety 在 IOC 失败后市价兜底
                        payload["timeInForce"] = "IOC"
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
                        # OKX 计划委托 (ordType=trigger) 不接受 timeInForce 参数，
                        # 触发后默认 GTC 行为；如果限价不成交，由
                        # _check_post_trigger_safety 在 grace 期后市价兜底
                        order = self.exchange.create_order(
                            symbol,
                            "limit",
                            side_ccxt,
                            amt,
                            float(lim_str),
                            co_params,
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

    def _check_post_trigger_safety(
        self,
        symbol: str,
        side: str,
        qty: float,
        cur_last: float,
        cur_mark: float,
        profit_pct: float,
        tier_ex_th: float | None,
        current_tier: str | None = None,
    ) -> bool:
        """条件单触发后的成交检查：双条件兜底。

        条件 A（价格穿透）：现价已经穿透限价 N%（默认 0.1%）→ 立即市价
            原理：IOC 失败的本质是"现价已低于（多）/高于（空）限价"，
            此时限价单物理上不可能成交，无需等待 grace 计时
        条件 B（计时兜底）：触发后超过 grace_sec（默认 3s）持仓仍存在 → 市价
            兜底中的兜底，处理网络/撮合异常等情况

        返回 True 表示已强制平仓。
        """
        if not self.exchange_sync_stop or tier_ex_th is None:
            self._post_trigger_grace.pop(symbol, None)
            return False
        if symbol not in self._algo_trailing_id:
            self._post_trigger_grace.pop(symbol, None)
            return False

        triggered_now = profit_pct <= tier_ex_th + 1.0e-9
        if not triggered_now:
            # 行情回升，未触发，清空 grace 状态
            self._post_trigger_grace.pop(symbol, None)
            return False

        # 解析上次挂单的限价（_last_trigger_tp_str 格式：'stop_limit|trigger|limit'）
        sig = self._last_trigger_tp_str.get(symbol, "")
        limit_px = 0.0
        if sig:
            parts = sig.split("|")
            if len(parts) >= 3:
                try:
                    limit_px = float(parts[2])
                except (ValueError, TypeError):
                    limit_px = 0.0

        # 条件 A：价格穿透
        breach_pct = self._post_trigger_breach_pct / 100.0
        ref_price = cur_last if self.use_last_price else cur_mark
        breach_triggered = False
        if limit_px > 0 and ref_price > 0 and breach_pct > 0:
            if side == "long":
                # 平多挂单是 SELL @ limit_px；现价低于 limit_px*(1-breach) 即穿透
                breach_triggered = ref_price < limit_px * (1.0 - breach_pct)
            else:
                # 平空挂单是 BUY @ limit_px；现价高于 limit_px*(1+breach) 即穿透
                breach_triggered = ref_price > limit_px * (1.0 + breach_pct)

        # 条件 B：计时兜底
        now = time.time()
        grace_start = self._post_trigger_grace.get(symbol)
        if grace_start is None:
            self._post_trigger_grace[symbol] = now
            grace_start = now
        elapsed = now - grace_start
        time_triggered = elapsed >= self._post_trigger_grace_sec

        if not breach_triggered and not time_triggered:
            return False

        reason = "价格穿透" if breach_triggered else f"超时{elapsed:.1f}s"
        logger.warning(
            "移动止盈[%s] %s 条件单触发后 %s 强制市价平仓 "
            "(profit=%.3f%% line=%.3f%% 现价=%.6f 限价=%.6f)",
            self.account_id, symbol, reason,
            profit_pct, tier_ex_th, ref_price, limit_px,
        )
        signal_price = cur_last if self.use_last_price else cur_mark
        closed = self.close_position(
            symbol,
            qty,
            "sell" if side == "long" else "buy",
            signal_price=signal_price,
            current_tier=current_tier,
        )
        if closed:
            self._post_trigger_grace.pop(symbol, None)
        return closed

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

    def _fetch_tickers_for(self, symbols: list[str]) -> dict[str, dict[str, Any]] | None:
        """按持仓的少量 symbol 拉取 ticker，减少流量与权重消耗。

        Binance: fetch_tickers(symbols=[...]) 走 GET /fapi/v1/ticker/price 批量
        OKX: fetch_tickers(symbols=[...]) ccxt 内部按 instId 批量。
        若交易所不支持按 symbols 批量（极少数情况），fall back 到全市场 fetch_tickers。
        """
        if not symbols:
            return None
        for attempt in (1, 2):
            try:
                with self.price_lock:
                    try:
                        all_tickers = self.exchange.fetch_tickers(symbols)
                    except (TypeError, NotImplementedError):
                        # 老版 ccxt 或个别交易所不支持 symbols 入参，退回全市场
                        all_tickers = self.exchange.fetch_tickers()
                if not isinstance(all_tickers, dict):
                    return None
                out: dict[str, dict[str, Any]] = {}
                for sym in symbols:
                    if sym in all_tickers:
                        t = all_tickers[sym]
                        last = t.get("last") or t.get("close") or t.get("mark")
                        mark = t.get("mark") or t.get("last") or t.get("close")
                        if last:
                            out[sym] = {
                                "last": last,
                                "close": last,
                                "mark": mark or last,
                            }
                return out or None
            except Exception as e:
                if attempt == 1:
                    logger.warning(
                        "移动止盈[%s] 按需 fetch_tickers(%d个) 失败(%s)，重试",
                        self.account_id, len(symbols), e,
                    )
                    time.sleep(0.15)
                else:
                    logger.warning(
                        "移动止盈[%s] 按需 fetch_tickers 连续失败: %s",
                        self.account_id, e,
                    )
                    return None
        return None

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
        current_tier: str | None = None,
    ) -> bool:
        """
        平仓：market 为纯市价；limit_ioc 为单笔 IOC 限价，不补市价（未全成则返回 False 留仓下轮再试）。
        signal_price：限价锚点，建议必传；限价模式下无有效价则不平仓。
        current_tier：当前档位（"低档保护止盈"/"第一档移动止盈"/"第二档移动止盈"/"第三档移动止盈"/"无"），
            用于在 limit_ioc 模式按档位选择限价滑点 bps；未传或 "无" 时回退到全局 limit_offset_bps。
        """
        last_order: dict[str, Any] | None = None
        try:
            if self.close_mode == "market":
                with self.trade_lock:
                    last_order = self.exchange.create_order(
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
                    # 按档位选择 bps；未传档位时使用全局兜底
                    tier_bps = self._tier_offset_bps(current_tier or "无")
                    bps = tier_bps / 10000.0
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
                        last_order = self.exchange.create_order(
                            symbol,
                            "limit",
                            side,
                            amt,
                            limit_px,
                            {"reduceOnly": True, "timeInForce": "IOC"},
                        )
                    finally:
                        self.exchange.timeout = old_timeout
                    filled = _fe_float(last_order.get("filled"), 0.0)
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
                                last_order = self.exchange.create_order(
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

            if last_order and self.close_log_hook:
                try:
                    self.close_log_hook(
                        {
                            "order": last_order,
                            "symbol": symbol,
                            "side": side,
                            "current_tier": current_tier,
                        }
                    )
                except Exception as hook_err:
                    logger.warning(
                        "移动止盈[%s] 交易记录回调失败 %s: %s",
                        self.account_id,
                        symbol,
                        hook_err,
                    )

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
        # 记录每个 symbol 的真实数据接收时刻（来自 WS 推送）
        self._batch_tickers_received_at: dict[str, float] = {}

        # 统一判断 WebSocket TCP 连接状态，在整个方法内都可用
        ws_connected = self._websocket_feed and self._websocket_feed.is_connected()
        # 区分 TCP 连接与真实数据到达（TCP 连通但无数据时仍视为 REST 模式）
        ws_has_real_data = ws_connected and self._websocket_feed.has_received_data()

        # 仅在「启用最新价」且「确有持仓」时才走价格获取流程；空仓直接跳过，
        # 避免对空 active_syms 调用 _fetch_tickers_for 误触发"获取失败"日志。
        if self.use_last_price and active_syms:
            # 优先使用 WebSocket 数据
            ws_data_available = False
            if ws_connected:
                ws_prices = {}
                for sym in active_syms:
                    price_data = self._websocket_feed.get_price(sym)
                    if price_data:
                        last = price_data.get("last")
                        mark = price_data.get("mark")
                        # last 没收到时用 mark 兜底，反之同理（WS 内部已做兜底，这里再保险）
                        last = last or mark
                        mark = mark or last
                        if last:
                            ws_prices[sym] = {
                                "last": last,
                                "close": last,
                                "mark": mark,
                            }
                            # 优先用 last 的接收时间（来自 aggTrade），其次 mark，再次整体
                            recv = (
                                price_data.get("received_at_last")
                                or price_data.get("received_at_mark")
                                or price_data.get("received_at")
                                or 0.0
                            )
                            self._batch_tickers_received_at[sym] = recv
                if ws_prices:
                    self._batch_tickers = ws_prices
                    self._batch_tickers_ts = time.time()
                    ws_data_available = True

            # WebSocket 已连接但无直接数据，按需 fetch_tickers 获取最新价
            # 即便 WS 仅 TCP 连通，也保留 1.0s 最小冷却避免触发交易所限频
            if ws_connected and not ws_data_available:
                current_ts = time.time()
                cooldown = 1.5 if ws_has_real_data else 1.0
                if current_ts - self._last_direct_fetch_ts >= cooldown:
                    fetched = self._fetch_tickers_for(list(active_syms))
                    if fetched:
                        self._batch_tickers = fetched
                        self._batch_tickers_ts = time.time()
                        self._last_direct_fetch_ts = self._batch_tickers_ts
                        for sym in fetched:
                            self._batch_tickers_received_at[sym] = self._batch_tickers_ts
                        ws_data_available = True

                # 直接 fetch 失败或冷却中，用 _price_feed_data 兜底
                if not ws_data_available:
                    pf = getattr(self, "_price_feed_data", None)
                    pf_ts = getattr(self, "_price_feed_data_ts", 0.0)
                    if pf is not None and isinstance(pf, dict) and (current_ts - pf_ts) < 5.0:
                        ws_prices = {}
                        for sym in active_syms:
                            entry = self._pf_lookup(pf, sym)
                            if entry:
                                ws_prices[sym] = entry
                                # 优先用 per-symbol 推送时间戳，缺失则用整体 pf_ts
                                recv = (
                                    entry.get("received_at_last")
                                    or entry.get("received_at_mark")
                                    or entry.get("received_at")
                                    or pf_ts
                                )
                                self._batch_tickers_received_at[sym] = recv
                        if ws_prices:
                            self._batch_tickers = ws_prices
                            self._batch_tickers_ts = pf_ts
                            ws_data_available = True

            # WebSocket 不可用或以上路径均无数据，使用 REST API（按持仓按需拉取）
            if not ws_data_available:
                pf = getattr(self, "_price_feed_data", None)
                pf_ts = getattr(self, "_price_feed_data_ts", 0.0)
                current_ts = time.time()

                # 优先用未过期的独立行情线程数据（5s 内）
                pf_used = False
                if pf is not None and isinstance(pf, dict) and (current_ts - pf_ts) < 5.0:
                    sub: dict[str, dict[str, Any]] = {}
                    for sym in active_syms:
                        entry = self._pf_lookup(pf, sym)
                        if entry:
                            sub[sym] = entry
                    if sub:
                        self._batch_tickers = sub
                        self._batch_tickers_ts = pf_ts
                        for sym in sub:
                            entry = sub[sym]
                            recv = (
                                entry.get("received_at_last")
                                or entry.get("received_at_mark")
                                or entry.get("received_at")
                                or pf_ts
                            )
                            self._batch_tickers_received_at[sym] = recv
                        pf_used = True

                # _price_feed_data 过期或缺失：按需 fetch_tickers
                # 加冷却保护：WS 长期失效时避免 0.3s 一次的循环里反复打 REST
                if not pf_used:
                    cooldown = 1.0
                    if current_ts - self._last_direct_fetch_ts >= cooldown:
                        for _attempt_tk in (1, 2):
                            fetched = self._fetch_tickers_for(list(active_syms))
                            if fetched:
                                self._batch_tickers = fetched
                                self._batch_tickers_ts = time.time()
                                self._last_direct_fetch_ts = self._batch_tickers_ts
                                for sym in fetched:
                                    self._batch_tickers_received_at[sym] = self._batch_tickers_ts
                                break
                            if _attempt_tk == 1:
                                logger.warning(
                                    "移动止盈[%s] 按需获取最新价失败，200ms后重试",
                                    self.account_id,
                                )
                                time.sleep(0.2)
                            else:
                                logger.warning(
                                    "移动止盈[%s] 按需获取最新价连续失败，改用标记价",
                                    self.account_id,
                                )
                                self._batch_tickers = None
                                self._batch_tickers_ts = 0.0
                    else:
                        # 冷却期：尽量用过期的 _price_feed_data 兜底（虽然超过 5s 但聊胜于无）
                        if pf is not None and isinstance(pf, dict):
                            sub = {sym: pf[sym] for sym in active_syms if sym in pf}
                            if sub:
                                self._batch_tickers = sub
                                self._batch_tickers_ts = pf_ts
                                for sym in sub:
                                    self._batch_tickers_received_at[sym] = pf_ts

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
            if highest_profit >= self.third_trail_profit_threshold:
                current_tier = "第三档移动止盈"
            elif highest_profit >= self.second_trail_profit_threshold:
                current_tier = "第二档移动止盈"
            elif highest_profit >= self.first_trail_profit_threshold:
                current_tier = "第一档移动止盈"
            elif highest_profit >= self.low_trail_profit_threshold:
                current_tier = "低档保护止盈"
            else:
                current_tier = "无"
            self.current_tiers[symbol] = current_tier

            # 数据延迟：优先用 per-symbol 真实接收时间（来自 WS 推送的 received_at）
            recv_map = getattr(self, "_batch_tickers_received_at", None)
            recv_at = recv_map.get(symbol, 0.0) if recv_map else 0.0
            if recv_at <= 0 and self._batch_tickers_ts > 0:
                recv_at = self._batch_tickers_ts
            data_age = time.time() - recv_at if recv_at > 0 else -1

            # 显示数据来源：WS=真实WebSocket推送数据，REST=REST API回落
            source_tag = "WS" if ws_has_real_data else "REST"

            if self.use_last_price:
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

                    # IOC 限价条件单触发后兜底检查：未成交 N 秒后强制市价平仓
                    if self.exchange_algo_type == "stop_limit":
                        if self._check_post_trigger_safety(
                            symbol, side, position_amt,
                            _last_px, mark_px, profit_pct, tier_ex_th,
                            current_tier=current_tier,
                        ):
                            continue
                else:
                    self._post_trigger_grace.pop(symbol, None)
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
                        current_tier=current_tier,
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
                        current_tier=current_tier,
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
                        current_tier=current_tier,
                    ):
                        continue

            if not use_ex and current_tier == "第三档移动止盈":
                trail_stop_loss = highest_profit * (
                    1.0 - self.third_trail_stop_loss_pct
                )
                if profit_pct <= trail_stop_loss:
                    logger.info(
                        "移动止盈[%s] %s 第三档回撤触发 threshold=%.2f%%",
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
                        current_tier=current_tier,
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
                # 止损不属于分档止盈，传 current_tier 让其按当前档位 bps；
                # 若当前是"无"档（持仓刚开亏损）则回退到全局 limit_offset_bps
                if self.close_position(
                    symbol,
                    position_amt,
                    "sell" if side == "long" else "buy",
                    signal_price=signal_price,
                    current_tier=current_tier,
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
                self._post_trigger_grace.pop(sym, None)

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
