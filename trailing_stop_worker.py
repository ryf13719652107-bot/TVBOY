# -*- coding: utf-8 -*-
"""币安 / OKX U 本位永续：多标的移动止盈 / 回撤止盈（供 TV 机器人后台线程调用）。"""
from __future__ import annotations

import logging
import threading
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
        close_mode: str = "market",
        limit_offset_bps: float = 25.0,
        trailing_exec: str = "signal",
        exchange_algo_type: str = "stop_market",
    ) -> None:
        self.exchange = exchange
        self.account_id = account_id
        self.trade_lock = trade_lock
        self._norm = normalize_symbol_fn
        self.use_last_price = bool(use_last_price)
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

    def _cancel_trailing_algo(self, symbol: str) -> None:
        aid = self._algo_trailing_id.pop(symbol, None)
        self._last_trigger_tp_str.pop(symbol, None)
        if not aid:
            return
        try:
            with self.trade_lock:
                if self._is_binance():
                    self.exchange.request(
                        "algoOrder", "fapiPrivate", "DELETE", {"algoId": aid}
                    )
                elif self._is_okx():
                    self.exchange.cancel_order(str(aid), symbol, {"trigger": True})
                else:
                    return
            logger.info("移动止盈[%s] 已撤条件单 %s id=%s", self.account_id, symbol, aid)
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 撤移动止盈条件单失败 %s: %s", self.account_id, symbol, e
            )

    def _tier_profit_exit_threshold(
        self, current_tier: str, highest_profit: float
    ) -> float | None:
        """
        与交易所条件单、非交易所轮询共用的「自峰值回撤后」浮盈比例阈值(%)，仅与 highest 有关。
        未进入任一档 返回 None（调用方应已把「无」挡在门外）。
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

        if side == "long":
            trigger = entry_price * (1.0 + profit_cutoff / 100.0)
            close_side = "SELL"
        else:
            trigger = entry_price * (1.0 - profit_cutoff / 100.0)
            close_side = "BUY"
        side_ccxt = close_side.lower()

        with self.trade_lock:
            self.exchange.load_markets()
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
            wt = "CONTRACT_PRICE" if self.use_last_price else "MARK_PRICE"
            tp_str = str(trigger)
            lim_str = str(limit_px)

        algo_sig = f"{self.exchange_algo_type}|{tp_str}|{lim_str}"
        prev_sig = self._last_trigger_tp_str.get(symbol)
        if prev_sig == algo_sig:
            return

        self._cancel_trailing_algo(symbol)
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
                        "triggerPxType": "last"
                        if self.use_last_price
                        else "mark",
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

    def send_feishu_notification(self, message: str) -> None:
        if not self.feishu_webhook:
            return
        try:
            headers = {"Content-Type": "application/json"}
            payload = {"msg_type": "text", "content": {"text": message}}
            response = requests.post(
                self.feishu_webhook, json=payload, headers=headers, timeout=10
            )
            if response.status_code == 200:
                logger.info("移动止盈[%s] 飞书通知发送成功", self.account_id)
            else:
                logger.error(
                    "移动止盈[%s] 飞书通知失败 HTTP %s",
                    self.account_id,
                    response.status_code,
                )
        except Exception as e:
            logger.error("移动止盈[%s] 飞书异常: %s", self.account_id, e)

    def fetch_positions(self) -> list[dict[str, Any]]:
        with self.trade_lock:
            return self.exchange.fetch_positions()

    def _resolve_current_price(self, symbol: str, mark_price: float) -> float:
        """浮盈计算用价：标记价或最新价（失败则回退标记价）。"""
        if not self.use_last_price:
            return float(mark_price)
        try:
            with self.trade_lock:
                tk = self.exchange.fetch_ticker(symbol)
            last = _fe_float(tk.get("last") or tk.get("close"), 0.0)
            if last > 0:
                return last
        except Exception as e:
            logger.warning(
                "移动止盈[%s] 获取最新价失败 %s: %s", self.account_id, symbol, e
            )
        return float(mark_price)

    def close_position(
        self,
        symbol: str,
        amount: float,
        side: str,
        *,
        signal_price: float | None = None,
    ) -> bool:
        """
        平仓：market 或 limit_ioc（IOC 限价优先吃盘口，未成交量再市价 reduceOnly）。
        signal_price：触发平仓时用于计价/限价锚点的价格（与浮盈计算一致）；限价模式建议必传。
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
                    ob = self.exchange.fetch_order_book(symbol, limit=5)
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
                    order = self.exchange.create_order(
                        symbol,
                        "limit",
                        side,
                        amt,
                        limit_px,
                        {"reduceOnly": True, "timeInForce": "IOC"},
                    )
                    filled = _fe_float(order.get("filled"), 0.0)
                    remaining = float(
                        self.exchange.amount_to_precision(
                            symbol, max(0.0, amt - filled)
                        )
                    )
                    if filled <= 0:
                        logger.warning(
                            "移动止盈[%s] %s IOC 限价未成交，改市价全量",
                            self.account_id,
                            symbol,
                        )
                        self.exchange.create_order(
                            symbol,
                            "market",
                            side,
                            amt,
                            None,
                            {"reduceOnly": True},
                        )
                    elif remaining > 0:
                        logger.info(
                            "移动止盈[%s] %s IOC 部分成交 filled=%s 剩余市价=%s",
                            self.account_id,
                            symbol,
                            filled,
                            remaining,
                        )
                        self.exchange.create_order(
                            symbol,
                            "market",
                            side,
                            remaining,
                            None,
                            {"reduceOnly": True},
                        )
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
        positions = self.fetch_positions()
        active_syms: set[str] = set()
        for position in positions:
            sym, q, _, _, sd = _parse_position_row(position)
            if sym and q > 0 and sd in ("long", "short"):
                active_syms.add(sym)
        if self.exchange_sync_stop:
            for sym in list(self._algo_trailing_id.keys()):
                if sym not in active_syms:
                    self._cancel_trailing_algo(sym)

        lines: list[str] = []
        has_open_for_interval = False
        px_tag = "最新" if self.use_last_price else "标记"
        use_ex = self.exchange_sync_stop and self._sync_algo_supported()
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

            current_price = self._resolve_current_price(symbol, mark_price)
            if current_price <= 0:
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

            if side == "long":
                profit_pct = (current_price - entry_price) / entry_price * 100.0
            else:
                profit_pct = (entry_price - current_price) / entry_price * 100.0

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

            line = (
                f"{symbol} {side}({px_tag}) 浮盈={profit_pct:.2f}% "
                f"最高={highest_profit:.2f}% 档={current_tier}"
            )
            lines.append(line)
            logger.info("移动止盈[%s] %s", self.account_id, line)

            if use_ex:
                tier_ex_th = self._tier_profit_exit_threshold(
                    current_tier, highest_profit
                )
                if tier_ex_th is not None and profit_pct <= tier_ex_th + 1.0e-9:
                    logger.info(
                        "移动止盈[%s] %s 分档(交易所) 峰值回撤触发 "
                        "profit=%.4f%% line=%.4f%% 档=%s",
                        self.account_id,
                        symbol,
                        profit_pct,
                        tier_ex_th,
                        current_tier,
                    )
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=current_price,
                    ):
                        continue
                    # 平仓失败时仍尝试同步条件单，避免无保护裸奔
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
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=current_price,
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
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=current_price,
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
                    if self.close_position(
                        symbol,
                        position_amt,
                        "sell" if side == "long" else "buy",
                        signal_price=current_price,
                    ):
                        continue

            if profit_pct <= -self.stop_loss_pct:
                logger.info(
                    "移动止盈[%s] %s 止损触发 %.2f%%",
                    self.account_id,
                    symbol,
                    profit_pct,
                )
                if self.close_position(
                    symbol,
                    position_amt,
                    "sell" if side == "long" else "buy",
                    signal_price=current_price,
                ):
                    continue

        summary = "; ".join(lines[:12]) if lines else "无持仓或无可解析仓位"
        if len(lines) > 12:
            summary += f" …共{len(lines)}个"
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
        logger.info(
            "移动止盈[%s] 循环启动 有仓=%ss 空仓=%ss 计价=%s 执行=%s",
            self.account_id,
            monitor_interval,
            idle_no_position_sec,
            "最新价" if self.use_last_price else "标记价",
            ex_txt,
        )
        try:
            while not stop_event.is_set():
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
                if stop_event.wait(timeout=wait_sec):
                    break
        finally:
            if self.exchange_sync_stop:
                for sym in list(self._algo_trailing_id.keys()):
                    self._cancel_trailing_algo(sym)
            logger.info("移动止盈[%s] 循环结束", self.account_id)
