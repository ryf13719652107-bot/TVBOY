"""
TP/SL 止盈止损监控引擎。
独立模块，通过后台线程轮询持仓并管理止盈止损挂单。
支持多交易对（逗号分隔，共用同一套 TP/SL 参数）。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent / "data"
_SETTINGS_PATH = _DATA_DIR / "tp_sl_settings.json"


def _default_config() -> dict[str, Any]:
    return {
        "version": 1,
        "account_id": "",
        "symbol": "ETH/USDT",
        "tp_points": 20.0,
        "sl_points": 50.0,
        "breakeven_trigger_points": 30.0,
        "breakeven_drawdown_points": 10.0,
        "use_limit_orders": True,
        "enabled": False,
    }


def load_tp_sl_config() -> dict[str, Any]:
    """读取 TP/SL 配置，若文件不存在返回默认值。"""
    try:
        if _SETTINGS_PATH.is_file():
            data = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg = _default_config()
                cfg.update({k: v for k, v in data.items() if k in cfg})
                return cfg
    except Exception as e:
        logger.warning("读取 TP/SL 配置失败: %s", e)
    return _default_config()


def save_tp_sl_config(config: dict[str, Any]) -> None:
    """保存 TP/SL 配置到文件（原子写入）。"""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    cfg = _default_config()
    cfg.update({k: v for k, v in config.items() if k in cfg})
    tmp = _SETTINGS_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_SETTINGS_PATH)
    logger.info("TP/SL 配置已保存")


def _parse_symbols(raw: str) -> list[str]:
    """解析逗号分隔的币种列表，去空白去重。"""
    if not raw or not raw.strip():
        return []
    seen = set()
    out = []
    for s in raw.split(","):
        s = s.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _new_symbol_state() -> dict[str, Any]:
    return {
        "state": "idle",  # idle | monitoring | breakeven
        "peak_price": None,
        "be_activated": False,
        "tp_order_id": None,
        "sl_order_id": None,
        "position_info": {},
        "orders_info": [],
        "last_reconciled_entry_price": None,
    }


class TpSlMonitor:
    """止盈止损监控器。后台线程轮询持仓，自动挂单并管理保本止损。
    支持多交易对——在 symbol 字段用逗号分隔，如 ETH/USDT,BTC/USDT。
    所有交易对共用同一套 TP/SL/保本参数。
    """

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._running = False
        self._lock = threading.RLock()
        self._config = load_tp_sl_config()
        self._symbol_data: dict[str, dict[str, Any]] = {}
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        """启动监控线程。"""
        config = load_tp_sl_config()
        if not config.get("account_id"):
            raise ValueError("请先选择交易账户")
        with self._lock:
            if self._running:
                return
            self._config = config
            self._running = True
            self._symbol_data = {}
            self._last_error = None
        self._thread = threading.Thread(target=self._loop, name="tp-sl-monitor", daemon=True)
        self._thread.start()
        symbols = _parse_symbols(config.get("symbol", ""))
        logger.info(
            "TP/SL 监控已启动: account=%s symbols=%s",
            config.get("account_id"),
            symbols,
        )

    def stop(self) -> None:
        """停止监控并尝试取消所有挂单。"""
        with self._lock:
            self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        try:
            self._cancel_all_orders()
        except Exception as e:
            logger.warning("停止 TP/SL 监控时取消挂单失败: %s", e)
        with self._lock:
            self._symbol_data = {}
        logger.info("TP/SL 监控已停止")

    def status(self) -> dict[str, Any]:
        """返回当前状态（供前端轮询）。每个交易对独立状态。"""
        with self._lock:
            symbols_status = {}
            for sym, sd in self._symbol_data.items():
                symbols_status[sym] = {
                    "state": sd.get("state", "idle"),
                    "peak_price": sd.get("peak_price"),
                    "be_activated": sd.get("be_activated", False),
                    "tp_order_id": sd.get("tp_order_id"),
                    "sl_order_id": sd.get("sl_order_id"),
                    "tp_price": sd.get("tp_price"),
                    "sl_price": sd.get("sl_price"),
                    "position": dict(sd.get("position_info", {})),
                    "orders": list(sd.get("orders_info", [])),
                    "last_reconciled_entry_price": sd.get("last_reconciled_entry_price"),
                }
            return {
                "running": self._running,
                "symbols": symbols_status,
                "last_error": self._last_error,
                "config": load_tp_sl_config(),
            }

    # ------------------------------------------------------------------
    # 内部 — 交易所实例
    # ------------------------------------------------------------------

    def _get_exchange(self):
        """通过 app.get_exchange_for_account 获取交易所实例。"""
        from app import get_exchange_for_account, _load_accounts  # noqa: E402

        account_id = self._config.get("account_id", "")
        accounts = _load_accounts()
        account = next((a for a in accounts if a.get("id") == account_id), None)
        if not account:
            raise ValueError(f"账户未找到: {account_id or '未设置'}")
        ex = get_exchange_for_account(account, purpose="tp-sl")
        if getattr(ex, "id", "") == "gate":
            from app import (  # noqa: E402
                _gate_ensure_decimal_header,
                _gate_fix_precision,
                _gate_patch_create_order,
            )

            _gate_ensure_decimal_header(ex)
            _gate_fix_precision(ex)
            _gate_patch_create_order(ex)
        return ex

    def _resolve_ex_symbol(self, ex, symbol: str) -> str:
        """将统一格式符号转为交易所格式（如 ETH/USDT → ETH/USDT:USDT）。"""
        from app import normalize_symbol, resolve_symbol  # noqa: E402

        norm = normalize_symbol(symbol)
        return resolve_symbol(ex, norm)

    # ------------------------------------------------------------------
    # 内部 — 每个交易对的状态存取
    # ------------------------------------------------------------------

    def _sd(self, symbol: str) -> dict[str, Any]:
        """获取或初始化某个交易对的状态（需持有锁）。"""
        if symbol not in self._symbol_data:
            self._symbol_data[symbol] = _new_symbol_state()
        return self._symbol_data[symbol]

    # ------------------------------------------------------------------
    # 内部 — 挂单管理
    # ------------------------------------------------------------------

    def _place_tp_sl(
        self,
        ex,
        ex_symbol: str,
        user_symbol: str,
        pos_side: str,
        pos_size: float,
        entry_price: float,
        config: dict[str, Any],
    ) -> None:
        """为指定交易对挂止盈/止损单。"""
        tp_points = float(config.get("tp_points", 20))
        sl_points = float(config.get("sl_points", 50))
        use_limit = bool(config.get("use_limit_orders", True))

        if pos_side == "long":
            tp_price = entry_price + tp_points
            sl_price = entry_price - sl_points
            tp_side = "sell"
            sl_side = "sell"
        else:
            tp_price = entry_price - tp_points
            sl_price = entry_price + sl_points
            tp_side = "buy"
            sl_side = "buy"

        amount = abs(pos_size)

        if getattr(ex, "id", "") == "gate":
            from app import _gate_effective_min_contracts, _gate_normalize_close_amount  # noqa: E402

            amount = _gate_normalize_close_amount(ex, ex_symbol, pos_size)
            mkt = ex.markets.get(ex_symbol) if getattr(ex, "markets", None) else None
            min_contracts = _gate_effective_min_contracts(mkt)
            if amount < min_contracts:
                logger.warning(
                    "[%s] Gate 合约止盈止损张数 %.4f 不足最小 %.4f 张，跳过",
                    user_symbol,
                    amount,
                    min_contracts,
                )
                return
            logger.info(
                "[%s] Gate TP/SL 平仓张数=%s（持仓=%s）",
                user_symbol,
                amount,
                pos_size,
            )

        # 取消该交易对的旧挂单
        self._cancel_symbol_orders(ex, ex_symbol, user_symbol)

        tp_oid = None
        sl_oid = None

        # 先尝试挂 TP
        if tp_price > 0:
            try:
                tp_order_obj = ex.create_order(
                    ex_symbol, "limit", tp_side, amount, tp_price,
                    {"reduceOnly": True},
                )
                tp_oid = str(tp_order_obj.get("id", ""))
                logger.info("[%s] TP 限价单已挂: %s @ %.4f id=%s", user_symbol, tp_side, tp_price, tp_oid)
            except Exception as e:
                logger.warning("[%s] 挂 TP 限价单失败: %s", user_symbol, e)
                tp_oid = None

        # 再尝试挂 SL（允许第一次失败，重试第二次）
        if sl_price > 0:
            try:
                sl_order_obj = ex.create_order(
                    ex_symbol, "stop_market", sl_side, amount, sl_price,
                    {"stopPrice": sl_price, "reduceOnly": True},
                )
                sl_oid = str(sl_order_obj.get("id", ""))
                logger.info("[%s] SL 止损单已挂: %s @ %.4f id=%s", user_symbol, sl_side, sl_price, sl_oid)
            except Exception as e:
                logger.warning("[%s] 挂 SL 止损单失败: %s", user_symbol, e)
                try:
                    sl_order_obj = ex.create_order(
                        ex_symbol, "stop", sl_side, amount, sl_price,
                        {"stopPrice": sl_price, "reduceOnly": True},
                    )
                    sl_oid = str(sl_order_obj.get("id", ""))
                    logger.info("[%s] SL 止损单已挂(stop): %s @ %.4f id=%s", user_symbol, sl_side, sl_price, sl_oid)
                except Exception as e2:
                    logger.warning("[%s] 挂 SL stop 单也失败: %s", user_symbol, e2)
                    sl_oid = None

        # 检查两个单是否都挂成了；SL 优先：TP 失败时仍保留已挂成的 SL
        tp_needed = tp_price > 0 and use_limit
        sl_needed = sl_price > 0
        tp_ok = (not tp_needed) or tp_oid is not None
        sl_ok = (not sl_needed) or sl_oid is not None

        if tp_ok and sl_ok:
            pass
        elif sl_ok and not tp_ok:
            logger.warning(
                "[%s] TP 未挂成，保留 SL id=%s（止损优先）",
                user_symbol,
                sl_oid,
            )
            if tp_oid:
                try:
                    ex.cancel_order(tp_oid, ex_symbol)
                except Exception:
                    pass
                tp_oid = None
        elif tp_ok and not sl_ok:
            logger.warning("[%s] SL 未挂成，撤销 TP id=%s", user_symbol, tp_oid)
            if tp_oid:
                try:
                    ex.cancel_order(tp_oid, ex_symbol)
                    logger.info("[%s] 已撤销 TP 单: %s", user_symbol, tp_oid)
                except Exception:
                    pass
            tp_oid = None
        else:
            logger.warning("[%s] TP/SL 均未挂成", user_symbol)
            for oid, label in ((tp_oid, "TP"), (sl_oid, "SL")):
                if not oid:
                    continue
                try:
                    ex.cancel_order(oid, ex_symbol)
                    logger.info("[%s] 已撤销 %s 单: %s", user_symbol, label, oid)
                except Exception:
                    pass
            tp_oid = None
            sl_oid = None

        with self._lock:
            sd = self._sd(user_symbol)
            sd["tp_order_id"] = tp_oid
            sd["sl_order_id"] = sl_oid
            sd["tp_price"] = tp_price if tp_oid else None
            sd["sl_price"] = sl_price if sl_oid else None

    def _cancel_symbol_orders(self, ex, ex_symbol: str, user_symbol: str) -> None:
        """取消某个交易对的所有挂单（先按内部记录取消，再通过 fetch_open_orders 清空遗漏）。"""
        tp_oid = None
        sl_oid = None
        with self._lock:
            sd = self._symbol_data.get(user_symbol)
            if sd:
                tp_oid = sd.get("tp_order_id")
                sl_oid = sd.get("sl_order_id")
                sd["tp_order_id"] = None
                sd["sl_order_id"] = None

        for oid in (tp_oid, sl_oid):
            if not oid:
                continue
            try:
                ex.cancel_order(str(oid), ex_symbol)
                logger.info("[%s] 已取消挂单(内部记录): %s", user_symbol, oid)
            except Exception as e:
                logger.debug("[%s] 取消挂单 %s 失败（可能已成交）: %s", user_symbol, oid, e)

        # 兜底：通过 fetch_open_orders 取消该交易对剩余所有挂单
        try:
            open_orders = ex.fetch_open_orders(ex_symbol) or []
            cancelled_any = False
            for o in open_orders:
                oid = str(o.get("id", ""))
                if not oid:
                    continue
                try:
                    ex.cancel_order(oid, ex_symbol)
                    cancelled_any = True
                    logger.info("[%s] 已取消挂单(兜底): %s", user_symbol, oid)
                except Exception:
                    pass
            if cancelled_any:
                # 清除内部状态，防止残留
                with self._lock:
                    sd2 = self._symbol_data.get(user_symbol)
                    if sd2:
                        sd2["orders_info"] = []
        except Exception as e:
            logger.debug("[%s] fetch_open_orders 兜底清理失败: %s", user_symbol, e)

    def _cancel_all_orders(self, ex=None) -> None:
        """取消所有交易对的所有挂单（停止/退出时调用）。"""
        all_pairs = []
        with self._lock:
            for sym, sd in self._symbol_data.items():
                tp_oid = sd.get("tp_order_id")
                sl_oid = sd.get("sl_order_id")
                if tp_oid or sl_oid:
                    all_pairs.append((sym, tp_oid, sl_oid))
                sd["tp_order_id"] = None
                sd["sl_order_id"] = None

        if not all_pairs:
            return

        if ex is None:
            try:
                ex = self._get_exchange()
            except Exception:
                return

        for sym, tp_oid, sl_oid in all_pairs:
            try:
                ex_sym = self._resolve_ex_symbol(ex, sym)
            except Exception:
                continue
            for oid in (tp_oid, sl_oid):
                if not oid:
                    continue
                try:
                    ex.cancel_order(str(oid), ex_sym)
                    logger.info("[%s] 已取消挂单: %s", sym, oid)
                except Exception as e:
                    logger.debug("[%s] 取消挂单 %s 失败: %s", sym, oid, e)

    # ------------------------------------------------------------------
    # 内部 — 保本止损
    # ------------------------------------------------------------------

    def _check_breakeven(
        self,
        ex,
        ex_symbol: str,
        user_symbol: str,
        pos_side: str,
        pos_size: float,
        entry_price: float,
        mark_price: float,
        config: dict[str, Any],
        sl_order: dict[str, Any],
    ) -> None:
        """检查并执行保本止损逻辑。"""
        be_trigger = float(config.get("breakeven_trigger_points", 0))
        be_drawdown = float(config.get("breakeven_drawdown_points", 0))
        if be_trigger <= 0 or be_drawdown <= 0:
            return

        if pos_side == "long":
            profit_points = mark_price - entry_price
        else:
            profit_points = entry_price - mark_price

        be_activated = False
        peak = None
        with self._lock:
            sd = self._sd(user_symbol)
            be_activated = sd.get("be_activated", False)
            peak = sd.get("peak_price")

        new_peak = peak
        if pos_side == "long":
            if peak is None or mark_price > peak:
                new_peak = mark_price
        elif pos_side == "short":
            if peak is None or mark_price < peak:
                new_peak = mark_price

        if not be_activated and profit_points >= be_trigger:
            be_activated = True
            logger.info(
                "[%s] 保本止损已激活: 盈利 %.2f 点 ≥ 触发 %.2f 点",
                user_symbol, profit_points, be_trigger,
            )

        if be_activated and be_drawdown > 0:
            if pos_side == "long":
                drawdown = new_peak - mark_price
            else:
                drawdown = mark_price - new_peak

            if drawdown >= be_drawdown:
                logger.info(
                    "[%s] 保本回撤触发: 回撤 %.2f 点 ≥ %.2f 点，立即市价平仓",
                    user_symbol, drawdown, be_drawdown,
                )
                sl_oid = str(sl_order.get("id", ""))
                if sl_oid:
                    try:
                        ex.cancel_order(sl_oid, ex_symbol)
                    except Exception:
                        pass

                amount = abs(pos_size)
                if getattr(ex, "id", "") == "gate":
                    from app import _gate_effective_min_contracts, _gate_normalize_close_amount  # noqa: E402

                    amount = _gate_normalize_close_amount(ex, ex_symbol, pos_size)
                    mkt = ex.markets.get(ex_symbol) if getattr(ex, "markets", None) else None
                    min_contracts = _gate_effective_min_contracts(mkt)
                    if amount < min_contracts:
                        logger.warning(
                            "[%s] Gate 合约保本平仓张数 %.4f 不足最小 %.4f 张，跳过",
                            user_symbol,
                            amount,
                            min_contracts,
                        )
                        return
                sl_side = "sell" if pos_side == "long" else "buy"
                try:
                    ex.create_order(
                        ex_symbol, "market", sl_side, amount, None,
                        {"reduceOnly": True},
                    )
                    logger.info("[%s] 保本市价平仓完成", user_symbol)
                except Exception as e:
                    logger.error("[%s] 保本市价平仓失败: %s", user_symbol, e)

        with self._lock:
            sd3 = self._sd(user_symbol)
            sd3["be_activated"] = be_activated
            sd3["peak_price"] = new_peak
            if be_activated and sd3.get("state") != "breakeven":
                sd3["state"] = "breakeven"

    # ------------------------------------------------------------------
    # 内部 — 持仓查询
    # ------------------------------------------------------------------

    def _fetch_position(self, ex, ex_symbol: str) -> tuple[float, str, float, float]:
        """获取单个交易对的持仓。返回 (pos_size, pos_side, entry_price, mark_price)。
        Gate：ccxt 的 contracts 恒为正，方向须读 side 或 info.size 符号。"""
        try:
            positions = ex.fetch_positions([ex_symbol])
            if positions and len(positions) > 0:
                pos = positions[0]
                info = pos.get("info") or {}
                entry_price = float(pos.get("entryPrice") or info.get("entry_price") or 0)
                mark_price = float(
                    pos.get("markPrice") or pos.get("mark") or info.get("mark_price") or 0
                )
                pos_side = str(pos.get("side") or "").lower().strip()
                pos_size = 0.0

                if getattr(ex, "id", "") == "gate" and info.get("size") is not None:
                    try:
                        raw_sz = float(info["size"])
                        pos_size = abs(raw_sz)
                        if raw_sz > 0:
                            pos_side = pos_side or "long"
                        elif raw_sz < 0:
                            pos_side = pos_side or "short"
                    except (TypeError, ValueError):
                        pass

                if pos_size <= 0:
                    contracts = float(
                        pos.get("contracts") or info.get("positionAmt") or 0
                    )
                    pos_size = abs(contracts)
                    if pos_side not in ("long", "short"):
                        if contracts > 0:
                            pos_side = "long"
                        elif contracts < 0:
                            pos_side = "short"

                if pos_side not in ("long", "short"):
                    pos_side = "flat"

                return pos_size, pos_side, entry_price, mark_price
        except Exception as e:
            logger.debug("获取持仓失败 (%s): %s", ex_symbol, e)
        return 0.0, "flat", 0.0, 0.0

    # ------------------------------------------------------------------
    # 内部 — 主循环
    # ------------------------------------------------------------------

    def _process_one_symbol(
        self, ex, user_symbol: str, config: dict[str, Any]
    ) -> None:
        """处理单个交易对的一个轮询周期。"""
        ex_symbol = self._resolve_ex_symbol(ex, user_symbol)
        pos_size, pos_side, entry_price, mark_price = self._fetch_position(ex, ex_symbol)

        # 无持仓 → 取消挂单，重置状态
        if pos_size <= 0 or pos_side == "flat" or entry_price <= 0:
            with self._lock:
                sd = self._sd(user_symbol)
                if sd.get("tp_order_id") or sd.get("sl_order_id"):
                    self._cancel_symbol_orders(ex, ex_symbol, user_symbol)
                sd["state"] = "idle"
                sd["peak_price"] = None
                sd["be_activated"] = False
                sd["last_reconciled_entry_price"] = None
                sd["position_info"] = {}
                sd["orders_info"] = []
            return

        # 更新持仓信息
        with self._lock:
            sd = self._sd(user_symbol)
            sd["position_info"] = {
                "symbol": user_symbol,
                "side": pos_side,
                "size": pos_size,
                "entry_price": entry_price,
                "mark_price": mark_price,
            }

        # 检查已有挂单
        open_orders = []
        try:
            open_orders = ex.fetch_open_orders(ex_symbol) or []
        except Exception:
            pass

        # 构建 id → order 映射，优先用已记录的 ID 匹配
        orders_by_id: dict[str, dict] = {}
        for o in open_orders:
            oid = str(o.get("id", ""))
            if oid:
                orders_by_id[oid] = o

        tp_order = None
        sl_order = None

        with self._lock:
            sd = self._sd(user_symbol)
            stored_tp_id = sd.get("tp_order_id")
            stored_sl_id = sd.get("sl_order_id")

        # 优先按已记录的 ID 识别（避免字段名差异导致匹配失败）
        if stored_tp_id and stored_tp_id in orders_by_id:
            tp_order = orders_by_id[stored_tp_id]
        if stored_sl_id and stored_sl_id in orders_by_id:
            sl_order = orders_by_id[stored_sl_id]

        # ID 匹配失败时，回退到模式匹配
        if tp_order is None or sl_order is None:
            for o in open_orders:
                oid = str(o.get("id", ""))
                otype = str(o.get("type", "")).lower()
                oside = str(o.get("side", "")).lower()
                oprice = float(o.get("price") or o.get("stopPrice") or 0)
                # 多仓平仓 = sell / close_long / close；空仓平仓 = buy / close_short / close
                _close_sides = ("sell", "close_long", "close") if pos_side == "long" else ("buy", "close_short", "close")
                if oside not in _close_sides:
                    continue
                if tp_order is None and otype == "limit" and oprice > 0:
                    if pos_side == "long" and oprice > entry_price:
                        tp_order = o
                    elif pos_side == "short" and oprice < entry_price:
                        tp_order = o
                if sl_order is None and ("stop" in otype or "market" in otype):
                    if pos_side == "long" and oprice <= entry_price:
                        sl_order = o
                    elif pos_side == "short" and oprice >= entry_price:
                        sl_order = o

        with self._lock:
            sd = self._sd(user_symbol)
            sd["orders_info"] = [
                {
                    "id": o.get("id"),
                    "type": o.get("type"),
                    "side": o.get("side"),
                    "price": o.get("price"),
                    "stopPrice": o.get("stopPrice"),
                    "amount": o.get("amount"),
                }
                for o in open_orders
            ]
            if tp_order:
                sd["tp_order_id"] = str(tp_order.get("id", ""))
                sd["tp_price"] = float(tp_order.get("price") or 0)
            if sl_order:
                sd["sl_order_id"] = str(sl_order.get("id", ""))
                sd["sl_price"] = float(sl_order.get("stopPrice") or sl_order.get("price") or 0)

        with self._lock:
            sd = self._sd(user_symbol)
            last_ep = sd.get("last_reconciled_entry_price")

        # 更新峰值价格
        with self._lock:
            sd = self._sd(user_symbol)
            peak = sd.get("peak_price")
            if pos_side == "long" and (peak is None or mark_price > peak):
                sd["peak_price"] = mark_price
            elif pos_side == "short" and (peak is None or mark_price < peak):
                sd["peak_price"] = mark_price
            sd["state"] = "monitoring"

        # 保本止损检查（有 SL 单即可执行）
        if sl_order is not None:
            self._check_breakeven(
                ex, ex_symbol, user_symbol, pos_side, pos_size,
                entry_price, mark_price, config, sl_order,
            )

        # 兜底：TP 已成交（挂单消失）但 SL 还在，验证仓位是否需要清理
        tp_gone = stored_tp_id and not tp_order
        if tp_gone and sl_order is not None:
            _ps, _pside, _ep, _ = self._fetch_position(ex, ex_symbol)
            if _ps <= 0 or _pside == "flat" or _ep <= 0:
                logger.info("[%s] TP 已成交，仓位已平，清理 SL 挂单", user_symbol)
                self._cancel_symbol_orders(ex, ex_symbol, user_symbol)
                with self._lock:
                    sd = self._sd(user_symbol)
                    sd["state"] = "idle"
                    sd["peak_price"] = None
                    sd["be_activated"] = False
                    sd["last_reconciled_entry_price"] = None
                return

        # 入场价变化时重新挂单
        entry_changed = (
            last_ep is None
            or abs(entry_price - last_ep) > 1e-8
        )
        if entry_changed:
            if tp_order is not None or sl_order is not None:
                self._cancel_symbol_orders(ex, ex_symbol, user_symbol)
            self._place_tp_sl(
                ex, ex_symbol, user_symbol, pos_side, pos_size, entry_price, config,
            )
            with self._lock:
                sd = self._sd(user_symbol)
                if mark_price > 0:
                    sd["peak_price"] = mark_price
                sd["be_activated"] = False
                sd["last_reconciled_entry_price"] = entry_price

    def _loop(self) -> None:
        """后台监控主循环（每 3 秒轮询所有交易对）。"""
        logger.info("TP/SL 监控循环已启动")
        consecutive_errors = 0
        while True:
            with self._lock:
                if not self._running:
                    break
                config = dict(self._config)

            symbols = _parse_symbols(config.get("symbol", ""))
            if not symbols:
                time.sleep(3)
                continue

            try:
                ex = self._get_exchange()
                for sym in symbols:
                    try:
                        self._process_one_symbol(ex, sym, config)
                    except Exception as e:
                        logger.debug("[%s] 处理异常: %s", sym, e)
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors <= 1 or consecutive_errors % 20 == 0:
                    logger.warning("TP/SL 监控循环异常 (#%d): %s", consecutive_errors, e)
                with self._lock:
                    self._last_error = str(e)

            time.sleep(3)

        logger.info("TP/SL 监控循环已退出")


# ------------------------------------------------------------------
# 模块级单例
# ------------------------------------------------------------------
_monitor: TpSlMonitor | None = None


def get_monitor() -> TpSlMonitor:
    """获取全局 TpSlMonitor 单例。"""
    global _monitor
    if _monitor is None:
        _monitor = TpSlMonitor()
    return _monitor
