"""
TradingView Webhook -> 币安下单服务。
运行: 在项目根目录或本目录下设置好环境变量后执行
  python -m flask --app app run --host 0.0.0.0 --port 5000
或: python app.py
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory

# 只从「python量化/.env」加载，避免无路径 load_dotenv() 扫到其它目录 .env 把密钥覆盖成空
_env = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env, override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)
if not _env.is_file():
    logger.warning("未找到配置文件（密钥将为空）: %s", _env)
else:
    logger.info("已加载环境配置: %s", _env)

# 旧版单密钥文件（仅用于一次性迁移到 data/accounts.json）
_RUNTIME_BINANCE_PATH = Path(__file__).resolve().parent / "data" / "binance_credentials.json"
_ACCOUNTS_PATH = Path(__file__).resolve().parent / "data" / "accounts.json"
_EXECUTION_LOG_PATH = Path(__file__).resolve().parent / "data" / "execution_log.json"
_SIGNAL_LOG_PATH = Path(__file__).resolve().parent / "data" / "signal_log.json"
_BOT_SETTINGS_PATH = Path(__file__).resolve().parent / "data" / "bot_settings.json"

_exchange_cache: dict[str, Any] = {}
_exchange_cache_lock = threading.Lock()


def _load_runtime_binance() -> tuple[str, str]:
    try:
        if not _RUNTIME_BINANCE_PATH.is_file():
            return "", ""
        with open(_RUNTIME_BINANCE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        k = (data.get("binance_api_key") or "").strip()
        s = (data.get("binance_secret") or "").strip()
        return k, s
    except Exception as e:
        logger.warning("读取旧版币安配置失败: %s", e)
        return "", ""


def _save_accounts_file(accounts: list[dict[str, Any]]) -> None:
    _ACCOUNTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _ACCOUNTS_PATH.with_suffix(".tmp")
    payload = {"version": 1, "accounts": accounts}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_ACCOUNTS_PATH)
    with _exchange_cache_lock:
        _exchange_cache.clear()
    with _BALANCE_CACHE_LOCK:
        _balance_cache.clear()


def _migrate_legacy_if_needed() -> None:
    if _ACCOUNTS_PATH.is_file():
        return
    k, s = _load_runtime_binance()
    if not k or not s:
        return
    fq = float(os.getenv("DEFAULT_QUOTE_AMOUNT", "20"))
    acc = {
        "id": str(uuid.uuid4()),
        "remark": "默认账户",
        "exchange": "binance",
        "api_key": k,
        "secret": s,
        "password": "",
        "fixed_quote_usdt": fq,
        "enabled": True,
        "webhook_enabled": True,
    }
    _save_accounts_file([acc])
    logger.info("已从 binance_credentials.json 迁移到 accounts.json（1 个账户）")


def _load_accounts() -> list[dict[str, Any]]:
    _migrate_legacy_if_needed()
    try:
        if not _ACCOUNTS_PATH.is_file():
            return []
        with open(_ACCOUNTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("accounts")
        return raw if isinstance(raw, list) else []
    except Exception as e:
        logger.warning("读取 accounts.json 失败: %s", e)
        return []


def _account_by_id(aid: str) -> dict[str, Any] | None:
    for a in _load_accounts():
        if str(a.get("id")) == str(aid):
            return a
    return None


def _mask_api_key(k: str) -> str:
    k = k.strip()
    if not k:
        return ""
    if len(k) <= 8:
        return "****"
    return f"{k[:4]}…{k[-4:]}"


WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "")
# 网页控制台专用（可与 TV_WEBHOOK_SECRET 分开）；未设置时回退为 TV_WEBHOOK_SECRET
DASHBOARD_SECRET = (os.getenv("DASHBOARD_SECRET") or "").strip()
# 网页只读访客密钥（仅可查看状态/日志，不可改配置、删记录、下单）
DASHBOARD_VIEWER_SECRET = (os.getenv("DASHBOARD_VIEWER_SECRET") or "").strip()
# spot | future
BINANCE_DEFAULT_TYPE = os.getenv("BINANCE_DEFAULT_TYPE", "future").lower()
# 默认每笔用多少 USDT（可被请求体 quote_amount 覆盖）
DEFAULT_QUOTE_AMOUNT = float(os.getenv("DEFAULT_QUOTE_AMOUNT", "20"))
# 测试网（需使用币安测试网密钥）
USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "false").lower() == "true"
# Webhook 下单后非关键后处理延迟秒数（止损/撤单/日志落盘）
WEBHOOK_POST_DELAY_SEC = float(os.getenv("WEBHOOK_POST_DELAY_SEC", "10"))
# 查询余额短缓存秒数：降低前端高频点按对交易网络请求的干扰；0 表示关闭缓存
BALANCE_CACHE_TTL_SEC = float(os.getenv("BALANCE_CACHE_TTL_SEC", "2"))

# Webhook 聚合信号 / 按账户执行记录：持久化到 data/*.json，不自动删除条数、不自动清空
ACCOUNT_LOG_QUERY_MAX = 50000

_account_log: list[dict[str, Any]] = []
_signal_log: list[dict[str, Any]] = []
# 多线程并发时保护内存列表与写盘（避免查余额/拉日志与 webhook 交错损坏数据）
_LOG_LOCK = threading.RLock()
_BALANCE_CACHE_LOCK = threading.RLock()
_balance_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _load_persisted_json_list(path: Path, key: str = "items") -> list[dict[str, Any]]:
    try:
        if not path.is_file():
            return []
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get(key)
        return raw if isinstance(raw, list) else []
    except Exception as e:
        logger.warning("读取 %s 失败: %s", path, e)
        return []


def _save_execution_log() -> None:
    _EXECUTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _EXECUTION_LOG_PATH.with_suffix(".tmp")
    payload = {"version": 1, "items": _account_log}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_EXECUTION_LOG_PATH)


def _save_signal_log() -> None:
    _SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _SIGNAL_LOG_PATH.with_suffix(".tmp")
    payload = {"version": 1, "items": _signal_log}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_SIGNAL_LOG_PATH)


def _reload_persisted_logs() -> None:
    global _account_log, _signal_log
    _account_log = _load_persisted_json_list(_EXECUTION_LOG_PATH)
    _signal_log = _load_persisted_json_list(_SIGNAL_LOG_PATH)
    if _backfill_account_log_latency_from_signal_log():
        _save_execution_log()


def _backfill_account_log_latency_from_signal_log() -> bool:
    """
    执行记录若缺分项耗时，用同笔订单在 signal_log 里已保存的 receive/execute/total 补全并写回磁盘。
    （仅当聚合记录已含新字段时有效；纯旧数据需重启服务后发新 Webhook 才会出现分项。）
    """
    idx: dict[str, dict[str, float]] = {}
    for s in _signal_log:
        if s.get("receive_signal_ms") is None:
            continue
        try:
            trip = {
                "receive_signal_ms": float(s["receive_signal_ms"]),
                "execute_trade_ms": float(s["execute_trade_ms"]),
                "total_trade_ms": float(s["total_trade_ms"]),
            }
        except (TypeError, ValueError, KeyError):
            continue
        for acc in s.get("accounts") or []:
            if not acc.get("ok") or not acc.get("order"):
                continue
            oid = str(acc["order"].get("id") or "").strip()
            if oid:
                idx[oid] = trip
    if not idx:
        return False
    changed = False
    for row in _account_log:
        if row.get("receive_signal_ms") is not None:
            continue
        oid = str(row.get("order_id") or "").strip()
        if not oid or oid not in idx:
            continue
        trip = idx[oid]
        row["receive_signal_ms"] = trip["receive_signal_ms"]
        row["execute_trade_ms"] = trip["execute_trade_ms"]
        row["total_trade_ms"] = trip["total_trade_ms"]
        row["server_latency_ms"] = trip["total_trade_ms"]
        changed = True
    if changed:
        logger.info("已从 signal_log 补全部分执行记录的分项耗时并写回 execution_log.json")
    return changed


_reload_persisted_logs()


def _default_bot_settings() -> dict[str, Any]:
    return {
        "stop_loss_enabled": os.getenv("STOP_LOSS_ENABLED", "").lower() == "true",
        "stop_loss_pct": float(os.getenv("STOP_LOSS_PCT", "3.33")),
    }


def load_bot_settings() -> dict[str, Any]:
    """开仓后限价止损：全局开关与百分比（data/bot_settings.json，未创建时回退 .env 默认）。"""
    base = _default_bot_settings()
    if not _BOT_SETTINGS_PATH.is_file():
        return base
    try:
        with open(_BOT_SETTINGS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            if "stop_loss_enabled" in data:
                base["stop_loss_enabled"] = bool(data["stop_loss_enabled"])
            if "stop_loss_pct" in data:
                base["stop_loss_pct"] = float(data["stop_loss_pct"])
    except Exception as e:
        logger.warning("读取 bot_settings 失败: %s", e)
    return base


def save_bot_settings(settings: dict[str, Any]) -> None:
    _BOT_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _BOT_SETTINGS_PATH.with_suffix(".tmp")
    payload = {
        "version": 1,
        "stop_loss_enabled": bool(settings.get("stop_loss_enabled")),
        "stop_loss_pct": float(settings.get("stop_loss_pct", 3.33)),
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_BOT_SETTINGS_PATH)


def _binance_futures_algo_order(exchange, body: dict[str, Any]) -> dict[str, Any]:
    """
    币安 USDⓈ-M 条件单须走 POST /fapi/v1/algoOrder（约 2025-12 起旧 /fapi/v1/order 对 STOP* 返回 -4120）。
    ccxt 可能尚未封装，使用 request('algoOrder', 'fapiPrivate', 'POST', body)。
    """
    return exchange.request("algoOrder", "fapiPrivate", "POST", body)


def _algo_response_as_order_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """统一成带 id 的 dict，便于日志/返回。"""
    aid = raw.get("algoId") or raw.get("clientAlgoId")
    return {
        "id": str(aid) if aid is not None else "",
        "info": raw,
        "algoOrder": True,
    }


def place_stop_loss_limit_order(
    exchange,
    unified_symbol: str,
    market_order: dict[str, Any],
    stop_loss_pct: float,
) -> dict[str, Any] | None:
    """
    市价开仓/加仓后挂止损：优先 STOP（限价触发），失败则 STOP_MARKET。
    仅用于合约：多头下方止损、空头上方止损。
    使用 Algo Order API（CONDITIONAL），避免 -4120。
    """
    filled = float(market_order.get("filled") or 0)
    if filled <= 0:
        amt = market_order.get("amount")
        if amt is not None:
            try:
                filled = float(amt)
            except (TypeError, ValueError):
                filled = 0.0
    if filled <= 0:
        logger.warning("止损跳过：市价单无成交数量")
        return None

    avg = float(market_order.get("average") or market_order.get("price") or 0)
    if avg <= 0:
        try:
            t = exchange.fetch_ticker(unified_symbol)
            avg = float(t.get("last") or t.get("close") or 0)
        except Exception as e:
            logger.warning("止损跳过：无法获取成交价 %s", e)
            return None
    if avg <= 0:
        return None

    side = str(market_order.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return None

    pct_f = float(stop_loss_pct) / 100.0
    if side == "buy":
        close_side = "SELL"
        stop_price = avg * (1 - pct_f)
    else:
        close_side = "BUY"
        stop_price = avg * (1 + pct_f)

    stop_price = float(exchange.price_to_precision(unified_symbol, stop_price))
    limit_price = stop_price
    exchange.load_markets()
    market = exchange.market(unified_symbol)
    symbol_id = str(market.get("id") or "")
    if not symbol_id:
        logger.warning("止损跳过：无法解析交易对 id")
        return None

    qty_prec = exchange.amount_to_precision(unified_symbol, filled)
    qty_str = str(qty_prec).strip()
    try:
        qf = float(qty_str)
    except (TypeError, ValueError):
        qf = 0.0
    if qf <= 0:
        return None

    tp_str = str(exchange.price_to_precision(unified_symbol, stop_price))
    lp_str = str(exchange.price_to_precision(unified_symbol, limit_price))

    # 文档：reduceOnly 为字符串 "true"/"false"
    base: dict[str, Any] = {
        "algoType": "CONDITIONAL",
        "symbol": symbol_id,
        "side": close_side,
        "workingType": "MARK_PRICE",
        "reduceOnly": "true",
        "triggerPrice": tp_str,
        "quantity": qty_str,
    }

    try:
        raw = _binance_futures_algo_order(
            exchange,
            {
                **base,
                "type": "STOP",
                "price": lp_str,
                "timeInForce": "GTC",
            },
        )
        return _algo_response_as_order_dict(raw if isinstance(raw, dict) else {"raw": raw})
    except Exception as e:
        logger.warning("Algo STOP 限价止损失败，改 STOP_MARKET: %s", e)
    try:
        raw = _binance_futures_algo_order(
            exchange,
            {
                **base,
                "type": "STOP_MARKET",
            },
        )
        return _algo_response_as_order_dict(raw if isinstance(raw, dict) else {"raw": raw})
    except Exception as e2:
        logger.warning("Algo STOP_MARKET 止损失败: %s", e2)
        raise


def _maybe_place_stop_loss_after_market(
    exchange,
    market_order: dict[str, Any],
    reduce_only: bool,
) -> tuple[dict[str, Any] | None, str | None]:
    """市价成交后挂止损；仅合约、非仅减仓、且开关开启时。"""
    bs = load_bot_settings()
    if not bs.get("stop_loss_enabled"):
        return None, None
    if BINANCE_DEFAULT_TYPE != "future":
        return None, None
    if reduce_only:
        return None, None
    sym = market_order.get("symbol")
    if not sym:
        return None, "市价单缺少 symbol"
    try:
        sl = place_stop_loss_limit_order(
            exchange,
            str(sym),
            market_order,
            float(bs["stop_loss_pct"]),
        )
        return sl, None
    except Exception as e:
        logger.warning("止损挂单失败: %s", e)
        return None, str(e)


def _tv_payload_is_strategy_full_flat(
    payload: dict[str, Any], reduce_only: bool
) -> bool:
    """
    策略「全部平仓」（可选）：告警里含 market_position=flat 或 position_size=0。
    未改 Pine、无上述字段时，请用 should_cancel_stop_loss_on_reduce 走交易所持仓判断。
    """
    if not reduce_only:
        return False
    mp = payload.get("market_position") or payload.get("strategy.market_position")
    cur = _normalize_tv_position_side(mp)
    if cur == "flat":
        return True
    ps = payload.get("position_size") or payload.get("strategy.position_size")
    if ps is not None and str(ps).strip() != "":
        try:
            if abs(float(str(ps).replace(",", ""))) < 1e-12:
                return True
        except (TypeError, ValueError):
            pass
    return False


def _futures_net_position_abs(exchange, unified_symbol: str) -> float | None:
    """U 本位合约当前净持仓数量绝对值；空仓为 0；查询失败为 None。"""
    try:
        exchange.load_markets()
        positions = exchange.fetch_positions([unified_symbol])
    except Exception as e:
        logger.warning("fetch_positions 失败: %s", e)
        return None
    if not positions:
        return 0.0
    matched = False
    for p in positions:
        if str(p.get("symbol") or "") != str(unified_symbol):
            continue
        matched = True
        c = p.get("contracts")
        if c is not None:
            try:
                return abs(float(c))
            except (TypeError, ValueError):
                pass
        info = p.get("info") or {}
        pa = info.get("positionAmt")
        if pa is not None:
            try:
                return abs(float(pa))
            except (TypeError, ValueError):
                pass
    if matched:
        # 有该合约仓位条目但解析不到数量：勿当作 0，避免误撤止损
        return None
    return 0.0


def _position_effectively_zero(
    exchange, unified_symbol: str, pos_abs: float
) -> bool:
    if pos_abs <= 0:
        return True
    try:
        exchange.load_markets()
        m = exchange.market(unified_symbol)
        mn = m.get("limits", {}).get("amount", {}).get("min")
        if mn is not None:
            return pos_abs < float(mn) * 0.51
    except Exception:
        pass
    return pos_abs < 1e-9


def should_cancel_stop_loss_on_reduce(
    exchange, unified_symbol: str, payload: dict[str, Any], reduce_only: bool
) -> bool:
    """
    仅减仓成交后是否视为「已全平」并撤止损：
    1）告警里含 flat / position_size=0（可选）；
    2）否则查询交易所：该合约净持仓≈0（无需改 Pine 策略）。
    """
    if not reduce_only or BINANCE_DEFAULT_TYPE != "future" or not unified_symbol:
        return False
    if _tv_payload_is_strategy_full_flat(payload, reduce_only):
        return True
    pos_abs = _futures_net_position_abs(exchange, unified_symbol)
    if pos_abs is None:
        return False
    return _position_effectively_zero(exchange, unified_symbol, pos_abs)


def _algo_row_is_stop_loss(row: dict[str, Any]) -> bool:
    ot = str(row.get("orderType") or "").upper()
    if ot not in ("STOP", "STOP_MARKET"):
        return False
    r = row.get("reduceOnly")
    if r is True:
        return True
    if isinstance(r, str) and r.lower() == "true":
        return True
    return False


def cancel_open_stop_loss_algo_orders(
    exchange, unified_symbol: str
) -> tuple[list[dict[str, Any]], str | None]:
    """
    取消该合约上未触发的 CONDITIONAL STOP / STOP_MARKET 且 reduceOnly 的挂单（机器人挂的止损）。
    """
    if BINANCE_DEFAULT_TYPE != "future":
        return [], None
    if not unified_symbol:
        return [], "缺少 symbol"
    try:
        exchange.load_markets()
        market = exchange.market(unified_symbol)
        symbol_id = str(market.get("id") or "")
    except Exception as e:
        return [], str(e)
    if not symbol_id:
        return [], "无法解析交易对 id"
    try:
        raw = exchange.request(
            "openAlgoOrders",
            "fapiPrivate",
            "GET",
            {"symbol": symbol_id, "algoType": "CONDITIONAL"},
        )
    except Exception as e:
        return [], str(e)
    rows: list[dict[str, Any]]
    if isinstance(raw, list):
        rows = raw
    elif isinstance(raw, dict) and isinstance(raw.get("orders"), list):
        rows = raw["orders"]
    else:
        rows = []
    cancelled: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not _algo_row_is_stop_loss(row):
            continue
        aid = row.get("algoId")
        if aid is None:
            continue
        try:
            exchange.request(
                "algoOrder",
                "fapiPrivate",
                "DELETE",
                {"algoId": aid},
            )
            cancelled.append(
                {"algoId": aid, "orderType": row.get("orderType")}
            )
        except Exception as e:
            logger.warning("取消止损 algo %s 失败: %s", aid, e)
    return cancelled, None


def _normalize_tv_position_side(raw: Any) -> str | None:
    """
    TradingView strategy.market_position / prev_market_position 填充后常见为
    long / short / flat，或数值 1 / -1 / 0。
    """
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s or s == "nan":
        return None
    if s in ("1", "1.0", "+1") or s.startswith("long"):
        return "long"
    if s in ("-1", "-1.0") or s.startswith("short"):
        return "short"
    if s in ("0", "0.0", "flat", "none"):
        return "flat"
    return None


def _infer_reduce_only_from_tv_context(payload: dict[str, Any], action: str) -> bool | None:
    """
    不修改 Pine：若告警 JSON 含 prev_market_position（与 TV 占位符），
    则根据「上一笔持仓方向 + 本次买卖方向」推断合约是否仅减仓。
    无法判断时返回 None（由调用方用默认 false）。
    """
    if BINANCE_DEFAULT_TYPE != "future":
        return None
    prev_raw = payload.get("prev_market_position") or payload.get(
        "strategy.prev_market_position"
    )
    prev = _normalize_tv_position_side(prev_raw)
    act = (action or "").lower().strip()
    if act not in ("buy", "sell"):
        return None
    if prev is None:
        return None
    if prev == "long" and act == "sell":
        return True
    if prev == "short" and act == "buy":
        return True
    if prev == "long" and act == "buy":
        return False
    if prev == "short" and act == "sell":
        return False
    if prev == "flat":
        return False
    return None


def _resolve_reduce_only(payload: dict[str, Any], action: str) -> bool:
    """显式 reduce_only 优先；否则用 TV 持仓占位符推断；否则 False。"""
    explicit = payload.get("reduce_only")
    if explicit is not None and str(explicit).strip() != "":
        return str(explicit).lower() in ("true", "1", "yes", "on")
    inferred = _infer_reduce_only_from_tv_context(payload, action)
    if inferred is not None:
        return inferred
    return False


def _trim_payload_for_log(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "action",
        "side",
        "symbol",
        "ticker",
        "quote_amount",
        "amount",
        "reduce_only",
        "prev_market_position",
        "market_position",
        "market_position_size",
    )
    out: dict[str, Any] = {}
    for k in keys:
        if k in payload:
            out[k] = payload[k]
    if not out:
        out["_raw_keys"] = list(payload.keys())[:20]
    return out


def _parse_reduce_only_from_payload(pl: dict[str, Any]) -> bool:
    """日志用：与下单一致的最终 reduce_only（含 TV 推断，见 _resolved_reduce_only）。"""
    if pl.get("_resolved_reduce_only") is not None:
        return bool(pl.get("_resolved_reduce_only"))
    v = pl.get("reduce_only")
    if v is None:
        return False
    return str(v).lower() in ("true", "1", "yes", "on")


def _service_action_label(reduce_only: bool, side: str) -> str:
    """本服务日志：根据 reduce_only 与方向给出中文动作（合约市价跟单）。"""
    s = (side or "").lower()
    if reduce_only:
        if s == "sell":
            return "减仓/平多"
        if s == "buy":
            return "减仓/平空"
        return "减仓/平仓"
    if s == "buy":
        return "开仓/加多"
    if s == "sell":
        return "开仓/加空"
    return "开仓/加仓"


def _utc_iso_from_ms(ms: float | int | None) -> str | None:
    if ms is None:
        return None
    try:
        m = float(ms)
        if m > 1e12:
            sec = m / 1000.0
        else:
            sec = m
        return datetime.fromtimestamp(sec, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3] + " UTC"
    except (TypeError, ValueError, OSError):
        return None


def record_webhook_signal(
    t_recv: float,
    payload: dict[str, Any],
    account_results: list[dict[str, Any]],
    *,
    receive_signal_ms: float,
    execute_trade_ms: float,
    total_trade_ms: float,
    completed_at: float | None = None,
) -> None:
    """记录一次 /webhook：多账户结果；account_results 含 account_id / remark / ok / order / error。"""
    t_done = completed_at if completed_at is not None else time.time()
    recv_ms = int(t_recv * 1000)
    done_ms = int(t_done * 1000)
    entry: dict[str, Any] = {
        "source": "webhook",
        "received_at_ms": recv_ms,
        "completed_at_ms": done_ms,
        "receive_signal_ms": receive_signal_ms,
        "execute_trade_ms": execute_trade_ms,
        "total_trade_ms": total_trade_ms,
        "server_latency_ms": total_trade_ms,
        "payload": _trim_payload_for_log(payload),
        "accounts": account_results,
        "ok": all(r.get("ok") for r in account_results) if account_results else False,
    }
    entry["received_at_utc"] = _utc_iso_from_ms(recv_ms)
    entry["completed_at_utc"] = _utc_iso_from_ms(done_ms)
    # 摘要：取第一个成功订单的时间戳便于前端展示
    for r in account_results:
        if not r.get("ok") or not r.get("order"):
            continue
        order = r["order"]
        ex_raw = order.get("timestamp") or order.get("lastUpdateTimestamp")
        ex_ms = None
        if ex_raw is not None:
            try:
                er = float(ex_raw)
                ex_ms = int(er) if er > 1e12 else int(er * 1000)
            except (TypeError, ValueError):
                ex_ms = None
        entry["exchange_timestamp_ms"] = ex_ms
        entry["exchange_time_utc"] = _utc_iso_from_ms(ex_ms)
        entry["order_id"] = order.get("id")
        entry["symbol"] = order.get("symbol")
        entry["average"] = order.get("average")
        entry["filled"] = order.get("filled")
        break
    with _LOG_LOCK:
        _signal_log.insert(0, entry)
        _save_signal_log()
        _record_account_rows_from_webhook(
            payload,
            account_results,
            receive_signal_ms=receive_signal_ms,
            execute_trade_ms=execute_trade_ms,
            total_trade_ms=total_trade_ms,
        )


def _merge_order_with_fetch(exchange, order: dict[str, Any]) -> dict[str, Any]:
    """create_order 返回时常缺少成交均价/手续费；补拉一次 fetch_order。"""
    oid = order.get("id")
    sym = order.get("symbol")
    if oid is None or not sym:
        return order
    try:
        fresh = exchange.fetch_order(str(oid), sym)
        if not isinstance(fresh, dict):
            return order
        merged = dict(order)
        for k in (
            "average",
            "filled",
            "price",
            "amount",
            "cost",
            "fee",
            "fees",
            "status",
            "lastTradeTimestamp",
        ):
            v = fresh.get(k)
            if v is not None and v != "":
                merged[k] = v
        if fresh.get("info") is not None:
            merged["info"] = fresh["info"]
        return merged
    except Exception as e:
        logger.debug("fetch_order 补全订单字段失败: %s", e)
        return order


def _order_summary_for_log(order: dict[str, Any] | None) -> dict[str, Any]:
    """从 ccxt 订单对象提取列表展示用成交价、数量、手续费（若有）。"""
    if not order:
        return {}
    out: dict[str, Any] = {}

    def _f(x: Any) -> float | None:
        if x is None or x == "":
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    avg = order.get("average")
    if avg is None:
        avg = order.get("price")
    fv = _f(avg)
    if fv is not None and fv > 0:
        out["order_average"] = fv

    filled = order.get("filled")
    ff = _f(filled)
    if ff is not None and ff > 0:
        out["order_filled"] = ff

    fee = order.get("fee")
    if isinstance(fee, dict) and fee.get("cost") is not None:
        fc = _f(fee.get("cost"))
        if fc is not None:
            out["order_fee"] = fc

    fees = order.get("fees")
    if isinstance(fees, list) and fees:
        total = 0.0
        for fe in fees:
            if isinstance(fe, dict) and fe.get("cost") is not None:
                c = _f(fe.get("cost"))
                if c is not None:
                    total += c
        if total > 0:
            out["order_fee"] = total

    info = order.get("info")
    if isinstance(info, dict):
        ap = info.get("avgPrice") or info.get("averagePrice")
        apf = _f(ap)
        if apf is not None and apf > 0 and "order_average" not in out:
            out["order_average"] = apf
        exq = info.get("executedQty") or info.get("cumQty")
        exf = _f(exq)
        if exf is not None and exf > 0 and "order_filled" not in out:
            out["order_filled"] = exf
        comm = info.get("commission")
        cmf = _f(comm)
        if cmf is not None and cmf > 0 and "order_fee" not in out:
            out["order_fee"] = cmf
        cq = _f(info.get("cumQuote"))
        exq2 = _f(info.get("executedQty"))
        if (
            cq is not None
            and cq > 0
            and exq2 is not None
            and exq2 > 0
            and "order_average" not in out
        ):
            out["order_average"] = cq / exq2

    cost = _f(order.get("cost"))
    filled2 = out.get("order_filled") or _f(order.get("filled"))
    if (
        cost is not None
        and cost > 0
        and filled2 is not None
        and filled2 > 0
        and "order_average" not in out
    ):
        out["order_average"] = cost / filled2

    return out


def _row_matches_market_filter(row: dict[str, Any], market_filter: str) -> bool:
    """market_filter: future | spot | 空（不过滤）。"""
    mf = market_filter.strip().lower()
    if not mf:
        return True
    m = str(row.get("market") or "").strip().lower()
    sym = str(row.get("symbol") or "")
    unified_future = ":" in sym and "/" in sym
    if mf == "future":
        if m == "future":
            return True
        if m == "spot":
            return False
        if unified_future:
            return True
        return BINANCE_DEFAULT_TYPE == "future"
    if mf == "spot":
        if m == "spot":
            return True
        if m == "future":
            return False
        if unified_future:
            return False
        return BINANCE_DEFAULT_TYPE == "spot"
    return True


def _symbol_matches_query(row_symbol: str, query: str) -> bool:
    """query 如 ETHUSDT，与统一 symbol 或紧凑形式匹配。"""
    q = (query or "").strip().upper().replace("/", "")
    if not q:
        return True
    s = (row_symbol or "").upper().replace("/", "").replace(":USDT", "").replace(":USDC", "")
    return q in s or q in (row_symbol or "").upper()


def _append_account_log_row(**kwargs: Any) -> None:
    ts_ms = int(kwargs.pop("ts_ms", time.time() * 1000))
    row: dict[str, Any] = {
        "ts_ms": ts_ms,
        "time_utc": _utc_iso_from_ms(ts_ms),
        **kwargs,
    }
    with _LOG_LOCK:
        _account_log.insert(0, row)
        _save_execution_log()


def _record_account_rows_from_webhook(
    payload: dict[str, Any],
    account_results: list[dict[str, Any]],
    *,
    receive_signal_ms: float,
    execute_trade_ms: float,
    total_trade_ms: float,
) -> None:
    pl = _trim_payload_for_log(payload)
    sym_hint = str(pl.get("symbol") or pl.get("ticker") or "")
    side_hint = str(pl.get("action") or pl.get("side") or "")
    ro = _parse_reduce_only_from_payload(pl)
    for r in account_results:
        rid = str(r.get("account_id") or "")
        remark = str(r.get("remark") or "")
        ts_ms = int(time.time() * 1000)
        fq = r.get("fixed_quote_usdt")
        fq_f = float(fq) if fq is not None else None
        if r.get("ok") and r.get("order"):
            o = r["order"]
            ex_raw = o.get("timestamp") or o.get("lastUpdateTimestamp")
            ex_ms = None
            if ex_raw is not None:
                try:
                    er = float(ex_raw)
                    ex_ms = int(er) if er > 1e12 else int(er * 1000)
                except (TypeError, ValueError):
                    ex_ms = None
            side_v = str(o.get("side") or side_hint or "")
            _append_account_log_row(
                ts_ms=ts_ms,
                source="webhook",
                market=BINANCE_DEFAULT_TYPE,
                account_id=rid,
                remark=remark,
                ok=True,
                symbol=o.get("symbol") or sym_hint,
                side=side_v,
                reduce_only=ro,
                action=_service_action_label(ro, side_v),
                quote_usdt=fq_f,
                order_id=str(o.get("id") or ""),
                error=None,
                exchange_timestamp_ms=ex_ms,
                receive_signal_ms=receive_signal_ms,
                execute_trade_ms=execute_trade_ms,
                total_trade_ms=total_trade_ms,
                server_latency_ms=total_trade_ms,
                **_order_summary_for_log(o),
            )
        else:
            _append_account_log_row(
                ts_ms=ts_ms,
                source="webhook",
                market=BINANCE_DEFAULT_TYPE,
                account_id=rid,
                remark=remark,
                ok=False,
                symbol=sym_hint,
                side=side_hint,
                reduce_only=ro,
                action=_service_action_label(ro, str(side_hint or "")),
                quote_usdt=fq_f,
                order_id=None,
                error=str(r.get("error") or ""),
                exchange_timestamp_ms=None,
                receive_signal_ms=receive_signal_ms,
                execute_trade_ms=execute_trade_ms,
                total_trade_ms=total_trade_ms,
                server_latency_ms=total_trade_ms,
            )


def _failures_24h_count_in_list(logs: list[dict[str, Any]]) -> int:
    now_ms = time.time() * 1000
    cutoff = now_ms - 86400000 * 1000
    n = 0
    for e in logs:
        if not e.get("ok") and (e.get("ts_ms") or 0) >= cutoff:
            n += 1
    return n


def refresh_env() -> None:
    """每次请求前从 .env 重新载入。"""
    global WEBHOOK_SECRET, DASHBOARD_SECRET, DASHBOARD_VIEWER_SECRET
    global BINANCE_DEFAULT_TYPE, DEFAULT_QUOTE_AMOUNT, USE_TESTNET, WEBHOOK_POST_DELAY_SEC
    global BALANCE_CACHE_TTL_SEC
    load_dotenv(_env, override=True)
    WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "")
    DASHBOARD_SECRET = (os.getenv("DASHBOARD_SECRET") or "").strip()
    DASHBOARD_VIEWER_SECRET = (os.getenv("DASHBOARD_VIEWER_SECRET") or "").strip()
    BINANCE_DEFAULT_TYPE = os.getenv("BINANCE_DEFAULT_TYPE", "future").lower()
    DEFAULT_QUOTE_AMOUNT = float(os.getenv("DEFAULT_QUOTE_AMOUNT", "20"))
    USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "false").lower() == "true"
    WEBHOOK_POST_DELAY_SEC = float(os.getenv("WEBHOOK_POST_DELAY_SEC", "10"))
    BALANCE_CACHE_TTL_SEC = float(os.getenv("BALANCE_CACHE_TTL_SEC", "2"))


def get_exchange_for_account(account: dict[str, Any], *, purpose: str = "default"):
    """
    按账户创建/复用 ccxt 实例（币安）。

    purpose:
      - trade: 下单关键路径（/webhook、/api/order）
      - read: 只读路径（/api/balance 等）
      - default: 兼容保留
    交易与只读拆分不同实例，避免查余额等慢请求与下单共享同一实例的内部限速队列。
    """
    import ccxt

    aid = str(account.get("id") or "")
    if not aid:
        raise ValueError("账户缺少 id")
    cache_key = f"{purpose}:{aid}"
    with _exchange_cache_lock:
        if cache_key in _exchange_cache:
            return _exchange_cache[cache_key]
    api_key = (account.get("api_key") or "").strip()
    secret = (account.get("secret") or "").strip()
    if not api_key or not secret:
        raise ValueError("账户未配置 API Key / Secret")
    ex_name = (account.get("exchange") or "binance").lower()
    if ex_name != "binance":
        raise ValueError("暂仅支持 binance")
    opts: dict[str, Any] = {
        "defaultType": BINANCE_DEFAULT_TYPE,
        "fetchCurrencies": False,
        "fetchBalance": {"defaultType": BINANCE_DEFAULT_TYPE},
    }
    ex = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret,
            "options": opts,
            "enableRateLimit": True,
        }
    )
    ex.options["fetchCurrencies"] = False
    if USE_TESTNET:
        ex.set_sandbox_mode(True)
    with _exchange_cache_lock:
        _exchange_cache[cache_key] = ex
    return ex


def webhook_accounts() -> list[dict[str, Any]]:
    """参与 Webhook 跟单的账户：启用 + 勾选 webhook。"""
    out: list[dict[str, Any]] = []
    for a in _load_accounts():
        if not a.get("enabled", True):
            continue
        if not a.get("webhook_enabled", True):
            continue
        if not (a.get("api_key") or "").strip() or not (a.get("secret") or "").strip():
            continue
        if float(a.get("fixed_quote_usdt") or 0) <= 0:
            continue
        out.append(a)
    return out


def build_order_payload_for_account(
    base: dict[str, Any], account: dict[str, Any]
) -> dict[str, Any]:
    """
    多账户：每账户使用固定 USDT 名义，不按 TV 的 quote_amount 比例缩放；
    余额变化也不改此固定值（由用户在各账户单独配置 fixed_quote_usdt）。
    """
    p = dict(base)
    fq = float(account.get("fixed_quote_usdt") or 0)
    if fq <= 0:
        raise ValueError(f"账户「{account.get('remark')}」未设置固定下单金额（USDT）")
    p["quote_amount"] = fq
    p.pop("amount", None)
    return p


def webhook_auth_or_error():
    """
    TradingView /webhook 鉴权。
    通过返回 None；否则返回 (jsonify 体, HTTP 状态码)。
    """
    if not WEBHOOK_SECRET:
        logger.error("未配置 TV_WEBHOOK_SECRET，拒绝所有请求")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "未配置 TV_WEBHOOK_SECRET",
                    "hint": "在 python量化/.env 中设置 TV_WEBHOOK_SECRET 后保存；已支持热加载，刷新页面即可。",
                }
            ),
            503,
        )
    header_secret = request.headers.get("X-TV-Secret")
    if header_secret and hmac.compare_digest(header_secret, WEBHOOK_SECRET):
        return None
    sig = request.headers.get("X-TV-Signature")
    if sig and request.data:
        digest = hmac.new(
            WEBHOOK_SECRET.encode(),
            request.data,
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(digest, sig):
            return None
    q = request.args.get("secret", "")
    if q and hmac.compare_digest(q, WEBHOOK_SECRET):
        return None
    return jsonify({"ok": False, "error": "Webhook 密钥错误或缺失"}), 401


def _dashboard_secret_expected() -> str:
    """网页里填的密钥须与此一致。"""
    return DASHBOARD_SECRET or WEBHOOK_SECRET


def _dashboard_viewer_secret_expected() -> str:
    """网页访客只读密钥（只允许看状态/日志）。"""
    return DASHBOARD_VIEWER_SECRET


def _dashboard_secret_from_request() -> str:
    """统一从 Header / Query / JSON 提取控制台密钥。"""
    header_secret = request.headers.get("X-TV-Secret")
    if header_secret is not None and str(header_secret).strip():
        return str(header_secret)
    q = request.args.get("secret")
    if q is not None and str(q).strip():
        return str(q)
    if request.is_json:
        body = request.get_json(silent=True) or {}
        s = body.get("secret")
        if s is not None and str(s).strip():
            return str(s)
    return ""


def dashboard_auth_role() -> str | None:
    """返回当前请求角色：admin | viewer | None。"""
    supplied = _dashboard_secret_from_request()
    if not supplied:
        return None
    admin = _dashboard_secret_expected()
    viewer = _dashboard_viewer_secret_expected()
    if admin and hmac.compare_digest(supplied, admin):
        return "admin"
    if viewer and hmac.compare_digest(supplied, viewer):
        return "viewer"
    return None


def dashboard_auth_response_if_invalid(*, allow_viewer: bool = False):
    """
    控制台接口鉴权：
    - admin：可读写
    - viewer：仅在 allow_viewer=True 的只读接口可访问
    若未通过，返回 (jsonify(...), status_code)；通过则返回 None。
    """
    admin = _dashboard_secret_expected()
    viewer = _dashboard_viewer_secret_expected()
    if not admin and not viewer:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "未配置控制台密钥",
                    "hint": "在 python量化/.env 添加：DASHBOARD_SECRET=主密钥（可读写）；可选 DASHBOARD_VIEWER_SECRET=访客只读密钥。保存后重启 python app.py。",
                }
            ),
            503,
        )
    role = dashboard_auth_role()
    if role == "admin":
        return None
    if allow_viewer and role == "viewer":
        return None
    if not allow_viewer and role == "viewer":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "访客密钥仅可只读，当前操作需要主密钥",
                    "hint": "请使用 DASHBOARD_SECRET（主密钥）执行改配置/删记录/下单等写操作。",
                }
            ),
            403,
        )
    if not allow_viewer and not admin:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "未配置主密钥，写操作已禁用",
                    "hint": "在 python量化/.env 设置 DASHBOARD_SECRET=主密钥 后重启进程。",
                }
            ),
            503,
        )
    return (
        jsonify(
            {
                "ok": False,
                "error": "密钥错误或未填写",
                "hint": "在「访问密钥」输入 .env 里的主密钥 DASHBOARD_SECRET（可读写）或访客密钥 DASHBOARD_VIEWER_SECRET（只读）。未单独设置主密钥时，DASHBOARD_SECRET 回退为 TV_WEBHOOK_SECRET。",
            }
        ),
        401,
    )


def normalize_symbol(raw: str) -> str:
    """BINANCE:BTCUSDT.P -> BTC/USDT"""
    s = raw.strip().upper()
    s = re.sub(r"^[^:]+:", "", s)
    s = s.replace(".P", "").replace("-PERP", "")
    if "/" in s:
        a, b = s.split("/", 1)
        return f"{a}/{b}"
    for quote in ("USDT", "BUSD", "USDC"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[: -len(quote)]}/{quote}"
    return s


def resolve_symbol(exchange, symbol: str) -> str:
    """
    统一 symbol。注意：BTCUSDT 会先被归一成 BTC/USDT，而 markets 里 BTC/USDT 是现货；
    若 BINANCE_DEFAULT_TYPE=future，必须优先落到 U 本位永续（如 BTC/USDT:USDT），
    否则下单/拉成交会走错现货市场，合约有成交时现货列表仍为空。
    """
    exchange.load_markets()
    if symbol in exchange.markets:
        m = exchange.markets[symbol]
        if (
            BINANCE_DEFAULT_TYPE == "future"
            and m.get("spot")
            and not m.get("contract")
        ):
            alt = f"{symbol}:USDT"
            if alt in exchange.markets:
                return alt
        return symbol
    if BINANCE_DEFAULT_TYPE == "future":
        alt = f"{symbol}:USDT"
        if alt in exchange.markets:
            return alt
    raise ValueError(f"未知交易对: {symbol}")


def quote_to_base_amount(exchange, symbol: str, quote_usdt: float) -> float:
    t = exchange.fetch_ticker(symbol)
    price = float(t.get("last") or t.get("close") or 0)
    if price <= 0:
        raise ValueError("无法从行情获取有效价格")
    return quote_usdt / price


def parse_body() -> dict[str, Any]:
    if not request.data:
        return {}
    try:
        return json.loads(request.data.decode("utf-8"))
    except json.JSONDecodeError:
        # TV 有时会把整段消息当纯文本 POST
        text = request.data.decode("utf-8", errors="replace").strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logger.warning("无法解析 JSON，原始前 200 字: %s", text[:200])
            return {}


def place_order(exchange, payload: dict[str, Any]) -> dict[str, Any]:
    """
    请求体字段（常用）:
      action / side: buy | sell
      symbol: BTCUSDT 或 BTC/USDT（必填）
      quote_amount: 用多少 USDT 市价（现货买单 / U 本位合约市价常用）
      amount: 基础币数量（与 quote_amount 二选一，优先 quote_amount）
      reduce_only: true/false（合约平仓）
    """
    sym_raw = payload.get("symbol") or payload.get("ticker") or ""
    if not sym_raw:
        raise ValueError("缺少 symbol / ticker")

    symbol = normalize_symbol(str(sym_raw))
    action = (
        payload.get("action")
        or payload.get("side")
        or payload.get("strategy.order.action")
        or ""
    )
    action = str(action).lower()
    if action not in ("buy", "sell"):
        raise ValueError(f"action/side 必须是 buy 或 sell，当前: {action}")

    quote_amount = payload.get("quote_amount")
    amount = payload.get("amount")
    reduce_only = _resolve_reduce_only(payload, action)
    payload["_resolved_reduce_only"] = reduce_only

    if quote_amount is not None:
        cost = float(quote_amount)
    elif amount is not None:
        cost = None
        amt = float(amount)
    else:
        cost = DEFAULT_QUOTE_AMOUNT
        amt = None

    params: dict[str, Any] = {}
    if reduce_only:
        params["reduceOnly"] = True

    symbol = resolve_symbol(exchange, symbol)

    if cost is not None:
        # 现货/合约均按最新价把 USDT 名义换算成基础币数量（与币安 API 一致）
        base_amt = quote_to_base_amount(exchange, symbol, cost)
        order = exchange.create_order(
            symbol, "market", action, base_amt, None, params
        )
    else:
        order = exchange.create_order(
            symbol, "market", action, amt, None, params
        )

    return order


_BASE = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(_BASE / "static"), static_url_path="/s")


@app.before_request
def _before_request_reload_env():
    # POST /webhook 跳过：启动时已 load_dotenv；避免每次 TV 信号都读盘解析 .env。
    # 改 TV_WEBHOOK_SECRET 等请重启进程，或先访问一次控制台（其它路由仍会 refresh）。
    if request.method == "POST" and request.path == "/webhook":
        return
    refresh_env()


@app.get("/")
def dashboard():
    return send_from_directory(app.static_folder, "index.html")


def _accounts_response_masked(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for a in accounts:
        out.append(
            {
                "id": a.get("id"),
                "remark": a.get("remark"),
                "exchange": a.get("exchange") or "binance",
                "api_key_preview": _mask_api_key((a.get("api_key") or "")),
                "has_secret": bool((a.get("secret") or "").strip()),
                "fixed_quote_usdt": a.get("fixed_quote_usdt"),
                "enabled": a.get("enabled", True),
                "webhook_enabled": a.get("webhook_enabled", True),
            }
        )
    return out


@app.get("/api/status")
def api_status():
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    dash = _dashboard_secret_expected()
    viewer = _dashboard_viewer_secret_expected()
    accs = _load_accounts()
    configured = any(
        (x.get("api_key") or "").strip() and (x.get("secret") or "").strip()
        for x in accs
    )
    bs = load_bot_settings()
    return jsonify(
        {
            "ok": True,
            "env_path": str(_env.resolve()),
            "env_file_exists": _env.is_file(),
            "binance_type": BINANCE_DEFAULT_TYPE,
            "testnet": USE_TESTNET,
            "api_configured": configured,
            "accounts_count": len(accs),
            "accounts_path": str(_ACCOUNTS_PATH.resolve()),
            "webhook_secret_configured": bool(WEBHOOK_SECRET),
            "dashboard_secret_configured": bool(dash),
            "dashboard_uses_dedicated_env": bool(DASHBOARD_SECRET),
            "dashboard_viewer_secret_configured": bool(viewer),
            "auth_role": dashboard_auth_role(),
            "default_quote_amount": DEFAULT_QUOTE_AMOUNT,
            "stop_loss_enabled": bs["stop_loss_enabled"],
            "stop_loss_pct": bs["stop_loss_pct"],
            "hint": "多账户：TradingView 信号按各账户「固定下单 USDT」分别下单，与策略里写的名义无关；余额增减不改变该固定值。",
        }
    )


@app.put("/api/bot-settings")
def api_bot_settings_put():
    """全局止损开关与百分比（需控制台密钥）；写入 data/bot_settings.json。"""
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    body = request.get_json(silent=True) or {}
    cur = load_bot_settings()
    if "stop_loss_enabled" in body:
        cur["stop_loss_enabled"] = bool(body.get("stop_loss_enabled"))
    if "stop_loss_pct" in body and body.get("stop_loss_pct") is not None:
        cur["stop_loss_pct"] = float(body.get("stop_loss_pct"))
    if cur["stop_loss_pct"] <= 0 or cur["stop_loss_pct"] > 50:
        return jsonify({"ok": False, "error": "止损百分比须在 0～50 之间"}), 400
    save_bot_settings(cur)
    return jsonify({"ok": True, **cur})


@app.get("/api/accounts")
def api_accounts_get():
    """交易账户列表（Key 掩码；需控制台密钥）。"""
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    accs = _load_accounts()
    return jsonify(
        {
            "ok": True,
            "accounts": _accounts_response_masked(accs),
            "path": str(_ACCOUNTS_PATH.resolve()),
        }
    )


@app.put("/api/accounts")
def api_accounts_put():
    """保存全部交易账户（需控制台密钥）。留空的 api_key/secret 表示保留原值；新行须填写密钥。"""
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    body = request.get_json(silent=True) or {}
    incoming = body.get("accounts")
    if not isinstance(incoming, list):
        return jsonify({"ok": False, "error": "需要 JSON 字段 accounts 为数组"}), 400
    old_map = {str(a["id"]): a for a in _load_accounts() if a.get("id")}
    merged: list[dict[str, Any]] = []
    for row in incoming:
        rid = str(row.get("id") or "").strip()
        e = old_map.get(rid) if rid else None
        if not rid:
            rid = str(uuid.uuid4())
        ak = (row.get("api_key") or "").strip()
        sk = (row.get("secret") or "").strip()
        if e:
            if not ak:
                ak = (e.get("api_key") or "").strip()
            if not sk:
                sk = (e.get("secret") or "").strip()
        else:
            if not ak or not sk:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"新账户「{row.get('remark') or rid}」须填写 API Key 与 Secret",
                    }
                ), 400
        fq = float(row.get("fixed_quote_usdt") or 0)
        if fq <= 0:
            return jsonify(
                {
                    "ok": False,
                    "error": f"账户「{row.get('remark') or rid}」固定下单金额须大于 0",
                }
            ), 400
        merged.append(
            {
                "id": rid,
                "remark": ((row.get("remark") or "").strip() or "未命名"),
                "exchange": (row.get("exchange") or "binance").lower(),
                "api_key": ak,
                "secret": sk,
                "password": (row.get("password") or "").strip(),
                "fixed_quote_usdt": fq,
                "enabled": bool(row.get("enabled", True)),
                "webhook_enabled": bool(row.get("webhook_enabled", True)),
            }
        )
    try:
        _save_accounts_file(merged)
    except OSError as ex:
        return jsonify({"ok": False, "error": str(ex)}), 500
    logger.info("已保存 %s 个交易账户", len(merged))
    return jsonify(
        {
            "ok": True,
            "accounts": _accounts_response_masked(merged),
        }
    )


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def normalize_usdt_balance(bal: dict[str, Any]) -> dict[str, Any]:
    """
    合约余额。fapi/v2 等接口的原始响应有时是「数组」，ccxt 放在 bal['info'] 里为 list，
    之前只处理 dict 会落到 default_zero。依次：统一结构 → 聚合 → info 为 list → info 为 dict。
    """
    u = bal.get("USDT") or {}
    f, used, total = _to_float(u.get("free")), _to_float(u.get("used")), _to_float(u.get("total"))
    if f is not None or used is not None or total is not None:
        return {"free": f, "used": used, "total": total, "source": "unified"}

    agg_f = bal.get("free") if isinstance(bal.get("free"), dict) else {}
    agg_u = bal.get("used") if isinstance(bal.get("used"), dict) else {}
    agg_t = bal.get("total") if isinstance(bal.get("total"), dict) else {}
    if isinstance(agg_f, dict) and "USDT" in agg_f:
        return {
            "free": _to_float(agg_f.get("USDT")),
            "used": _to_float(agg_u.get("USDT")),
            "total": _to_float(agg_t.get("USDT")),
            "source": "aggregated",
        }

    info = bal.get("info")
    if isinstance(info, list):
        for row in info:
            if str(row.get("asset", "")).upper() != "USDT":
                continue
            return {
                "free": _to_float(row.get("availableBalance")),
                "used": _to_float(row.get("initialMargin")),
                "total": _to_float(
                    row.get("marginBalance")
                    or row.get("walletBalance")
                    or row.get("balance")
                ),
                "source": "info_list",
            }

    if isinstance(info, dict):
        assets = info.get("assets")
        if isinstance(assets, list):
            for row in assets:
                if str(row.get("asset", "")).upper() == "USDT":
                    return {
                        "free": _to_float(row.get("availableBalance")),
                        "used": _to_float(row.get("initialMargin")),
                        "total": _to_float(
                            row.get("marginBalance") or row.get("walletBalance")
                        ),
                        "source": "assets",
                    }
        summary_keys = (
            "availableBalance",
            "totalWalletBalance",
            "totalMarginBalance",
        )
        if any(k in info for k in summary_keys):
            return {
                "free": _to_float(info.get("availableBalance")),
                "used": _to_float(info.get("totalInitialMargin")),
                "total": _to_float(
                    info.get("totalMarginBalance") or info.get("totalWalletBalance")
                ),
                "source": "account_summary",
                "note": "合约账户在部分资产为 0 时可能不返回 USDT 明细行，此处为账户 USDT 汇总",
            }

    return {
        "free": 0.0,
        "used": 0.0,
        "total": 0.0,
        "source": "default_zero",
        "note": "未能解析合约原始 info；若你实际在现货钱包，请看返回里的 usdt_spot",
    }


def normalize_spot_usdt(bal: dict[str, Any]) -> dict[str, Any]:
    """现货 USDT。"""
    u = bal.get("USDT") or {}
    free, used, tot = (
        _to_float(u.get("free")),
        _to_float(u.get("used")),
        _to_float(u.get("total")),
    )
    if free is not None or used is not None or tot is not None:
        return {"free": free, "used": used, "total": tot, "source": "spot"}
    return {"free": 0.0, "used": 0.0, "total": 0.0, "source": "spot_empty"}


@app.post("/api/balance")
def api_balance():
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    accs = _load_accounts()
    if not accs:
        return jsonify(
            {
                "ok": False,
                "error": "未配置交易账户：请在「交易账户」添加并保存。",
            }
        ), 503
    body = request.get_json(silent=True) or {}
    only_id = body.get("account_id")
    if only_id:
        accs = [a for a in accs if str(a.get("id")) == str(only_id)]
        if not accs:
            return jsonify({"ok": False, "error": "找不到该 account_id"}), 400
    cache_key = str(only_id) if only_id else "__all__"
    ttl = max(0.0, float(BALANCE_CACHE_TTL_SEC))
    now = time.time()
    if ttl > 0:
        with _BALANCE_CACHE_LOCK:
            item = _balance_cache.get(cache_key)
        if item and (now - item[0]) <= ttl:
            payload = dict(item[1])
            payload["cache"] = {"hit": True, "ttl_sec": ttl, "key": cache_key}
            return jsonify(payload)
    rows: list[dict[str, Any]] = []
    try:
        for a in accs:
            if not a.get("enabled", True):
                continue
            try:
                ex = get_exchange_for_account(a, purpose="read")
                usdt_future: dict[str, Any] = {}
                usdt_spot: dict[str, Any] = {}
                try:
                    usdt_future = normalize_usdt_balance(
                        ex.fetch_balance({"type": "future"})
                    )
                except Exception as e:
                    logger.warning("查询合约余额失败: %s", e)
                    usdt_future = {"error": str(e), "source": "future_error"}
                try:
                    usdt_spot = normalize_spot_usdt(
                        ex.fetch_balance({"type": "spot"})
                    )
                except Exception as e:
                    logger.warning("查询现货余额失败: %s", e)
                    usdt_spot = {"error": str(e), "source": "spot_error"}
                primary = (
                    usdt_future
                    if BINANCE_DEFAULT_TYPE == "future"
                    else usdt_spot
                )
                rows.append(
                    {
                        "account_id": a["id"],
                        "remark": a.get("remark"),
                        "fixed_quote_usdt": a.get("fixed_quote_usdt"),
                        "usdt": primary,
                        "usdt_future": usdt_future,
                        "usdt_spot": usdt_spot,
                    }
                )
            except Exception as e:
                rows.append(
                    {
                        "account_id": a.get("id"),
                        "remark": a.get("remark"),
                        "error": str(e),
                    }
                )
        first = rows[0] if rows else None
        resp = {
            "ok": True,
            "accounts": rows,
            "usdt": first.get("usdt") if first else None,
            "usdt_future": first.get("usdt_future") if first else None,
            "usdt_spot": first.get("usdt_spot") if first else None,
            "hint": "多账户：默认展示第一个账户摘要；完整结果见 accounts 数组。合约与现货资金池分开，需在币安划转。",
            "cache": {"hit": False, "ttl_sec": ttl, "key": cache_key},
        }
        if ttl > 0:
            with _BALANCE_CACHE_LOCK:
                _balance_cache[cache_key] = (time.time(), dict(resp))
        return jsonify(resp)
    except Exception as e:
        logger.exception("查询余额失败")
        return jsonify({"ok": False, "error": str(e)}), 400


def _strip_secret_from_payload(body: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in body.items() if k != "secret"}
    return out


@app.post("/api/order")
def api_order():
    """浏览器手动测试下单；须指定 account_id，名义按该账户固定 USDT。"""
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    body = request.get_json(silent=True) or {}
    aid = body.get("account_id")
    if not aid:
        return jsonify({"ok": False, "error": "请指定 account_id（交易账户）"}), 400
    a = _account_by_id(str(aid))
    if not a:
        return jsonify({"ok": False, "error": "找不到该交易账户"}), 400
    use_base_amount = bool(body.get("use_base_amount"))
    payload = _strip_secret_from_payload(body)
    for k in ("account_id", "use_base_amount"):
        payload.pop(k, None)
    try:
        t_total_start = time.time()
        ex = get_exchange_for_account(a, purpose="trade")
        if use_base_amount:
            t_exec_start = time.time()
            order = _merge_order_with_fetch(ex, place_order(ex, payload))
            ro = bool(payload.get("_resolved_reduce_only", False))
        else:
            op = build_order_payload_for_account(payload, a)
            t_exec_start = time.time()
            order = _merge_order_with_fetch(ex, place_order(ex, op))
            ro = bool(op.get("_resolved_reduce_only", False))
            payload["_resolved_reduce_only"] = op.get("_resolved_reduce_only")
            payload["reduce_only"] = op.get("_resolved_reduce_only")
        ex_raw = order.get("timestamp") or order.get("lastUpdateTimestamp")
        ex_ms = None
        if ex_raw is not None:
            try:
                er = float(ex_raw)
                ex_ms = int(er) if er > 1e12 else int(er * 1000)
            except (TypeError, ValueError):
                ex_ms = None
        side_v = str(order.get("side") or "")
        sl_order, sl_err = _maybe_place_stop_loss_after_market(ex, order, ro)
        cancel_res: dict[str, Any] = {}
        if (
            BINANCE_DEFAULT_TYPE == "future"
            and ro
            and should_cancel_stop_loss_on_reduce(
                ex, str(order.get("symbol") or ""), payload, ro
            )
        ):
            sym_u = str(order.get("symbol") or "")
            if sym_u:
                cancelled, cerr = cancel_open_stop_loss_algo_orders(ex, sym_u)
                cancel_res["stop_loss_cancelled"] = cancelled
                if cerr:
                    cancel_res["stop_loss_cancel_error"] = cerr
        t_trade_done = time.time()
        receive_signal_ms = round((t_exec_start - t_total_start) * 1000, 2)
        execute_trade_ms = round((t_trade_done - t_exec_start) * 1000, 2)
        total_trade_ms = round((t_trade_done - t_total_start) * 1000, 2)
        _append_account_log_row(
            ts_ms=int(time.time() * 1000),
            source="manual",
            market=BINANCE_DEFAULT_TYPE,
            account_id=str(a["id"]),
            remark=str(a.get("remark") or ""),
            ok=True,
            symbol=str(order.get("symbol") or ""),
            side=side_v,
            reduce_only=ro,
            action=_service_action_label(ro, side_v),
            quote_usdt=float(a.get("fixed_quote_usdt") or 0) if not use_base_amount else None,
            order_id=str(order.get("id") or ""),
            error=None,
            exchange_timestamp_ms=ex_ms,
            receive_signal_ms=receive_signal_ms,
            execute_trade_ms=execute_trade_ms,
            total_trade_ms=total_trade_ms,
            server_latency_ms=total_trade_ms,
            stop_loss_order_id=str(sl_order.get("id") or "") if sl_order else None,
            stop_loss_error=sl_err,
            **_order_summary_for_log(order),
        )
        return jsonify(
            {
                "ok": True,
                "order": order,
                "account_id": a["id"],
                "remark": a.get("remark"),
                "used_quote_usdt": None if use_base_amount else a.get("fixed_quote_usdt"),
                "stop_loss_order": sl_order,
                "stop_loss_error": sl_err,
                **cancel_res,
            }
        )
    except Exception as e:
        logger.exception("下单失败")
        try:
            ro = _parse_reduce_only_from_payload(payload)
            sh = str(payload.get("action") or payload.get("side") or "")
            _append_account_log_row(
                ts_ms=int(time.time() * 1000),
                source="manual",
                market=BINANCE_DEFAULT_TYPE,
                account_id=str(a["id"]),
                remark=str(a.get("remark") or ""),
                ok=False,
                symbol=str(payload.get("symbol") or payload.get("ticker") or ""),
                side=sh,
                reduce_only=ro,
                action=_service_action_label(ro, sh),
                quote_usdt=float(a.get("fixed_quote_usdt") or 0)
                if not use_base_amount
                else None,
                order_id=None,
                error=str(e),
                exchange_timestamp_ms=None,
                receive_signal_ms=None,
                execute_trade_ms=None,
                total_trade_ms=None,
                server_latency_ms=None,
            )
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 400


@app.get("/health")
def health():
    return jsonify({"ok": True, "binance_type": BINANCE_DEFAULT_TYPE})


@app.get("/api/signal-log")
def api_signal_log():
    """Webhook 聚合记录（持久化；需控制台密钥）；按账户明细见 /api/account-log。"""
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    with _LOG_LOCK:
        items = list(_signal_log)
        total = len(_signal_log)
    return jsonify(
        {
            "ok": True,
            "total_stored": total,
            "hint": "保存在 data/signal_log.json；不自动删除，仅控制台「清空」或 DELETE /api/signal-log。",
            "items": items,
        }
    )


@app.delete("/api/signal-log")
def api_signal_log_delete():
    """清空 Webhook 聚合记录（需控制台密钥）。"""
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    global _signal_log
    with _LOG_LOCK:
        _signal_log = []
        _save_signal_log()
    return jsonify({"ok": True, "cleared": True})


@app.get("/api/account-log")
def api_account_log():
    """按账户执行记录：Webhook/手动试单 每笔一条（持久化；需控制台密钥）。"""
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    aid = (request.args.get("account_id") or "").strip()
    market_filter = (request.args.get("market") or "").strip().lower()
    symbol_q = (request.args.get("symbol") or "").strip()
    try:
        limit = min(
            max(int(request.args.get("limit") or 2000), 1), ACCOUNT_LOG_QUERY_MAX
        )
    except ValueError:
        limit = 2000
    with _LOG_LOCK:
        snapshot = list(_account_log)
        total_stored = len(_account_log)
    out: list[dict[str, Any]] = []
    for e in snapshot:
        if aid and str(e.get("account_id")) != aid:
            continue
        if market_filter and not _row_matches_market_filter(e, market_filter):
            continue
        if symbol_q and not _symbol_matches_query(str(e.get("symbol") or ""), symbol_q):
            continue
        out.append(e)
        if len(out) >= limit:
            break
    hint = "保存在 data/execution_log.json；不自动删除条数，仅手动删除或清空。时间 UTC。"
    if any(
        x.get("server_latency_ms") is not None and x.get("receive_signal_ms") is None
        for x in out
    ):
        hint += " 部分行为升级前保存，耗时仅含「总计」；请重启机器人进程后由 TradingView 再发一笔信号，新记录将显示接受/执行/总计三项。"
    return jsonify(
        {
            "ok": True,
            "items": out,
            "failures_24h": _failures_24h_count_in_list(snapshot),
            "total_stored": total_stored,
            "hint": hint,
            "binance_default_type": BINANCE_DEFAULT_TYPE,
        }
    )


@app.delete("/api/account-log")
def api_account_log_delete():
    """
    删除执行记录。query: account_id= 时只删该账户；不传则清空全部。
    """
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    aid = (request.args.get("account_id") or "").strip()
    global _account_log
    with _LOG_LOCK:
        if aid:
            before = len(_account_log)
            _account_log = [
                e for e in _account_log if str(e.get("account_id")) != aid
            ]
            removed = before - len(_account_log)
        else:
            removed = len(_account_log)
            _account_log = []
        _save_execution_log()
        remaining = len(_account_log)
    return jsonify(
        {
            "ok": True,
            "removed": removed,
            "remaining": remaining,
        }
    )


def _finalize_webhook_results_after_delay(
    *,
    delay_sec: float,
    t_recv: float,
    payload: dict[str, Any],
    base_results: list[dict[str, Any]],
    post_tasks: list[dict[str, Any] | None],
    receive_signal_ms: float,
    execute_trade_ms: float,
    total_trade_ms: float,
    completed_at: float,
) -> None:
    if delay_sec > 0:
        time.sleep(delay_sec)
    finalized: list[dict[str, Any]] = []
    for i, base in enumerate(base_results):
        task = post_tasks[i] if i < len(post_tasks) else None
        if not base.get("ok") or not task:
            finalized.append(base)
            continue
        try:
            ex = task["exchange"]
            ro = bool(task["reduce_only"])
            order = _merge_order_with_fetch(ex, task["order"])
            payload_for_post = dict(task["payload"])
            sl_order, sl_err = _maybe_place_stop_loss_after_market(ex, order, ro)
            cancel_res: dict[str, Any] = {}
            if (
                BINANCE_DEFAULT_TYPE == "future"
                and ro
                and should_cancel_stop_loss_on_reduce(
                    ex, str(order.get("symbol") or ""), payload_for_post, ro
                )
            ):
                sym_u = str(order.get("symbol") or "")
                if sym_u:
                    cancelled, cerr = cancel_open_stop_loss_algo_orders(ex, sym_u)
                    cancel_res["stop_loss_cancelled"] = cancelled
                    if cerr:
                        cancel_res["stop_loss_cancel_error"] = cerr
                    if cancelled:
                        logger.info(
                            "持仓已平/全平：已取消 %s 上止损条件单 %s 笔",
                            sym_u,
                            len(cancelled),
                        )
            row = dict(base)
            row["order"] = order
            row["stop_loss_order"] = sl_order
            row["stop_loss_error"] = sl_err
            row.pop("post_process", None)
            row.update(cancel_res)
            finalized.append(row)
        except Exception as e:
            logger.exception("账户 %s 后处理失败", base.get("account_id"))
            row = dict(base)
            row["post_process_error"] = str(e)
            finalized.append(row)

    record_webhook_signal(
        t_recv,
        payload,
        finalized,
        receive_signal_ms=receive_signal_ms,
        execute_trade_ms=execute_trade_ms,
        total_trade_ms=total_trade_ms,
        completed_at=completed_at,
    )


@app.post("/webhook")
def webhook():
    auth_err = webhook_auth_or_error()
    if auth_err is not None:
        return auth_err
    targets = webhook_accounts()
    if not targets:
        logger.error("无可用交易账户（请添加账户、填写密钥、固定金额，并勾选参与 Webhook）")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "无可用交易账户：请在控制台添加账户、保存 API，并设置固定下单 USDT 与 Webhook 跟单。",
                }
            ),
            503,
        )

    t_recv = time.time()
    payload = parse_body()
    logger.info("收到 TV 载荷: %s", json.dumps(payload, ensure_ascii=False)[:500])
    t_trade_start = time.time()
    receive_signal_ms = round((t_trade_start - t_recv) * 1000, 2)

    results: list[dict[str, Any]] = []
    post_tasks: list[dict[str, Any] | None] = []
    for a in targets:
        try:
            ex = get_exchange_for_account(a, purpose="trade")
            op = build_order_payload_for_account(payload, a)
            order = place_order(ex, op)
            if "_resolved_reduce_only" in op:
                payload["reduce_only"] = op["_resolved_reduce_only"]
                payload["_resolved_reduce_only"] = op["_resolved_reduce_only"]
            ro = bool(op.get("_resolved_reduce_only", False))
            results.append(
                {
                    "account_id": a["id"],
                    "remark": a.get("remark"),
                    "ok": True,
                    "order": order,
                    "fixed_quote_usdt": a.get("fixed_quote_usdt"),
                    "stop_loss_order": None,
                    "stop_loss_error": None,
                    "post_process": f"delayed_{int(WEBHOOK_POST_DELAY_SEC)}s",
                }
            )
            post_tasks.append(
                {
                    "exchange": ex,
                    "order": order,
                    "reduce_only": ro,
                    "payload": dict(payload),
                }
            )
        except Exception as e:
            logger.exception("账户 %s 下单失败", a.get("id"))
            results.append(
                {
                    "account_id": a["id"],
                    "remark": a.get("remark"),
                    "ok": False,
                    "error": str(e),
                }
            )
            post_tasks.append(None)
    t_trade_done = time.time()
    execute_trade_ms = round((t_trade_done - t_trade_start) * 1000, 2)
    total_trade_ms = round((t_trade_done - t_recv) * 1000, 2)
    threading.Thread(
        target=_finalize_webhook_results_after_delay,
        kwargs={
            "delay_sec": WEBHOOK_POST_DELAY_SEC,
            "t_recv": t_recv,
            "payload": dict(payload),
            "base_results": results,
            "post_tasks": post_tasks,
            "receive_signal_ms": receive_signal_ms,
            "execute_trade_ms": execute_trade_ms,
            "total_trade_ms": total_trade_ms,
            "completed_at": t_trade_done,
        },
        daemon=True,
    ).start()
    ok_all = all(r.get("ok") for r in results)
    # 始终 200，避免 TradingView 因 4xx 反复重试；成功与否看 ok 与 results。
    return jsonify({"ok": ok_all, "results": results})


def main():
    if not WEBHOOK_SECRET and not DASHBOARD_SECRET and not DASHBOARD_VIEWER_SECRET:
        print(
            "警告: 未设置 TV_WEBHOOK_SECRET / DASHBOARD_SECRET / DASHBOARD_VIEWER_SECRET："
            "/webhook 不可用；网页控制台接口也不可用。请在 .env 配置后重启。",
            file=sys.stderr,
        )
    elif not WEBHOOK_SECRET and not DASHBOARD_SECRET:
        print(
            "提示: 未设置 TV_WEBHOOK_SECRET 与 DASHBOARD_SECRET："
            "/webhook 不可用，网页仅在设置 DASHBOARD_VIEWER_SECRET 后可只读访问。",
            file=sys.stderr,
        )
    elif not WEBHOOK_SECRET:
        print("提示: 未设置 TV_WEBHOOK_SECRET，TradingView /webhook 仍不可用。", file=sys.stderr)
    port = int(os.getenv("PORT", "5000"))
    # threaded=True：控制台查余额/拉日志等慢请求不阻塞另一条线程处理 /webhook 下单
    threaded = os.getenv("FLASK_THREADED", "true").lower() in ("1", "true", "yes", "on")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=threaded)


if __name__ == "__main__":
    main()
