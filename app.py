"""
TradingView Webhook -> 多交易所下单服务。
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
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory

from trailing_stop_worker import TrailingStopWorker

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
_EXECUTION_LOG_JSONL_PATH = Path(__file__).resolve().parent / "data" / "execution_log.jsonl"
_SIGNAL_LOG_JSONL_PATH = Path(__file__).resolve().parent / "data" / "signal_log.jsonl"
_EXECUTION_ARCHIVE_JSONL_PATH = (
    Path(__file__).resolve().parent / "data" / "execution_log.archive.jsonl"
)
_SIGNAL_ARCHIVE_JSONL_PATH = (
    Path(__file__).resolve().parent / "data" / "signal_log.archive.jsonl"
)
_BOT_SETTINGS_PATH = Path(__file__).resolve().parent / "data" / "bot_settings.json"

_exchange_cache: dict[str, Any] = {}
_exchange_cache_lock = threading.Lock()
_account_trade_locks: dict[str, threading.Lock] = {}
_account_trade_locks_lock = threading.Lock()
_webhook_finalize_executor: ThreadPoolExecutor | None = None
_webhook_finalize_executor_lock = threading.Lock()
_trailing_stop_tasks_lock = threading.Lock()
_trailing_stop_tasks: dict[str, dict[str, Any]] = {}


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
        "template_capital_usdt": 0,
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


def _get_account_trade_lock(aid: str) -> threading.Lock:
    key = str(aid or "")
    with _account_trade_locks_lock:
        lock = _account_trade_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _account_trade_locks[key] = lock
        return lock


def _get_webhook_finalize_executor() -> ThreadPoolExecutor:
    global _webhook_finalize_executor
    with _webhook_finalize_executor_lock:
        if _webhook_finalize_executor is None:
            workers = max(1, WEBHOOK_FINALIZE_MAX_WORKERS)
            _webhook_finalize_executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="webhook-finalize",
            )
        return _webhook_finalize_executor


def _mask_api_key(k: str) -> str:
    k = k.strip()
    if not k:
        return ""
    if len(k) <= 8:
        return "****"
    return f"{k[:4]}…{k[-4:]}"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _parse_display_est_fee_rate(value: str | None, default: float = 0.0005) -> float:
    """网页「交易记录」估算手续费费率：名义=成交价×数量，手续费=名义×费率。默认 0.0005=0.05%。"""
    if value is None:
        return default
    s = str(value).strip()
    if not s:
        return default
    try:
        v = float(s)
    except (TypeError, ValueError):
        return default
    if v < 0:
        return default
    if v > 0.02:
        return 0.02
    return v


WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "")
# 网页控制台专用（可与 TV_WEBHOOK_SECRET 分开）；未设置时回退为 TV_WEBHOOK_SECRET
DASHBOARD_SECRET = (os.getenv("DASHBOARD_SECRET") or "").strip()
# 网页只读访客密钥（仅可查看状态/日志，不可改配置、删记录、下单）
DASHBOARD_VIEWER_SECRET = (os.getenv("DASHBOARD_VIEWER_SECRET") or "").strip()
# spot | future
BINANCE_DEFAULT_TYPE = os.getenv("BINANCE_DEFAULT_TYPE", "future").lower()
# 默认每笔用多少 USDT（可被请求体 quote_amount 覆盖）
DEFAULT_QUOTE_AMOUNT = float(os.getenv("DEFAULT_QUOTE_AMOUNT", "20"))
# 模板本金（USDT）：>0 时，若 TV 载荷含 contracts，则按比例换算各账户下单数量
TEMPLATE_BASE_CAPITAL_USDT = float(os.getenv("TEMPLATE_BASE_CAPITAL_USDT", "0"))
# 启动时预加载交易所 markets（减少重启后首单 load_markets 冷启动延迟）
PRELOAD_MARKETS_ON_STARTUP = (
    os.getenv("PRELOAD_MARKETS_ON_STARTUP", "true").lower()
    in ("1", "true", "yes", "on")
)
# 测试网（需使用币安测试网密钥）
USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "false").lower() == "true"


def _load_okx_td_mode() -> str:
    """OKX 永续下单 tdMode：cross 全仓 / isolated 逐仓，须与 OKX 账户实际模式一致。"""
    v = (os.getenv("OKX_TD_MODE", "cross") or "cross").strip().lower()
    return v if v in ("cross", "isolated") else "cross"


OKX_TD_MODE = _load_okx_td_mode()
# Webhook 下单后非关键后处理延迟秒数（止损/撤单/日志落盘）
WEBHOOK_POST_DELAY_SEC = float(os.getenv("WEBHOOK_POST_DELAY_SEC", "10"))
# Webhook 多账户并发下单线程上限（默认 8）
WEBHOOK_TRADE_MAX_WORKERS = _env_int("WEBHOOK_TRADE_MAX_WORKERS", 8)
# Webhook 后处理线程池大小（默认 4）
WEBHOOK_FINALIZE_MAX_WORKERS = _env_int("WEBHOOK_FINALIZE_MAX_WORKERS", 4)
# Webhook 是否打印完整 payload 日志（速度优先建议 false）
WEBHOOK_LOG_PAYLOAD = os.getenv("WEBHOOK_LOG_PAYLOAD", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 查询余额短缓存秒数：降低前端高频点按对交易网络请求的干扰；0 表示关闭缓存
BALANCE_CACHE_TTL_SEC = float(os.getenv("BALANCE_CACHE_TTL_SEC", "2"))
# 热日志上限（超出后自动归档到 *.archive.jsonl）
LOG_HOT_MAX_EXECUTION = _env_int("LOG_HOT_MAX_EXECUTION", 50000)
LOG_HOT_MAX_SIGNAL = _env_int("LOG_HOT_MAX_SIGNAL", 20000)
# 控制台「交易记录」表格估算手续费（成交价×数量×费率）；真实扣费仍以交易所 order_fee 为准
DISPLAY_EST_FEE_RATE = _parse_display_est_fee_rate(os.getenv("DISPLAY_EST_FEE_RATE"))

# Webhook 聚合信号 / 按账户执行记录：持久化到 data/*.json，不自动删除条数、不自动清空
ACCOUNT_LOG_QUERY_MAX = 50000

_account_log: deque[dict[str, Any]] = deque()
_signal_log: deque[dict[str, Any]] = deque()
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


def _load_persisted_jsonl_list(path: Path) -> list[dict[str, Any]]:
    try:
        if not path.is_file():
            return []
        rows: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
        rows.reverse()
        return rows
    except Exception as e:
        logger.warning("读取 %s 失败: %s", path, e)
        return []


def _append_jsonl_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _rewrite_jsonl_from_newest(path: Path, rows_newest_first: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for row in reversed(rows_newest_first):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(path)


def _delete_rows_in_jsonl(path: Path, should_delete) -> int:
    rows = _load_persisted_jsonl_list(path)
    if not rows:
        return 0
    kept: list[dict[str, Any]] = []
    removed = 0
    for row in rows:
        if should_delete(row):
            removed += 1
        else:
            kept.append(row)
    _rewrite_jsonl_from_newest(path, kept)
    return removed


def _save_execution_log() -> None:
    _EXECUTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _EXECUTION_LOG_PATH.with_suffix(".tmp")
    payload = {"version": 1, "items": list(_account_log)}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_EXECUTION_LOG_PATH)
    _rewrite_jsonl_from_newest(_EXECUTION_LOG_JSONL_PATH, list(_account_log))


def _save_signal_log() -> None:
    _SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _SIGNAL_LOG_PATH.with_suffix(".tmp")
    payload = {"version": 1, "items": list(_signal_log)}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(_SIGNAL_LOG_PATH)
    _rewrite_jsonl_from_newest(_SIGNAL_LOG_JSONL_PATH, list(_signal_log))


def _trim_hot_logs_locked(kind: str) -> None:
    if kind == "execution":
        max_keep = max(1, LOG_HOT_MAX_EXECUTION)
        hot = _account_log
        archive_path = _EXECUTION_ARCHIVE_JSONL_PATH
    else:
        max_keep = max(1, LOG_HOT_MAX_SIGNAL)
        hot = _signal_log
        archive_path = _SIGNAL_ARCHIVE_JSONL_PATH
    if len(hot) <= max_keep:
        return
    overflow: list[dict[str, Any]] = []
    while len(hot) > max_keep:
        overflow.append(hot.pop())
    if overflow:
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with open(archive_path, "a", encoding="utf-8") as f:
            for row in overflow:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _reload_persisted_logs() -> None:
    global _account_log, _signal_log
    _account_log = deque(_load_persisted_jsonl_list(_EXECUTION_LOG_JSONL_PATH))
    _signal_log = deque(_load_persisted_jsonl_list(_SIGNAL_LOG_JSONL_PATH))
    if not _account_log:
        _account_log = deque(_load_persisted_json_list(_EXECUTION_LOG_PATH))
        if _account_log:
            _rewrite_jsonl_from_newest(_EXECUTION_LOG_JSONL_PATH, list(_account_log))
    if not _signal_log:
        _signal_log = deque(_load_persisted_json_list(_SIGNAL_LOG_PATH))
        if _signal_log:
            _rewrite_jsonl_from_newest(_SIGNAL_LOG_JSONL_PATH, list(_signal_log))
    if _backfill_account_log_latency_from_signal_log():
        _save_execution_log()
    with _LOG_LOCK:
        _trim_hot_logs_locked("execution")
        _trim_hot_logs_locked("signal")


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
        "webhook_sizing_mode": (
            os.getenv("WEBHOOK_SIZING_MODE", "template_or_fixed").strip().lower()
            or "template_or_fixed"
        ),
        "template_base_capital_usdt": float(
            os.getenv("TEMPLATE_BASE_CAPITAL_USDT", "0")
        ),
        "webhook_open_only": False,
    }


def load_bot_settings() -> dict[str, Any]:
    """机器人全局设置（data/bot_settings.json，未创建时回退 .env 默认）。"""
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
            if "webhook_sizing_mode" in data:
                mode = str(data["webhook_sizing_mode"] or "").strip().lower()
                if mode in ("template_or_fixed", "follow_tv"):
                    base["webhook_sizing_mode"] = mode
            if "template_base_capital_usdt" in data:
                base["template_base_capital_usdt"] = float(
                    data["template_base_capital_usdt"]
                )
            if "webhook_open_only" in data:
                base["webhook_open_only"] = bool(data["webhook_open_only"])
    except Exception as e:
        logger.warning("读取 bot_settings 失败: %s", e)
    if base["template_base_capital_usdt"] < 0:
        base["template_base_capital_usdt"] = 0.0
    if base["webhook_sizing_mode"] not in ("template_or_fixed", "follow_tv"):
        base["webhook_sizing_mode"] = "template_or_fixed"
    return base


def save_bot_settings(settings: dict[str, Any]) -> None:
    _BOT_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _BOT_SETTINGS_PATH.with_suffix(".tmp")
    webhook_sizing_mode = str(
        settings.get("webhook_sizing_mode", "template_or_fixed") or "template_or_fixed"
    ).strip().lower()
    if webhook_sizing_mode not in ("template_or_fixed", "follow_tv"):
        webhook_sizing_mode = "template_or_fixed"
    template_base_capital_usdt = float(
        settings.get("template_base_capital_usdt", TEMPLATE_BASE_CAPITAL_USDT)
    )
    if template_base_capital_usdt < 0:
        template_base_capital_usdt = 0.0
    payload = {
        "version": 1,
        "stop_loss_enabled": bool(settings.get("stop_loss_enabled")),
        "stop_loss_pct": float(settings.get("stop_loss_pct", 3.33)),
        "webhook_sizing_mode": webhook_sizing_mode,
        "template_base_capital_usdt": template_base_capital_usdt,
        "webhook_open_only": bool(settings.get("webhook_open_only")),
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
    """市价成交后挂止损；当前仅 binance 合约支持。"""
    if not _is_binance_exchange(exchange):
        return None, None
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
    if BINANCE_DEFAULT_TYPE != "future" or not _is_binance_exchange(exchange):
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
    也处理 TV 占位符未替换的情况（如 {{strategy.prev_market_position}}）。
    """
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s or s == "nan":
        return None
    # TV 占位符未替换时原样发送，如 {{strategy.prev_market_position}}
    if s.startswith("{{") and s.endswith("}}"):
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
        _signal_log.appendleft(entry)
        _append_jsonl_row(_SIGNAL_LOG_JSONL_PATH, entry)
        if len(_signal_log) >= LOG_HOT_MAX_SIGNAL * 1.5:
            _trim_hot_logs_locked("signal")
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
    amt = _f(order.get("amount"))
    if amt is not None and amt > 0 and "order_filled" not in out:
        # 部分交易所市价单立即返回可能无 filled，先回退展示下单数量
        out["order_filled"] = amt

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
        # Hyperliquid 成交回报常见字段
        sz = _f(info.get("sz"))
        if sz is not None and sz > 0 and "order_filled" not in out:
            out["order_filled"] = sz
        hfee = _f(info.get("fee"))
        if hfee is not None and hfee >= 0 and "order_fee" not in out:
            out["order_fee"] = hfee
        hpnl = _f(info.get("closedPnl"))
        if hpnl is not None:
            out["order_pnl"] = hpnl

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
        _account_log.appendleft(row)
        _append_jsonl_row(_EXECUTION_LOG_JSONL_PATH, row)
        if len(_account_log) >= LOG_HOT_MAX_EXECUTION * 1.5:
            _trim_hot_logs_locked("execution")


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
        fq = r.get("used_quote_usdt", r.get("fixed_quote_usdt"))
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
    cutoff = now_ms - 86400000
    n = 0
    for e in logs:
        if not e.get("ok") and (e.get("ts_ms") or 0) >= cutoff:
            n += 1
    return n


def refresh_env() -> None:
    """每次请求前从 .env 重新载入。"""
    global WEBHOOK_SECRET, DASHBOARD_SECRET, DASHBOARD_VIEWER_SECRET
    global BINANCE_DEFAULT_TYPE, DEFAULT_QUOTE_AMOUNT, TEMPLATE_BASE_CAPITAL_USDT
    global USE_TESTNET, WEBHOOK_POST_DELAY_SEC, PRELOAD_MARKETS_ON_STARTUP
    global BALANCE_CACHE_TTL_SEC, WEBHOOK_TRADE_MAX_WORKERS, WEBHOOK_FINALIZE_MAX_WORKERS, WEBHOOK_LOG_PAYLOAD
    global LOG_HOT_MAX_EXECUTION, LOG_HOT_MAX_SIGNAL, DISPLAY_EST_FEE_RATE
    global OKX_TD_MODE
    load_dotenv(_env, override=True)
    WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "")
    DASHBOARD_SECRET = (os.getenv("DASHBOARD_SECRET") or "").strip()
    DASHBOARD_VIEWER_SECRET = (os.getenv("DASHBOARD_VIEWER_SECRET") or "").strip()
    BINANCE_DEFAULT_TYPE = os.getenv("BINANCE_DEFAULT_TYPE", "future").lower()
    DEFAULT_QUOTE_AMOUNT = float(os.getenv("DEFAULT_QUOTE_AMOUNT", "20"))
    TEMPLATE_BASE_CAPITAL_USDT = float(os.getenv("TEMPLATE_BASE_CAPITAL_USDT", "0"))
    USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "false").lower() == "true"
    WEBHOOK_POST_DELAY_SEC = float(os.getenv("WEBHOOK_POST_DELAY_SEC", "10"))
    WEBHOOK_TRADE_MAX_WORKERS = _env_int("WEBHOOK_TRADE_MAX_WORKERS", 8)
    WEBHOOK_FINALIZE_MAX_WORKERS = _env_int("WEBHOOK_FINALIZE_MAX_WORKERS", 4)
    WEBHOOK_LOG_PAYLOAD = os.getenv("WEBHOOK_LOG_PAYLOAD", "false").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    BALANCE_CACHE_TTL_SEC = float(os.getenv("BALANCE_CACHE_TTL_SEC", "2"))
    LOG_HOT_MAX_EXECUTION = _env_int("LOG_HOT_MAX_EXECUTION", 50000)
    LOG_HOT_MAX_SIGNAL = _env_int("LOG_HOT_MAX_SIGNAL", 20000)
    DISPLAY_EST_FEE_RATE = _parse_display_est_fee_rate(os.getenv("DISPLAY_EST_FEE_RATE"))
    PRELOAD_MARKETS_ON_STARTUP = (
        os.getenv("PRELOAD_MARKETS_ON_STARTUP", "true").lower()
        in ("1", "true", "yes", "on")
    )
    OKX_TD_MODE = _load_okx_td_mode()


def preload_trade_markets_on_startup() -> None:
    """启动时预热交易账户 markets，降低首笔 webhook 下单冷启动延迟。"""
    targets = webhook_accounts()
    if not targets:
        return
    warmed = 0
    for a in targets:
        try:
            ex = get_exchange_for_account(a, purpose="trade")
            ex.load_markets()
            warmed += 1
        except Exception as e:
            logger.warning("启动预加载 markets 失败（账户 %s）: %s", a.get("id"), e)
    if warmed:
        logger.info("启动预加载 markets 完成：%s 个账户", warmed)


def start_preload_trade_markets_in_background() -> None:
    """后台预加载 markets，避免启动阶段因网络抖动阻塞主进程。"""
    threading.Thread(
        target=preload_trade_markets_on_startup,
        name="preload-trade-markets",
        daemon=True,
    ).start()


def get_exchange_for_account(account: dict[str, Any], *, purpose: str = "default"):
    """
    按账户创建/复用 ccxt 实例（binance / okx / hyperliquid）。

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
    if ex_name == "binance":
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
    elif ex_name == "okx":
        default_type = "swap" if BINANCE_DEFAULT_TYPE == "future" else "spot"
        password = (account.get("password") or "").strip()
        if not password:
            raise ValueError("OKX 账户未配置 API Password / Passphrase")
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        config = {
            "apiKey": api_key,
            "secret": secret,
            "password": password,
            "options": {
                "defaultType": default_type,
                "marginMode": OKX_TD_MODE,
                "defaultMarginMode": OKX_TD_MODE,
            },
            "enableRateLimit": True,
        }
        if proxy:
            config["proxy"] = proxy
        ex = ccxt.okx(config)

        # 默认关闭：每次新建 ccxt 实例都查余额会拖慢首单并增加限频风险；排查 OKX 时设 OKX_DEBUG_BALANCE=true
        if os.getenv("OKX_DEBUG_BALANCE", "").lower() in ("1", "true", "yes", "on"):
            try:
                logger.info("[OKX调试] 查询账户余额…")
                balance = ex.fetch_balance({"type": "swap"})
                usdt_balance = balance.get("USDT", {}).get("free", 0)
                total_balance = balance.get("USDT", {}).get("total", 0)
                logger.info(
                    "[OKX调试] 合约账户余额 - 可用: %s USDT, 总额: %s USDT",
                    usdt_balance,
                    total_balance,
                )
                spot_balance = ex.fetch_balance({"type": "spot"})
                spot_usdt = spot_balance.get("USDT", {}).get("free", 0)
                logger.info("[OKX调试] 现货账户余额: %s USDT", spot_usdt)
            except Exception as e:
                logger.warning("[OKX调试] 余额查询失败: %s", e)

        if USE_TESTNET:
            ex.set_sandbox_mode(True)
    elif ex_name == "hyperliquid":
        default_type = "swap" if BINANCE_DEFAULT_TYPE == "future" else "spot"
        # 复用现有字段：
        # - api_key: walletAddress
        # - secret : privateKey
        ex = ccxt.hyperliquid(
            {
                "walletAddress": api_key,
                "privateKey": secret,
                "options": {"defaultType": default_type},
                "enableRateLimit": True,
            }
        )
    else:
        raise ValueError("暂仅支持 binance / okx / hyperliquid")
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
        fq = float(a.get("fixed_quote_usdt") or 0)
        tpl_cap = float(a.get("template_capital_usdt") or 0)
        if fq <= 0 and tpl_cap <= 0:
            continue
        out.append(a)
    return out


def _webhook_targets_scoped_by_payload(
    payload: dict[str, Any], targets: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]] | None, tuple[Any, int] | None]:
    """
    若 TV 消息体含 account_id（或别名 account），仅对该账户下单；便于一 Webhook 多策略/多账户分流。
    未指定时保持原样：对所有参与 Webhook 的账户广播。
    signal_id 仅作识别与日志透传，不参与路由。
    """
    raw = payload.get("account_id")
    if raw is None:
        raw = payload.get("account")
    aid = str(raw or "").strip()
    if not aid:
        return targets, None
    matched = [a for a in targets if str(a.get("id") or "").strip() == aid]
    if not matched:
        return None, (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        f"account_id={aid!r} 与当前任一 Webhook 账户不匹配。"
                        "请核对控制台「交易账户」中的账户 id（与告警模板助手里下拉框一致），"
                        "并确认该账户已启用、勾选 Webhook、已保存 API 且设置了固定 USDT 或模板本金。"
                    ),
                    "signal_id": payload.get("signal_id"),
                }
            ),
            400,
        )
    return matched, None


def _payload_contracts_amount(payload: dict[str, Any]) -> float | None:
    raw = payload.get("contracts")
    if raw is None:
        raw = payload.get("strategy.order.contracts")
    if raw is None:
        return None
    try:
        v = float(str(raw).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v


def build_order_payload_for_account(
    base: dict[str, Any], account: dict[str, Any]
) -> dict[str, Any]:
    """
    多账户下单支持两种模式：
    1) template_or_fixed：优先按模板本金比例把 TV contracts 换算到各账户；
       若条件不满足则回退账户 fixed_quote_usdt。
    2) follow_tv：优先直接跟随 TradingView 载荷中的 amount / quote_amount / contracts；
       若 TV 未提供有效下单量则回退账户 fixed_quote_usdt。
    若请求体含有效 quote_amount（含控制台「模拟 Webhook」），在两种模式下均优先使用该 USDT 名义；
    quote_amount 必须为「打算花多少 USDT」的数字，勿把张数/标的币数量写入该字段（否则会被当作 USDT）。
    """
    p = dict(base)
    settings = load_bot_settings()
    sizing_mode = str(
        settings.get("webhook_sizing_mode") or "template_or_fixed"
    ).strip().lower()
    if sizing_mode not in ("template_or_fixed", "follow_tv"):
        sizing_mode = "template_or_fixed"

    tv_quote_raw = base.get("quote_amount")
    if tv_quote_raw is not None:
        try:
            qv = float(str(tv_quote_raw).replace(",", "").strip())
        except (TypeError, ValueError):
            qv = None
        if qv is not None and qv > 0:
            p["quote_amount"] = qv
            p.pop("amount", None)
            p["_sizing_mode"] = "payload_quote_amount"
            return p

    tv_contracts = _payload_contracts_amount(base)
    tv_amount_raw = base.get("amount")

    if sizing_mode == "follow_tv":
        if tv_amount_raw is not None:
            try:
                tv_amount = float(tv_amount_raw)
            except (TypeError, ValueError):
                tv_amount = None
            if tv_amount is not None and tv_amount > 0:
                p["amount"] = tv_amount
                p.pop("quote_amount", None)
                p["_sizing_mode"] = "follow_tv_amount"
                return p
        if tv_contracts is not None and tv_contracts > 0:
            p["amount"] = tv_contracts
            p.pop("quote_amount", None)
            p["_sizing_mode"] = "follow_tv_contracts"
            p["_tv_contracts_raw"] = tv_contracts
            return p

    tpl_base = float(settings.get("template_base_capital_usdt") or 0)
    acc_cap = float(account.get("template_capital_usdt") or 0)
    if tpl_base > 0 and tv_contracts is not None and acc_cap > 0:
        scale = acc_cap / tpl_base
        amt = tv_contracts * scale
        if amt <= 0:
            raise ValueError(f"账户「{account.get('remark')}」模板换算后数量无效")
        p["amount"] = amt
        p.pop("quote_amount", None)
        p["_sizing_mode"] = "template_contracts"
        p["_template_scale"] = scale
        p["_template_contracts_raw"] = tv_contracts
        p["_template_base_capital_usdt"] = tpl_base
        return p

    fq = float(account.get("fixed_quote_usdt") or 0)
    if fq <= 0:
        if sizing_mode == "follow_tv":
            raise ValueError(
                f"账户「{account.get('remark')}」未设置固定下单金额（USDT），且 TradingView 载荷缺少有效 amount / quote_amount / contracts"
            )
        raise ValueError(
            f"账户「{account.get('remark')}」未设置固定下单金额（USDT），且模板比例条件未满足"
        )
    p["quote_amount"] = fq
    p.pop("amount", None)
    p["_sizing_mode"] = "fixed_quote"
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
    """BINANCE:BTCUSDT.P -> BTC/USDT;  ETH/USDT:USDT -> ETH/USDT"""
    s = raw.strip().upper()
    # 仅截交易所前缀（如 BINANCE:）：要求 : 前不含 /，避免误伤 ETH/USDT:USDT
    if "/" not in s.split(":", 1)[0]:
        s = re.sub(r"^[^:]+:", "", s)
    s = s.replace(".P", "").replace("-PERP", "")
    # 截结算后缀：ETH/USDT:USDT → ETH/USDT
    s = s.split(":")[0] if ":" in s else s
    if "/" in s:
        a, b = s.split("/", 1)
        return f"{a}/{b}"
    for quote in ("USDT", "BUSD", "USDC"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[: -len(quote)]}/{quote}"
    return s


def _exchange_id(exchange) -> str:
    return str(getattr(exchange, "id", "") or "").lower()


def _is_binance_exchange(exchange) -> bool:
    return _exchange_id(exchange) == "binance"


def _replace_quote(symbol: str, new_quote: str) -> str:
    if "/" not in symbol:
        return symbol
    base, _ = symbol.split("/", 1)
    return f"{base}/{new_quote}"


def resolve_symbol(exchange, symbol: str) -> str:
    """
    统一 symbol。注意：BTCUSDT 会先被归一成 BTC/USDT，而 markets 里 BTC/USDT 是现货；
    若 BINANCE_DEFAULT_TYPE=future，必须优先落到 U 本位永续（如 BTC/USDT:USDT），
    否则下单/拉成交会走错现货市场，合约有成交时现货列表仍为空。
    若币种不存在，自动强制刷新 markets（处理新上线币种）。
    """
    ex_id = _exchange_id(exchange)
    exchange.load_markets()
    if symbol in exchange.markets:
        m = exchange.markets[symbol]
        if (
            BINANCE_DEFAULT_TYPE == "future"
            and m.get("spot")
            and not m.get("contract")
        ):
            candidates = [f"{symbol}:USDT", f"{symbol}:USDC"]
            if ex_id == "hyperliquid":
                usdc_sym = _replace_quote(symbol, "USDC")
                candidates = [
                    f"{usdc_sym}:USDC",
                    f"{symbol}:USDC",
                    f"{symbol}:USDT",
                ]
            for alt in candidates:
                if alt in exchange.markets:
                    return alt
        return symbol
    if BINANCE_DEFAULT_TYPE == "future":
        candidates = [f"{symbol}:USDT", f"{symbol}:USDC"]
        if ex_id == "hyperliquid":
            usdc_sym = _replace_quote(symbol, "USDC")
            candidates = [f"{usdc_sym}:USDC", f"{symbol}:USDC", f"{symbol}:USDT", usdc_sym]
        for alt in candidates:
            if alt in exchange.markets:
                return alt
    if ex_id == "hyperliquid":
        usdc_sym = _replace_quote(symbol, "USDC")
        for alt in (usdc_sym, symbol):
            if alt in exchange.markets:
                return alt
    # 币种不存在时，强制刷新 markets 再试一次（处理新上线币种）
    logger.info("币种 %s 不存在，强制刷新 markets 重试", symbol)
    exchange.load_markets(True)  # True = 强制刷新，不用缓存
    if symbol in exchange.markets:
        return symbol
    for alt in candidates:
        if alt in exchange.markets:
            return alt
    raise ValueError(f"未知交易对: {symbol}")


def quote_to_base_amount(exchange, symbol: str, quote_usdt: float) -> float:
    t = exchange.fetch_ticker(symbol)
    price = float(t.get("last") or t.get("close") or 0)
    if price <= 0:
        raise ValueError("无法从行情获取有效价格")
    return quote_usdt / price


def _okx_linear_swap_base_qty_to_contract_amount(
    exchange, symbol: str, base_coin_qty: float
) -> float:
    """
    OKX USDT 线性永续：REST 的 sz / ccxt 传入的 amount 步长按「张」计，每张 = contractSize(ctVal) 个标的币。
    将「希望成交的标的币数量」换成张数，避免把 87 ENA 误当成 87 张（每张 10 ENA → 870 ENA）。
    """
    if _exchange_id(exchange) != "okx" or BINANCE_DEFAULT_TYPE != "future":
        return float(base_coin_qty)
    mkt = exchange.markets.get(symbol) if getattr(exchange, "markets", None) else None
    if not isinstance(mkt, dict) or not mkt.get("swap") or not mkt.get("linear"):
        return float(base_coin_qty)
    ct = float(mkt.get("contractSize") or 0)
    if ct <= 0:
        return float(base_coin_qty)
    return float(base_coin_qty) / ct


def _okx_linear_swap_amount_to_base(
    exchange,
    symbol: str,
    amount_val: float,
    payload: dict[str, Any],
    *,
    from_position_contracts: bool,
) -> float:
    """
    OKX USDT 线性永续：内部先统一到「标的币数量」；fetch_positions / TV 的 contracts 多为「张」，
    仅在明确来自张数语义时乘以 contractSize(ctVal)。下单前再经 _okx_linear_swap_base_qty_to_contract_amount 换成张数。
    币安等所原样返回。
    """
    if _exchange_id(exchange) != "okx" or BINANCE_DEFAULT_TYPE != "future":
        return amount_val
    mkt = exchange.markets.get(symbol) if getattr(exchange, "markets", None) else None
    if not isinstance(mkt, dict) or not mkt.get("swap") or not mkt.get("linear"):
        return amount_val
    ct = float(mkt.get("contractSize") or 0)
    if ct <= 0:
        return amount_val
    mode = str(payload.get("_sizing_mode") or "")
    if from_position_contracts or mode in (
        "follow_tv_contracts",
        "template_contracts",
    ):
        return float(amount_val) * ct
    return float(amount_val)


def _market_price_for_order(exchange, symbol: str) -> float | None:
    """部分交易所（如 hyperliquid）市价单要求参考价用于滑点。"""
    if _exchange_id(exchange) != "hyperliquid":
        return None
    t = exchange.fetch_ticker(symbol)
    price = float(t.get("last") or t.get("close") or 0)
    if price <= 0:
        raise ValueError("hyperliquid 无法获取有效市价")
    return price


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
      quote_amount: 用多少 USDT 市价（名义 USDT；先换标的币数量再换 OKX「张」）
      amount: 基础币数量（与 quote_amount 二选一，优先 quote_amount）
      reduce_only: true/false（合约平仓）
    OKX USDT 线性永续：TV/持仓张数先乘 ctVal 得到标的币数量，再除以 ctVal 得到 API 张数 sz。
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

    reduce_only = _resolve_reduce_only(payload, action)
    payload["_resolved_reduce_only"] = reduce_only
    quote_amount = payload.get("quote_amount")
    amount = payload.get("amount")

    params: dict[str, Any] = {}
    if reduce_only:
        params["reduceOnly"] = True

    # OKX 合约：tdMode 须与账户一致；默认 cross，逐仓用户在 .env 设 OKX_TD_MODE=isolated
    if _exchange_id(exchange) == "okx" and BINANCE_DEFAULT_TYPE == "future":
        params["marginMode"] = OKX_TD_MODE

    symbol = resolve_symbol(exchange, symbol)
    market_price = _market_price_for_order(exchange, symbol)

    full_flat = _tv_payload_is_strategy_full_flat(payload, reduce_only)
    # 合约仅减仓：
    # - 若策略语义是「全平」（market_position=flat 或 position_size=0），按交易所持仓全平；
    # - 其它减仓场景（部分减仓）按传入 amount/contracts 或 quote 逻辑执行。
    if reduce_only and BINANCE_DEFAULT_TYPE == "future":
        if full_flat:
            pos_abs = _futures_net_position_abs(exchange, symbol)
            if pos_abs is None:
                raise ValueError("查询当前持仓失败，无法执行仅减仓全平")
            if pos_abs <= 0:
                raise ValueError("当前无持仓可平")
            cost = None
            amt = _okx_linear_swap_amount_to_base(
                exchange,
                symbol,
                float(pos_abs),
                payload,
                from_position_contracts=True,
            )
        elif amount is not None:
            cost = None
            amt = _okx_linear_swap_amount_to_base(
                exchange,
                symbol,
                float(amount),
                payload,
                from_position_contracts=False,
            )
        else:
            if quote_amount is not None:
                cost = float(quote_amount)
                amt = None
            else:
                cost = DEFAULT_QUOTE_AMOUNT
                amt = None
    else:
        if quote_amount is not None:
            cost = float(quote_amount)
            amt = None
        elif amount is not None:
            cost = None
            amt = _okx_linear_swap_amount_to_base(
                exchange,
                symbol,
                float(amount),
                payload,
                from_position_contracts=False,
            )
        else:
            cost = DEFAULT_QUOTE_AMOUNT
            amt = None

    try:
        if cost is not None:
            # USDT 名义：统一换为标的币数量再市价（OKX 永续的 tgtCcy 仅适用于现货，合约须用基础币数量）
            if market_price is not None and market_price > 0:
                base_amt = float(cost) / market_price
            else:
                base_amt = quote_to_base_amount(exchange, symbol, float(cost))
            base_coin = (symbol.split("/")[0] if "/" in symbol else "").strip() or "标的"
            order_amt = _okx_linear_swap_base_qty_to_contract_amount(
                exchange, symbol, base_amt
            )
            mkt = exchange.markets.get(symbol) if getattr(exchange, "markets", None) else None
            ct = float(mkt.get("contractSize") or 0) if isinstance(mkt, dict) else 0.0
            if _exchange_id(exchange) == "okx" and ct > 0:
                logger.info(
                    "[下单调试] 市价单: %s %s 标的≈%.8f %s（名义约 %s USDT）→ OKX 张数=%s（每张 %s %s）",
                    symbol,
                    action,
                    base_amt,
                    base_coin,
                    cost,
                    order_amt,
                    ct,
                    base_coin,
                )
            else:
                logger.info(
                    "[下单调试] 市价单: %s %s 标的数量=%.8f %s（名义约 %s USDT）",
                    symbol,
                    action,
                    base_amt,
                    base_coin,
                    cost,
                )
            order = exchange.create_order(
                symbol, "market", action, order_amt, market_price, params
            )
        else:
            base_coin = (symbol.split("/")[0] if "/" in symbol else "").strip() or "标的"
            order_amt = _okx_linear_swap_base_qty_to_contract_amount(
                exchange, symbol, float(amt)
            )
            mkt = exchange.markets.get(symbol) if getattr(exchange, "markets", None) else None
            ct = float(mkt.get("contractSize") or 0) if isinstance(mkt, dict) else 0.0
            if _exchange_id(exchange) == "okx" and ct > 0:
                logger.info(
                    "[下单调试] 市价单: %s %s 标的≈%s %s → OKX 张数=%s（每张 %s %s）",
                    symbol,
                    action,
                    amt,
                    base_coin,
                    order_amt,
                    ct,
                    base_coin,
                )
            else:
                logger.info(
                    "[下单调试] 市价单: %s %s 标的数量=%s %s",
                    symbol,
                    action,
                    amt,
                    base_coin,
                )
            order = exchange.create_order(
                symbol, "market", action, order_amt, market_price, params
            )
        
        logger.info(f"[下单调试] 订单创建成功: {order.get('id', 'N/A')}")
        return order
        
    except Exception as e:
        logger.error(f"[下单调试] 订单创建失败: {e}")
        logger.error(f"[下单调试] 交易对: {symbol}")
        logger.error(f"[下单调试] 动作: {action}")
        logger.error(f"[下单调试] 成本: {cost}")
        logger.error(f"[下单调试] 数量: {amt}")
        logger.error(f"[下单调试] 参数: {params}")
        logger.error(f"[下单调试] 市场价: {market_price}")
        logger.error(f"[下单调试] 交易所: {exchange.id}")
        err_txt = str(e)
        if _exchange_id(exchange) == "okx" and (
            "51008" in err_txt or "Insufficient" in err_txt and "margin" in err_txt
        ):
            logger.warning(
                "[OKX 51008 说明] 交易所判定「该 tdMode 下可用 USDT 保证金」不足。"
                " 常见原因：① USDT 在资金/现货账户，未划入交易账户；② 实际用逐仓但机器人发全仓（在 %s 设 OKX_TD_MODE=isolated 并重启）；"
                "③ 全仓模式下其它持仓/挂单已占用保证金；④ 下单名义过大。当前 OKX_TD_MODE=%s",
                _env,
                OKX_TD_MODE,
            )
        # 重新抛出异常，让上层处理
        raise


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
                "has_password": bool((a.get("password") or "").strip()),
                "fixed_quote_usdt": a.get("fixed_quote_usdt"),
                "template_capital_usdt": a.get("template_capital_usdt"),
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
    with _trailing_stop_tasks_lock:
        trailing_snap = [
            {
                "account_id": aid,
                "remark": (t or {}).get("remark"),
                "exchange": (t or {}).get("exchange"),
                "running": bool((t or {}).get("running")),
            }
            for aid, t in _trailing_stop_tasks.items()
        ]
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
            "template_base_capital_usdt": bs["template_base_capital_usdt"],
            "webhook_sizing_mode": bs["webhook_sizing_mode"],
            "display_est_fee_rate": DISPLAY_EST_FEE_RATE,
            "stop_loss_enabled": bs["stop_loss_enabled"],
            "stop_loss_pct": bs["stop_loss_pct"],
            "trailing_stop_tasks": trailing_snap,
            "hint": "Webhook 下单支持两种模式：模板比例/固定USDT，或直接跟随 TradingView 的 amount/quote_amount/contracts。",
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
    if "webhook_sizing_mode" in body:
        cur["webhook_sizing_mode"] = str(body.get("webhook_sizing_mode") or "").strip().lower()
    if "template_base_capital_usdt" in body and body.get("template_base_capital_usdt") is not None:
        cur["template_base_capital_usdt"] = float(body.get("template_base_capital_usdt"))
    if cur["stop_loss_pct"] <= 0 or cur["stop_loss_pct"] > 50:
        return jsonify({"ok": False, "error": "止损百分比须在 0～50 之间"}), 400
    if cur.get("webhook_sizing_mode") not in ("template_or_fixed", "follow_tv"):
        return jsonify({"ok": False, "error": "下单模式仅支持 template_or_fixed 或 follow_tv"}), 400
    if float(cur.get("template_base_capital_usdt") or 0) < 0:
        return jsonify({"ok": False, "error": "模板本金不能小于 0"}), 400
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
    """保存全部交易账户（需控制台密钥）。留空的 api_key/secret/password 表示保留原值；新行须填写必需密钥。"""
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
        pw = (row.get("password") or "").strip()
        ex_name = (row.get("exchange") or (e.get("exchange") if e else "binance") or "binance").lower()
        if e:
            if not ak:
                ak = (e.get("api_key") or "").strip()
            if not sk:
                sk = (e.get("secret") or "").strip()
            if not pw:
                pw = (e.get("password") or "").strip()
        else:
            if not ak or not sk:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"新账户「{row.get('remark') or rid}」须填写 API Key 与 Secret",
                    }
                ), 400
        if ex_name == "okx" and not pw:
            return jsonify(
                {
                    "ok": False,
                    "error": f"账户「{row.get('remark') or rid}」使用 OKX 时必须填写 Passphrase",
                }
            ), 400
        fq = float(row.get("fixed_quote_usdt") or 0)
        tcap = float(row.get("template_capital_usdt") or 0)
        if fq <= 0 and tcap <= 0:
            return jsonify(
                {
                    "ok": False,
                    "error": f"账户「{row.get('remark') or rid}」固定USDT或模板本金至少一项须大于 0",
                }
            ), 400
        merged.append(
            {
                "id": rid,
                "remark": ((row.get("remark") or "").strip() or "未命名"),
                "exchange": ex_name,
                "api_key": ak,
                "secret": sk,
                "password": pw,
                "fixed_quote_usdt": fq,
                "template_capital_usdt": tcap,
                "enabled": bool(row.get("enabled", True)),
                "webhook_enabled": bool(row.get("webhook_enabled", True)),
            }
        )
        if merged[-1]["exchange"] not in ("binance", "okx", "hyperliquid"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": f"账户「{row.get('remark') or rid}」交易所仅支持 binance、okx 或 hyperliquid",
                    }
                ),
                400,
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


def _okx_fetch_balance(exchange, account_type: str) -> dict[str, Any]:
    try:
        if account_type == "spot":
            raw = exchange.privateGetAssetBalances({})
            data = raw.get("data") if isinstance(raw, dict) else None
            rows = data if isinstance(data, list) else []
            out: dict[str, Any] = {"info": rows, "free": {}, "used": {}, "total": {}}
            for row in rows:
                ccy = str(row.get("ccy") or "").upper()
                if not ccy:
                    continue
                avail = _to_float(row.get("availBal") or row.get("availEq"))
                bal = _to_float(row.get("bal") or row.get("eq"))
                frozen = _to_float(row.get("frozenBal"))
                out[ccy] = {
                    "free": avail,
                    "used": frozen,
                    "total": bal,
                }
                if avail is not None:
                    out["free"][ccy] = avail
                if frozen is not None:
                    out["used"][ccy] = frozen
                if bal is not None:
                    out["total"][ccy] = bal
            return out

        raw = exchange.privateGetAccountBalance({})
        data = raw.get("data") if isinstance(raw, dict) else None
        rows = data if isinstance(data, list) else []
        details: list[dict[str, Any]] = []
        if rows:
            first = rows[0]
            details = first.get("details") if isinstance(first, dict) else []
            if not isinstance(details, list):
                details = []
        out: dict[str, Any] = {"info": {"details": details}, "free": {}, "used": {}, "total": {}}
        for row in details:
            ccy = str(row.get("ccy") or "").upper()
            if not ccy:
                continue
            avail = _to_float(row.get("availBal") or row.get("availEq"))
            eq = _to_float(row.get("eq") or row.get("cashBal") or row.get("bal"))
            frozen = None
            if avail is not None and eq is not None:
                frozen = max(eq - avail, 0.0)
            out[ccy] = {
                "free": avail,
                "used": frozen,
                "total": eq,
            }
            if avail is not None:
                out["free"][ccy] = avail
            if frozen is not None:
                out["used"][ccy] = frozen
            if eq is not None:
                out["total"][ccy] = eq
        return out
    except Exception as e:
        logger.exception("OKX 查询余额失败，账户类型: %s", account_type)
        raise ValueError(f"OKX 查询余额失败: {e}") from e


def normalize_usdt_balance(bal: dict[str, Any]) -> dict[str, Any]:
    """
    合约稳定币余额。优先 USDT，回退 USDC。
    fapi/v2 等接口的原始响应有时是「数组」，ccxt 放在 bal['info'] 里为 list，
    之前只处理 dict 会落到 default_zero。依次：统一结构 → 聚合 → info 为 list → info 为 dict。
    """
    stable = "USDT" if bal.get("USDT") else ("USDC" if bal.get("USDC") else None)
    if stable:
        u = bal.get(stable) or {}
        f, used, total = _to_float(u.get("free")), _to_float(u.get("used")), _to_float(
            u.get("total")
        )
        if f is not None or used is not None or total is not None:
            return {"free": f, "used": used, "total": total, "source": "unified", "currency": stable}

    agg_f = bal.get("free") if isinstance(bal.get("free"), dict) else {}
    agg_u = bal.get("used") if isinstance(bal.get("used"), dict) else {}
    agg_t = bal.get("total") if isinstance(bal.get("total"), dict) else {}
    if isinstance(agg_f, dict):
        stable = "USDT" if "USDT" in agg_f else ("USDC" if "USDC" in agg_f else None)
        if stable:
            return {
                "free": _to_float(agg_f.get(stable)),
                "used": _to_float(agg_u.get(stable)),
                "total": _to_float(agg_t.get(stable)),
                "source": "aggregated",
                "currency": stable,
            }

    info = bal.get("info")
    if isinstance(info, list):
        for row in info:
            asset = str(row.get("asset", "")).upper()
            if asset not in ("USDT", "USDC"):
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
                "currency": asset,
            }

    if isinstance(info, dict):
        assets = info.get("assets")
        if isinstance(assets, list):
            for row in assets:
                asset = str(row.get("asset", "")).upper()
                if asset in ("USDT", "USDC"):
                    return {
                        "free": _to_float(row.get("availableBalance")),
                        "used": _to_float(row.get("initialMargin")),
                        "total": _to_float(
                            row.get("marginBalance") or row.get("walletBalance")
                        ),
                        "source": "assets",
                        "currency": asset,
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
        "currency": "USDT",
        "note": "未能解析合约原始 info；若你实际在现货钱包，请看返回里的 usdt_spot",
    }


def normalize_spot_usdt(bal: dict[str, Any]) -> dict[str, Any]:
    """现货稳定币余额。优先 USDT，回退 USDC。"""
    stable = "USDT" if bal.get("USDT") else ("USDC" if bal.get("USDC") else "USDT")
    u = bal.get(stable) or {}
    free, used, tot = (
        _to_float(u.get("free")),
        _to_float(u.get("used")),
        _to_float(u.get("total")),
    )
    if free is not None or used is not None or tot is not None:
        return {"free": free, "used": used, "total": tot, "source": "spot", "currency": stable}
    return {"free": 0.0, "used": 0.0, "total": 0.0, "source": "spot_empty", "currency": stable}


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
                ex_id = _exchange_id(ex)
                usdt_future: dict[str, Any] = {}
                usdt_spot: dict[str, Any] = {}
                try:
                    fut_type = "swap" if ex_id in ("hyperliquid", "okx") else "future"
                    if ex_id == "okx":
                        usdt_future = normalize_usdt_balance(
                            _okx_fetch_balance(ex, fut_type)
                        )
                    else:
                        fut_params: dict[str, Any] = {"type": fut_type}
                        if ex_id == "hyperliquid":
                            fut_params["user"] = str(a.get("api_key") or "")
                        usdt_future = normalize_usdt_balance(
                            ex.fetch_balance(fut_params)
                        )
                except Exception as e:
                    logger.warning("查询合约余额失败: %s", e)
                    usdt_future = {"error": str(e), "source": "future_error"}
                try:
                    if ex_id == "okx":
                        usdt_spot = normalize_spot_usdt(
                            _okx_fetch_balance(ex, "spot")
                        )
                    else:
                        spot_params: dict[str, Any] = {"type": "spot"}
                        if ex_id == "hyperliquid":
                            spot_params["user"] = str(a.get("api_key") or "")
                        usdt_spot = normalize_spot_usdt(
                            ex.fetch_balance(spot_params)
                        )
                except Exception as e:
                    logger.warning("查询现货余额失败: %s", e)
                    usdt_spot = {"error": str(e), "source": "spot_error"}
                if ex_id in ("hyperliquid", "okx"):
                    fut_total = _to_float(usdt_future.get("total")) or 0.0
                    spot_total = _to_float(usdt_spot.get("total")) or 0.0
                    if fut_total > 0 and fut_total >= spot_total:
                        primary = usdt_future
                    elif spot_total > 0:
                        primary = usdt_spot
                    else:
                        primary = usdt_future
                else:
                    primary = usdt_future if BINANCE_DEFAULT_TYPE == "future" else usdt_spot
                rows.append(
                    {
                        "account_id": a["id"],
                        "remark": a.get("remark"),
                        "exchange": a.get("exchange") or "binance",
                        "fixed_quote_usdt": a.get("fixed_quote_usdt"),
                        "template_capital_usdt": a.get("template_capital_usdt"),
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
            "hint": "多账户：默认展示第一个账户摘要；完整结果见 accounts 数组。Binance/OKX/Hyperliquid 的合约与现货资金池可能分开显示。",
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
    """浏览器手动测试下单；须指定 account_id。默认走账户模板/固定USDT下单逻辑。"""
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
        account_trade_lock = _get_account_trade_lock(str(a["id"]))
        sizing_mode = None
        op: dict[str, Any] | None = None
        if use_base_amount:
            t_exec_start = time.time()
            with account_trade_lock:
                created = place_order(ex, payload)
            order = _merge_order_with_fetch(ex, created)
            ro = bool(payload.get("_resolved_reduce_only", False))
            sizing_mode = "manual_amount"
        else:
            op = build_order_payload_for_account(payload, a)
            t_exec_start = time.time()
            with account_trade_lock:
                created = place_order(ex, op)
            order = _merge_order_with_fetch(ex, created)
            ro = bool(op.get("_resolved_reduce_only", False))
            sizing_mode = op.get("_sizing_mode")
            payload["_resolved_reduce_only"] = op.get("_resolved_reduce_only")
            payload["reduce_only"] = op.get("_resolved_reduce_only")
        manual_quote_usdt: float | None = None
        if not use_base_amount and op is not None:
            if op.get("quote_amount") is not None:
                manual_quote_usdt = _to_float(op.get("quote_amount"))
            elif op.get("_sizing_mode") == "fixed_quote":
                manual_quote_usdt = _to_float(a.get("fixed_quote_usdt"))
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
            and _is_binance_exchange(ex)
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
            quote_usdt=manual_quote_usdt,
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
                "used_quote_usdt": None
                if use_base_amount
                else (op.get("quote_amount") if op and op.get("quote_amount") is not None else None),
                "sizing_mode": sizing_mode,
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
    include_archive = str(request.args.get("include_archive") or "").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    with _LOG_LOCK:
        items = list(_signal_log)
        total = len(_signal_log)
    if include_archive:
        archived = _load_persisted_jsonl_list(_SIGNAL_ARCHIVE_JSONL_PATH)
        items = items + archived
        total += len(archived)
    return jsonify(
        {
            "ok": True,
            "total_stored": total,
            "include_archive": include_archive,
            "hint": "热日志保存在 data/signal_log.jsonl；归档为 data/signal_log.archive.jsonl。",
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
        _signal_log = deque()
        _save_signal_log()
        if _SIGNAL_ARCHIVE_JSONL_PATH.is_file():
            _SIGNAL_ARCHIVE_JSONL_PATH.unlink(missing_ok=True)
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
    include_archive = str(request.args.get("include_archive") or "").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    try:
        limit = min(
            max(int(request.args.get("limit") or 2000), 1), ACCOUNT_LOG_QUERY_MAX
        )
    except ValueError:
        limit = 2000
    with _LOG_LOCK:
        snapshot = list(_account_log)
        total_stored = len(_account_log)
    archived_count = 0
    if include_archive:
        archived = _load_persisted_jsonl_list(_EXECUTION_ARCHIVE_JSONL_PATH)
        archived_count = len(archived)
        snapshot = snapshot + archived
        total_stored += archived_count
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
    hint = "热日志保存在 data/execution_log.jsonl；超出上限会归档到 data/execution_log.archive.jsonl。时间 UTC。"
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
            "archived_count": archived_count,
            "include_archive": include_archive,
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
            _account_log = deque(
                e for e in _account_log if str(e.get("account_id")) != aid
            )
            removed_hot = before - len(_account_log)
            removed_archive = _delete_rows_in_jsonl(
                _EXECUTION_ARCHIVE_JSONL_PATH,
                lambda row: str((row or {}).get("account_id")) == aid,
            )
            removed = removed_hot + removed_archive
        else:
            removed = len(_account_log)
            _account_log = deque()
            if _EXECUTION_ARCHIVE_JSONL_PATH.is_file():
                _EXECUTION_ARCHIVE_JSONL_PATH.unlink(missing_ok=True)
        _save_execution_log()
        remaining = len(_account_log)
    return jsonify(
        {
            "ok": True,
            "removed": removed,
            "remaining": remaining,
        }
    )


def _parse_trailing_blacklist(raw: Any) -> set[str]:
    out: set[str] = set()
    if raw is None:
        return out
    if isinstance(raw, list):
        for x in raw:
            s = str(x).strip()
            if s:
                out.add(s)
        return out
    s = str(raw).strip()
    if not s:
        return out
    for part in re.split(r"[\s,;，；]+", s):
        p = part.strip()
        if p:
            out.add(p)
    return out


def _trailing_stop_thread_main(
    account: dict[str, Any],
    params: dict[str, Any],
    stop_event: threading.Event,
    task: dict[str, Any],
) -> None:
    aid = str(account.get("id") or "")
    try:
        ex = get_exchange_for_account(account, purpose="trade")
        lock = _get_account_trade_lock(aid)
        with lock:
            ex.load_markets()

        def _hook(msg: str) -> None:
            task["last_status"] = str(msg)[:2000]

        # 旧任务 params 里可能没有 exchange_algo_type，曾误默认 stop_market → 选「限价追踪」仍走 STOP_MARKET 市价腿
        _raw_eat = params.get("exchange_algo_type")
        _tpm = str(params.get("take_profit_mode") or "").strip().lower()
        if _raw_eat is None or str(_raw_eat).strip() == "":
            resolved_eat = "stop_limit" if _tpm == "track" else "stop_market"
        else:
            resolved_eat = str(_raw_eat).strip().lower()
        if resolved_eat not in ("stop_market", "stop_limit"):
            resolved_eat = "stop_market"
        params["exchange_algo_type"] = resolved_eat

        worker = TrailingStopWorker(
            ex,
            account_id=aid,
            trade_lock=lock,
            normalize_symbol_fn=normalize_symbol,
            stop_loss_pct=float(params["stop_loss_pct"]),
            low_trail_stop_loss_pct=float(params["low_trail_stop_loss_pct"]),
            trail_stop_loss_pct=float(params["trail_stop_loss_pct"]),
            higher_trail_stop_loss_pct=float(params["higher_trail_stop_loss_pct"]),
            low_trail_profit_threshold=float(params["low_trail_profit_threshold"]),
            first_trail_profit_threshold=float(params["first_trail_profit_threshold"]),
            second_trail_profit_threshold=float(params["second_trail_profit_threshold"]),
            feishu_webhook=params.get("feishu_webhook") or None,
            blacklist=params.get("blacklist") or set(),
            status_hook=_hook,
            use_last_price=bool(params.get("use_last_price")),
            use_last_price_only=bool(params.get("use_last_price_only")),
            close_mode=str(params.get("close_mode") or "market"),
            limit_offset_bps=float(params.get("limit_offset_bps", 25)),
            trailing_exec=str(params.get("trailing_exec") or "signal"),
            exchange_algo_type=resolved_eat,
        )
        worker.restore_existing_algos()
        _idle_sec = float(params.get("idle_no_position_sec", 10))
        _monitor_int = float(params["monitor_interval"])
        if _idle_sec <= 0 or _idle_sec < _monitor_int:
            _idle_sec = max(_monitor_int * 2, 5)
        worker.run_loop(
            stop_event,
            _monitor_int,
            _idle_sec,
        )
    except Exception as e:
        logger.exception("移动止盈线程异常 账户=%s", aid)
        task["last_status"] = f"线程异常退出: {e}"
    finally:
        with _trailing_stop_tasks_lock:
            cur = _trailing_stop_tasks.get(aid)
            if cur is task:
                cur["running"] = False
                cur["stopped_at"] = datetime.now(timezone.utc).isoformat()
                # 线程结束即从注册表移除，避免前端长期显示「已停止」的僵尸任务
                _trailing_stop_tasks.pop(aid, None)


@app.get("/trailing-stop")
def trailing_stop_dashboard():
    return send_from_directory(app.static_folder, "trailing_stop.html")


@app.get("/api/trailing-stop/status")
def api_trailing_stop_status():
    bad = dashboard_auth_response_if_invalid(allow_viewer=True)
    if bad:
        return bad
    role = dashboard_auth_role()
    with _trailing_stop_tasks_lock:
        snap = dict(_trailing_stop_tasks)
    out: list[dict[str, Any]] = []
    for aid, t in snap.items():
        params = t.get("params")
        if role == "viewer" and isinstance(params, dict):
            p2 = dict(params)
            if p2.get("feishu_webhook"):
                p2["feishu_webhook"] = "(已配置，访客不可见)"
            params = p2
        out.append(
            {
                "account_id": aid,
                "remark": t.get("remark"),
                "exchange": t.get("exchange"),
                "running": bool(t.get("running")),
                "started_at": t.get("started_at"),
                "stopped_at": t.get("stopped_at"),
                "last_status": t.get("last_status"),
                "params": params,
            }
        )
    return jsonify({"ok": True, "tasks": out})


@app.post("/api/trailing-stop/start")
def api_trailing_stop_start():
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    if BINANCE_DEFAULT_TYPE != "future":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "移动止盈仅支持永续合约模式",
                    "hint": "请在 .env 设置 BINANCE_DEFAULT_TYPE=future 并重启（币安 U 本位；OKX 将映射为 SWAP）。",
                }
            ),
            400,
        )
    body = request.get_json(silent=True) or {}
    aid = str(body.get("account_id") or "").strip()
    if not aid:
        return jsonify({"ok": False, "error": "请指定 account_id"}), 400
    acc = _account_by_id(aid)
    if not acc:
        return jsonify({"ok": False, "error": "找不到该交易账户"}), 400
    exn = (acc.get("exchange") or "binance").lower()
    if exn == "hyperliquid":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "移动止盈暂不支持 Hyperliquid",
                    "hint": "请使用币安或 OKX 账户。",
                }
            ),
            400,
        )
    if exn not in ("binance", "okx"):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "移动止盈仅支持币安或 OKX 账户",
                    "hint": "其它交易所请使用自带止盈止损或其它工具。",
                }
            ),
            400,
        )
    if not (acc.get("api_key") or "").strip() or not (acc.get("secret") or "").strip():
        return jsonify({"ok": False, "error": "该账户未配置 API Key / Secret"}), 400

    try:
        monitor_interval = float(body.get("monitor_interval", 1))
    except (TypeError, ValueError):
        monitor_interval = 1.0
    if monitor_interval < 0.3 or monitor_interval > 120:
        return jsonify({"ok": False, "error": "monitor_interval 须在 0.3～120 秒之间"}), 400

    try:
        idle_no_position_sec = float(body.get("idle_no_position_sec", 10))
    except (TypeError, ValueError):
        idle_no_position_sec = 10.0
    if idle_no_position_sec < 5 or idle_no_position_sec > 120:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "idle_no_position_sec（无持仓轮询间隔）须在 5～120 秒之间",
                }
            ),
            400,
        )

    raw_last = body.get("use_last_price")
    if isinstance(raw_last, str):
        use_last_price = raw_last.strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
            "last",
        )
    else:
        use_last_price = bool(raw_last)

    raw_only = body.get("use_last_price_only")
    if isinstance(raw_only, str):
        use_last_price_only = raw_only.strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    else:
        use_last_price_only = bool(raw_only)
    if not use_last_price:
        use_last_price_only = False

    exchange_algo_type = "stop_market"
    tpm_in = body.get("take_profit_mode")
    if tpm_in is not None:
        t = str(tpm_in).strip().lower()
        if t in ("market", "市价", "m"):
            close_mode_str = "market"
            trailing_exec = "signal"
        elif t in ("track", "限价", "追踪", "limit_track", "l"):
            # 所里用条件 STOP+限价；程序代为平仓时仅走 IOC 限价，不转市价（见 TrailingStopWorker.close_position）
            close_mode_str = "limit_ioc"
            trailing_exec = "exchange_stop"
            exchange_algo_type = "stop_limit"
        else:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "take_profit_mode 仅支持 market（市价止盈/平仓）或 track（条件限价 STOP：触发后以 GTC 限价委托，按有仓间隔撤挂刷新）",
                    }
                ),
                400,
            )
        try:
            limit_offset_bps = float(body.get("limit_offset_bps", 25))
        except (TypeError, ValueError):
            limit_offset_bps = 25.0
        if limit_offset_bps < 0 or limit_offset_bps > 500:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "limit_offset_bps 须在 0～500",
                    }
                ),
                400,
            )
        take_profit_mode = "track" if trailing_exec == "exchange_stop" else "market"
    else:
        raw_close = body.get("close_mode") or "market"
        close_mode_str = str(raw_close).strip().lower()
        if close_mode_str == "limit":
            close_mode_str = "limit_ioc"
        if close_mode_str not in ("market", "limit_ioc"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "close_mode 仅支持 market（市价）或 limit（限价 IOC）",
                    }
                ),
                400,
            )

        raw_exec = body.get("trailing_exec") or "signal"
        trailing_exec = str(raw_exec).strip().lower()
        if trailing_exec in ("exchange", "stop", "algo"):
            trailing_exec = "exchange_stop"
        if trailing_exec not in ("signal", "exchange_stop"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "trailing_exec 仅支持 signal（轮询触发平仓）或 exchange_stop（交易所 STOP 线按间隔刷新）",
                    }
                ),
                400,
            )
        try:
            limit_offset_bps = float(body.get("limit_offset_bps", 25))
        except (TypeError, ValueError):
            limit_offset_bps = 25.0
        if limit_offset_bps < 0 or limit_offset_bps > 500:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "limit_offset_bps 须在 0～500（基点，25≈触发与限价相差 0.25%）",
                    }
                ),
                400,
            )
        take_profit_mode = (
            "track"
            if trailing_exec == "exchange_stop"
            else ("market" if close_mode_str == "market" else "limit_ioc")
        )
        exchange_algo_type = str(body.get("exchange_algo_type") or "stop_market").strip().lower()
        if exchange_algo_type not in ("stop_market", "stop_limit"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "exchange_algo_type 仅支持 stop_market 或 stop_limit（仅 trailing_exec=exchange_stop 时生效）",
                    }
                ),
                400,
            )
        if trailing_exec != "exchange_stop":
            exchange_algo_type = "stop_market"

    def _req_pct(name: str, default: float | None = None) -> float:
        if name not in body and default is not None:
            return float(default)
        v = body.get(name)
        if v is None:
            raise ValueError(f"缺少数字字段 {name}")
        return float(v)

    try:
        p = {
            "monitor_interval": monitor_interval,
            "idle_no_position_sec": idle_no_position_sec,
            "use_last_price": use_last_price,
            "use_last_price_only": use_last_price_only,
            "take_profit_mode": take_profit_mode,
            "close_mode": close_mode_str,
            "limit_offset_bps": limit_offset_bps,
            "trailing_exec": trailing_exec,
            "exchange_algo_type": exchange_algo_type,
            "stop_loss_pct": _req_pct("stop_loss_pct", 50),
            "low_trail_stop_loss_pct": _req_pct("low_trail_stop_loss_pct", 0.2),
            "trail_stop_loss_pct": _req_pct("trail_stop_loss_pct", 0.2),
            "higher_trail_stop_loss_pct": _req_pct("higher_trail_stop_loss_pct", 0.3),
            "low_trail_profit_threshold": _req_pct("low_trail_profit_threshold", 0.9),
            "first_trail_profit_threshold": _req_pct("first_trail_profit_threshold", 1),
            "second_trail_profit_threshold": _req_pct("second_trail_profit_threshold", 1.5),
        }
    except (TypeError, ValueError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if p["stop_loss_pct"] <= 0 or p["stop_loss_pct"] > 50:
        return jsonify({"ok": False, "error": "stop_loss_pct 须在 0～50 之间（不含 0）"}), 400
    for key, label in (
        ("trail_stop_loss_pct", "第一档回撤比例"),
        ("higher_trail_stop_loss_pct", "第二档回撤比例"),
    ):
        v = float(p[key])
        if v <= 0 or v > 1:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": f"{label}（{key}）须在 0～1 之间，例如 0.3 表示从最高浮盈回撤 30%",
                    }
                ),
                400,
            )

    lo = float(p["low_trail_profit_threshold"])
    fi = float(p["first_trail_profit_threshold"])
    se = float(p["second_trail_profit_threshold"])
    if not (lo < fi < se):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "分档浮盈阈值须严格递增：低档 < 第一档 < 第二档（例如 0.9 < 1.0 < 1.5）",
                }
            ),
            400,
        )

    feishu = body.get("feishu_webhook")
    if feishu is not None and str(feishu).strip():
        p["feishu_webhook"] = str(feishu).strip()
    else:
        p["feishu_webhook"] = None
    p["blacklist"] = _parse_trailing_blacklist(body.get("blacklist"))

    with _trailing_stop_tasks_lock:
        existing = _trailing_stop_tasks.get(aid)
        if existing and existing.get("running"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "该账户已在运行移动止盈",
                        "hint": "请先停止后再启动。",
                    }
                ),
                409,
            )
        stop_ev = threading.Event()
        task: dict[str, Any] = {
            "stop": stop_ev,
            "thread": None,
            "running": True,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "stopped_at": None,
            "remark": acc.get("remark"),
            "exchange": acc.get("exchange") or "binance",
            "last_status": "启动中…",
            "params": {k: v for k, v in p.items() if k != "blacklist"}
            | {"blacklist": sorted(p["blacklist"])},
        }
        th = threading.Thread(
            target=_trailing_stop_thread_main,
            args=(acc, p, stop_ev, task),
            name=f"trailing-stop-{aid[:8]}",
            daemon=True,
        )
        task["thread"] = th
        _trailing_stop_tasks[aid] = task
        th.start()
    return jsonify({"ok": True, "account_id": aid, "started_at": task["started_at"]})


@app.post("/api/trailing-stop/stop")
def api_trailing_stop_stop():
    bad = dashboard_auth_response_if_invalid()
    if bad:
        return bad
    body = request.get_json(silent=True) or {}
    aid = str(body.get("account_id") or "").strip()
    if not aid:
        return jsonify({"ok": False, "error": "请指定 account_id"}), 400
    with _trailing_stop_tasks_lock:
        task = _trailing_stop_tasks.get(aid)
        if not task or not task.get("running"):
            return jsonify({"ok": False, "error": "该账户没有运行中的移动止盈"}), 400
        stop: threading.Event = task["stop"]
        th: threading.Thread | None = task.get("thread")
        stop.set()
    if th is not None:
        th.join(timeout=30.0)
    with _trailing_stop_tasks_lock:
        # 线程 finally 里可能已 pop；幂等清理
        _trailing_stop_tasks.pop(aid, None)
    return jsonify({"ok": True, "account_id": aid})


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
                and _is_binance_exchange(ex)
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
        logger.error("无可用交易账户（请添加账户、填写密钥、配置模板本金或固定金额，并勾选参与 Webhook）")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "无可用交易账户：请在控制台添加账户、保存 API，并设置账户模板本金或固定下单 USDT 与 Webhook 跟单。",
                }
            ),
            503,
        )

    t_recv = time.time()
    payload = parse_body()
    scoped, scope_err = _webhook_targets_scoped_by_payload(payload, targets)
    if scope_err is not None:
        return scope_err[0], scope_err[1]
    targets = scoped
    if not targets:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Webhook 账户列表在按 account_id 筛选后为空",
                }
            ),
            400,
        )
    bs_wh = load_bot_settings()
    action_raw = (
        payload.get("action")
        or payload.get("side")
        or payload.get("strategy.order.action")
        or ""
    )
    action_wh = str(action_raw).lower().strip()
    # 仅开仓模式：仅从消息体读取
    if bool(payload.get("open_only")) and action_wh in ("buy", "sell"):
        try:
            if _resolve_reduce_only(payload, action_wh):
                logger.info(
                    "Webhook 已启用「仅开仓」：忽略本次减仓/平仓信号 action=%s",
                    action_wh,
                )
                return jsonify(
                    {
                        "ok": True,
                        "skipped": True,
                        "reason": "webhook_open_only",
                        "message": "已启用仅开仓：本次为减仓/平仓类信号，未下单（止盈请用移动止盈）",
                    }
                )
        except Exception as e:
            logger.warning("仅开仓判断 reduce_only 时异常，继续走正常下单: %s", e)
    if WEBHOOK_LOG_PAYLOAD:
        logger.info("收到 TV 载荷: %s", json.dumps(payload, ensure_ascii=False)[:500])
    else:
        logger.info(
            "收到 TV 载荷: symbol=%s action=%s",
            payload.get("symbol") or payload.get("ticker"),
            payload.get("action") or payload.get("side"),
        )
    t_trade_start = time.time()
    receive_signal_ms = round((t_trade_start - t_recv) * 1000, 2)

    results: list[dict[str, Any] | None] = [None] * len(targets)
    post_tasks: list[dict[str, Any] | None] = [None] * len(targets)
    payload_for_log = dict(payload)

    def _submit_one(idx: int, account: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any] | None, bool]:
        ex = get_exchange_for_account(account, purpose="trade")
        op = build_order_payload_for_account(dict(payload), account)
        with _get_account_trade_lock(str(account.get("id") or "")):
            order = place_order(ex, op)
        ro = bool(op.get("_resolved_reduce_only", False))
        result_row = {
            "account_id": account["id"],
            "remark": account.get("remark"),
            "ok": True,
            "order": order,
            "fixed_quote_usdt": account.get("fixed_quote_usdt"),
            "used_quote_usdt": op.get("quote_amount"),
            "sizing_mode": op.get("_sizing_mode"),
            "template_scale": op.get("_template_scale"),
            "stop_loss_order": None,
            "stop_loss_error": None,
            "post_process": f"delayed_{int(WEBHOOK_POST_DELAY_SEC)}s",
        }
        task_row = {
            "exchange": ex,
            "order": order,
            "reduce_only": ro,
            "payload": dict(op),
        }
        return idx, result_row, task_row, ("_resolved_reduce_only" in op)

    workers = max(1, min(len(targets), max(1, int(WEBHOOK_TRADE_MAX_WORKERS))))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="webhook-trade") as pool:
        fut_map = {
            pool.submit(_submit_one, idx, account): (idx, account)
            for idx, account in enumerate(targets)
        }
        for fut in as_completed(fut_map):
            idx, account = fut_map[fut]
            try:
                i, result_row, task_row, has_resolved = fut.result()
                results[i] = result_row
                post_tasks[i] = task_row
                if has_resolved and "_resolved_reduce_only" not in payload_for_log:
                    ro = bool(task_row.get("reduce_only")) if task_row else False
                    payload_for_log["_resolved_reduce_only"] = ro
                    payload_for_log["reduce_only"] = ro
            except Exception as e:
                logger.exception("账户 %s 下单失败", account.get("id"))
                results[idx] = {
                    "account_id": account["id"],
                    "remark": account.get("remark"),
                    "ok": False,
                    "error": str(e),
                }
                post_tasks[idx] = None

    final_results: list[dict[str, Any]] = [
        r
        if isinstance(r, dict)
        else {
            "account_id": targets[i]["id"],
            "remark": targets[i].get("remark"),
            "ok": False,
            "error": "下单结果为空",
        }
        for i, r in enumerate(results)
    ]
    t_trade_done = time.time()
    execute_trade_ms = round((t_trade_done - t_trade_start) * 1000, 2)
    total_trade_ms = round((t_trade_done - t_recv) * 1000, 2)
    _get_webhook_finalize_executor().submit(
        _finalize_webhook_results_after_delay,
        delay_sec=WEBHOOK_POST_DELAY_SEC,
        t_recv=t_recv,
        payload=payload_for_log,
        base_results=final_results,
        post_tasks=post_tasks,
        receive_signal_ms=receive_signal_ms,
        execute_trade_ms=execute_trade_ms,
        total_trade_ms=total_trade_ms,
        completed_at=t_trade_done,
    )
    ok_all = all(r.get("ok") for r in final_results)
    # 始终 200，避免 TradingView 因 4xx 反复重试；成功与否看 ok 与 results。
    return jsonify({"ok": ok_all, "results": final_results})


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
    if PRELOAD_MARKETS_ON_STARTUP:
        start_preload_trade_markets_in_background()
    _get_webhook_finalize_executor()
    port = int(os.getenv("PORT", "5000"))
    # threaded=True：控制台查余额/拉日志等慢请求不阻塞另一条线程处理 /webhook 下单
    threaded = os.getenv("FLASK_THREADED", "true").lower() in ("1", "true", "yes", "on")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=threaded)


if __name__ == "__main__":
    main()
