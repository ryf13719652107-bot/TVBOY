# -*- coding: utf-8 -*-
"""
WebSocket 价格订阅测试脚本
"""
import asyncio
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


async def test_binance_websocket():
    """测试币安 WebSocket 连接"""
    try:
        import websockets
    except ImportError:
        logger.error("请先安装 websockets: pip install websockets")
        return

    # 订阅 BSB 和 BTC
    symbols = ['bsbusdt', 'btcusdt']
    streams = '/'.join([f"{s}@aggTrade/{s}@markPrice" for s in symbols])
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    logger.info(f"连接 WebSocket: {url}")
    logger.info(f"订阅币种: {symbols}")

    message_count = 0
    start_time = time.time()

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            logger.info("✅ WebSocket 连接成功!")

            async for message in ws:
                data = json.loads(message)
                stream = data.get('stream', '')
                payload = data.get('data', {})

                message_count += 1
                elapsed = time.time() - start_time

                # 解析价格
                if '@aggTrade' in stream:
                    symbol = stream.replace('@aggTrade', '').upper()
                    price = payload.get('p', 'N/A')
                    trade_time = payload.get('T', 0)
                    delay = (time.time() - trade_time/1000) * 1000 if trade_time else 0
                    logger.info(f"📈 {symbol} 最新成交: {price} (延迟: {delay:.1f}ms)")

                elif '@markPrice' in stream:
                    symbol = stream.replace('@markPrice', '').upper()
                    mark_price = payload.get('p', 'N/A')
                    logger.info(f"📊 {symbol} 标记价格: {mark_price}")

                # 每10秒输出统计
                if message_count % 100 == 0:
                    rate = message_count / elapsed if elapsed > 0 else 0
                    logger.info(f"📊 统计: {message_count} 条消息, {rate:.1f} 条/秒")

    except Exception as e:
        logger.error(f"❌ WebSocket 错误: {e}")


def test_websocket_module():
    """测试 WebSocket 模块"""
    try:
        from websocket_price_feed import BinanceWebSocketPriceFeed

        logger.info("测试 WebSocket 模块...")

        # 创建实例
        ws = BinanceWebSocketPriceFeed(
            symbols=['BSBUSDT', 'BTCUSDT'],
            on_price_update=lambda s, d: logger.info(f"回调: {s} = {d}")
        )

        # 启动
        ws.start()
        logger.info("✅ WebSocket 模块启动成功")

        # 等待接收数据
        time.sleep(5)

        # 获取价格
        price = ws.get_price('BSBUSDT')
        logger.info(f"BSBUSDT 价格: {price}")

        # 获取统计
        stats = ws.get_stats()
        logger.info(f"统计: {stats}")

        # 停止
        ws.stop()
        logger.info("✅ WebSocket 模块停止成功")

    except ImportError as e:
        logger.error(f"❌ 导入错误: {e}")
        logger.info("请先安装依赖: pip install websockets")
    except Exception as e:
        logger.error(f"❌ 测试错误: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "direct":
        # 直接测试 WebSocket 连接
        logger.info("=== 直接测试 WebSocket 连接 ===")
        asyncio.run(test_binance_websocket())
    else:
        # 测试模块
        logger.info("=== 测试 WebSocket 模块 ===")
        test_websocket_module()
