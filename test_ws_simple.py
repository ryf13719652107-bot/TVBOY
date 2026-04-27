# -*- coding: utf-8 -*-
"""
简化版 WebSocket 测试
"""
import asyncio
import websockets
import json
import time

async def test_single():
    """测试单一 WebSocket 连接"""
    # 币安 WebSocket 格式：/ws/<stream>
    url = 'wss://fstream.binance.com/ws/ethusdt@aggTrade'
    print(f'连接: {url}')
    
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            print('✅ 连接成功')
            print('等待第一条数据...')
            
            msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
            data = json.loads(msg)
            print(f'✅ 收到数据: {json.dumps(data, indent=2)}')
            
            # 继续接收几条
            for i in range(5):
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                price = data.get('p', 'N/A')
                qty = data.get('q', 'N/A')
                print(f'📈 价格: {price}, 数量: {qty}')
                
    except asyncio.TimeoutError:
        print('❌ 超时：10秒内未收到数据')
    except Exception as e:
        print(f'❌ 错误: {e}')

async def test_combined():
    """测试组合流"""
    # 组合流格式：/stream?streams=<stream1>/<stream2>
    url = 'wss://fstream.binance.com/stream?streams=ethusdt@aggTrade/btcusdt@aggTrade'
    print(f'\n测试组合流: {url}')
    
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            print('✅ 组合流连接成功')
            
            for i in range(10):
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                stream = data.get('stream', '')
                payload = data.get('data', {})
                price = payload.get('p', 'N/A')
                print(f'📊 {stream}: {price}')
                
    except asyncio.TimeoutError:
        print('❌ 组合流超时')
    except Exception as e:
        print(f'❌ 组合流错误: {e}')

async def test_mark_price():
    """测试标记价格（更新频率更高）"""
    url = 'wss://fstream.binance.com/ws/ethusdt@markPrice'
    print(f'\n测试标记价格: {url}')
    
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            print('✅ 标记价格连接成功')
            
            for i in range(5):
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                mark_price = data.get('p', 'N/A')
                funding_rate = data.get('r', 'N/A')
                print(f'📊 标记价格: {mark_price}, 资金费率: {funding_rate}')
                
    except asyncio.TimeoutError:
        print('❌ 标记价格超时')
    except Exception as e:
        print(f'❌ 标记价格错误: {e}')

if __name__ == '__main__':
    print('=== WebSocket 测试开始 ===\n')
    
    # 测试单一连接
    asyncio.run(test_single())
    
    # 测试组合流
    # asyncio.run(test_combined())
    
    # 测试标记价格
    # asyncio.run(test_mark_price())
