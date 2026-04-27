# -*- coding: utf-8 -*-
"""
测试组合流 WebSocket
"""
import asyncio
import websockets
import json

async def test_combined_stream():
    """测试组合流格式"""
    # 组合流格式：/stream?streams=stream1/stream2
    url = 'wss://fstream.binance.com/stream?streams=ethusdt@aggTrade/bsbusdt@aggTrade'
    print(f'连接组合流: {url}')
    
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            print('✅ 组合流连接成功')
            print('等待数据...')
            
            for i in range(10):
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                
                # 组合流格式：{ "stream": "ethusdt@aggTrade", "data": {...} }
                stream = data.get('stream', '')
                payload = data.get('data', {})
                price = payload.get('p', 'N/A')
                
                print(f'📊 [{i+1}] {stream}: {price}')
                
    except asyncio.TimeoutError:
        print('❌ 组合流超时')
    except Exception as e:
        print(f'❌ 组合流错误: {e}')

if __name__ == '__main__':
    asyncio.run(test_combined_stream())
