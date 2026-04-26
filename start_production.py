#!/usr/bin/env python3
"""
生产环境启动脚本 - 使用 Waitress WSGI 服务器
比 Flask 开发服务器更稳定，支持并发连接
"""
import sys
from waitress import serve
from app import app

if __name__ == "__main__":
    print("=" * 60)
    print("TV量化机器人 - 生产环境启动")
    print("=" * 60)
    print("服务器: Waitress (生产级WSGI)")
    print("端口: 80")
    print("线程: 4")
    print("=" * 60)
    
    # 使用 waitress 启动
    # threads=4: 处理并发请求的线程数
    # channel_timeout=30: 连接超时时间
    serve(
        app,
        host='0.0.0.0',
        port=80,
        threads=4,
        channel_timeout=30,
        cleanup_interval=10,
        max_request_body_size=1073741824,  # 1GB
        expose_tracebacks=False  # 生产环境不暴露错误堆栈
    )
