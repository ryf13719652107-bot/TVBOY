#!/bin/bash
# 生产环境启动脚本 (Linux/Mac)

echo "============================================"
echo "TV量化机器人 - 生产环境启动"
echo "============================================"

# 检查 waitress 是否安装
if ! python -c "import waitress" 2>/dev/null; then
    echo "正在安装 waitress..."
    pip install waitress
fi

# 启动服务
echo "启动 Waitress 服务器..."
python start_production.py
