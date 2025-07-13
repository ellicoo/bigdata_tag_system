#!/bin/bash

# 本地环境一键设置脚本

set -e

echo "🚀 设置本地标签系统环境"
echo "================================="

# 检查Docker
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker未安装，请先安装Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    
    echo "✅ Docker环境检查通过"
}

# 启动环境
start_environment() {
    echo "🐳 启动Docker服务..."
    
    # 进入本地环境目录
    cd "$(dirname "$0")"
    
    # 停止现有服务
    docker-compose down -v 2>/dev/null || true
    
    # 启动所有服务
    docker-compose up -d
    
    echo "⏳ 等待服务启动..."
    sleep 30
    
    # 检查服务状态
    echo "📋 检查服务状态..."
    docker-compose ps
}

# 生成测试数据
setup_test_data() {
    echo "🗄️ 设置测试数据..."
    
    # 返回项目根目录
    cd ../..
    
    # 安装依赖
    pip install -r requirements.txt
    
    # 生成测试数据
    python -c "
import sys
sys.path.append('.')
from environments.local.test_data_generator import generate_test_data
generate_test_data()
"
    
    echo "✅ 测试数据设置完成"
}

# 显示访问信息
show_info() {
    echo ""
    echo "🔗 服务访问信息:"
    echo "================================="
    echo "📊 Spark Web UI:     http://localhost:8080"
    echo "🗄️ MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
    echo "💾 MySQL:             localhost:3307 (root/root123)"
    echo "📓 Jupyter Notebook: http://localhost:8888 (token: tag_system_2024)"
    echo ""
    echo "🎯 快速测试:"
    echo "  健康检查: python main.py --env local --mode health"
    echo "  全量计算: python main.py --env local --mode full"
    echo ""
}

# 主流程
main() {
    case "${1:-setup}" in
        "setup")
            check_docker
            start_environment
            setup_test_data
            show_info
            ;;
        "start")
            start_environment
            show_info
            ;;
        "stop")
            cd "$(dirname "$0")"
            docker-compose down
            ;;
        "clean")
            cd "$(dirname "$0")"
            docker-compose down -v
            docker system prune -f
            ;;
        "info")
            show_info
            ;;
        *)
            echo "用法: $0 {setup|start|stop|clean|info}"
            exit 1
            ;;
    esac
}

main "$@"