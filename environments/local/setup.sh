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

# 下载JDBC驱动
download_jdbc_driver() {
    echo "📥 下载MySQL JDBC驱动..."
    
    # 创建jars目录
    mkdir -p jars
    
    # 下载MySQL JDBC驱动
    if [ ! -f "jars/mysql-connector-j-8.0.33.jar" ]; then
        echo "正在下载MySQL JDBC驱动..."
        curl -L -o jars/mysql-connector-j-8.0.33.jar \
            https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
        echo "✅ MySQL JDBC驱动下载完成"
    else
        echo "✅ MySQL JDBC驱动已存在"
    fi
}

# 下载依赖和JDBC驱动
setup_dependencies() {
    echo "📦 设置依赖..."
    
    # 返回项目根目录
    cd ../..
    
    # 安装依赖
    echo "📦 安装Python依赖..."
    pip install -r requirements.txt
    
    # 返回本地环境目录
    cd environments/local
    
    # 下载JDBC驱动
    download_jdbc_driver
    
    echo "✅ 依赖设置完成"
}

# 显示访问信息
show_info() {
    echo ""
    echo "🎉 本地标签系统部署完成！"
    echo "================================="
    echo ""
    echo "🔗 服务访问信息:"
    echo "--------------------------------"
    echo "📊 Spark Master Web UI:"
    echo "   http://localhost:8080"
    echo "   (监控Spark集群状态和作业执行)"
    echo ""
    echo "🗄️ MinIO Console (S3存储):"
    echo "   http://localhost:9001"
    echo "   用户名: minioadmin"
    echo "   密码: minioadmin"
    echo "   (管理存储桶和文件)"
    echo ""
    echo "💾 MySQL 数据库:"
    echo "   主机: localhost:3307"
    echo "   用户名: root"
    echo "   密码: root123"
    echo "   数据库: tag_system"
    echo ""
    echo "📓 Jupyter Notebook:"
    echo "   http://localhost:8888"
    echo "   Token: tag_system_2024"
    echo "   (交互式数据分析)"
    echo ""
    echo "🎯 下一步操作:"
    echo "--------------------------------"
    echo "./init_data.sh                              # 初始化数据库和测试数据"
    echo "cd ../../  # 回到项目根目录"
    echo "python main.py --env local --mode health    # 健康检查"
    echo "python main.py --env local --mode full      # 全量计算"
    echo ""
    echo "📋 详细使用说明:"
    echo "查看 environments/local/README.md"
    echo ""
}

# 主流程
main() {
    case "${1:-setup}" in
        "setup")
            check_docker
            start_environment
            setup_dependencies
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