#!/bin/bash

# 测试环境设置脚本

set -e

echo "🚀 设置大数据标签系统测试环境"
echo "================================="

# 检查Docker和Docker Compose
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

# 下载必要的JAR包
download_jars() {
    echo "📦 下载必要的JAR包..."
    
    JARS_DIR="./test_env/jars"
    mkdir -p $JARS_DIR
    
    # MySQL Connector JAR
    if [ ! -f "$JARS_DIR/mysql-connector-java-8.0.33.jar" ]; then
        echo "下载 MySQL Connector..."
        curl -L "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar" \
             -o "$JARS_DIR/mysql-connector-java-8.0.33.jar"
    fi
    
    # AWS SDK for S3
    if [ ! -f "$JARS_DIR/hadoop-aws-3.3.4.jar" ]; then
        echo "下载 Hadoop AWS..."
        curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
             -o "$JARS_DIR/hadoop-aws-3.3.4.jar"
    fi
    
    if [ ! -f "$JARS_DIR/aws-java-sdk-bundle-1.12.367.jar" ]; then
        echo "下载 AWS Java SDK..."
        curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" \
             -o "$JARS_DIR/aws-java-sdk-bundle-1.12.367.jar"
    fi
    
    echo "✅ JAR包下载完成"
}

# 启动Docker服务
start_services() {
    echo "🐳 启动Docker服务..."
    
    # 停止现有服务（如果存在）
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
generate_test_data() {
    echo "🗄️ 生成测试数据..."
    
    # 安装Python依赖
    pip install boto3 pandas pyarrow pymysql
    
    # 等待MinIO服务完全启动
    echo "⏳ 等待MinIO服务启动..."
    sleep 10
    
    # 运行数据生成脚本
    python test_env/generate_test_data.py
    
    echo "✅ 测试数据生成完成"
}

# 运行测试
run_tests() {
    echo "🧪 运行标签计算测试..."
    
    # 等待所有服务就绪
    echo "⏳ 等待所有服务就绪..."
    sleep 20
    
    # 运行测试脚本
    python test_env/run_test.py
    
    echo "✅ 测试完成"
}

# 显示服务信息
show_services_info() {
    echo ""
    echo "🔗 服务访问信息:"
    echo "================================="
    echo "📊 Spark Web UI:     http://localhost:8080"
    echo "🗄️ MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
    echo "💾 MySQL:             localhost:3307 (root/root123)"
    echo "📓 Jupyter Notebook: http://localhost:8888 (token: tag_system_2024)"
    echo ""
    echo "🎯 测试数据位置:"
    echo "  S3 Bucket: test-data-lake"
    echo "  数据库: tag_system"
    echo ""
    echo "🛠️ 管理命令:"
    echo "  停止服务: docker-compose down"
    echo "  查看日志: docker-compose logs [service_name]"
    echo "  重启服务: docker-compose restart [service_name]"
    echo ""
}

# 清理环境
cleanup() {
    echo "🧹 清理测试环境..."
    docker-compose down -v
    docker system prune -f
    echo "✅ 环境清理完成"
}

# 主函数
main() {
    case "${1:-setup}" in
        "setup")
            check_docker
            download_jars
            start_services
            generate_test_data
            show_services_info
            ;;
        "start")
            start_services
            show_services_info
            ;;
        "test")
            run_tests
            ;;
        "data")
            generate_test_data
            ;;
        "stop")
            docker-compose down
            ;;
        "cleanup")
            cleanup
            ;;
        "info")
            show_services_info
            ;;
        *)
            echo "用法: $0 {setup|start|test|data|stop|cleanup|info}"
            echo ""
            echo "命令说明:"
            echo "  setup   - 完整环境设置（默认）"
            echo "  start   - 启动Docker服务"
            echo "  test    - 运行标签计算测试"
            echo "  data    - 重新生成测试数据"
            echo "  stop    - 停止Docker服务"
            echo "  cleanup - 清理环境和数据"
            echo "  info    - 显示服务信息"
            exit 1
            ;;
    esac
}

main "$@"