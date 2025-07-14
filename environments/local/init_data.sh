#!/bin/bash

# 本地环境数据初始化脚本

set -e

echo "🗄️ 初始化本地标签系统数据"
echo "================================="

# 等待MySQL服务就绪
wait_for_mysql() {
    echo "⏳ 等待MySQL服务启动..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec tag_system_mysql mysql -u root -proot123 -e "SELECT 1;" > /dev/null 2>&1; then
            echo "✅ MySQL服务已就绪"
            return 0
        fi
        
        echo "   尝试 $attempt/$max_attempts ..."
        sleep 2
        ((attempt++))
    done
    
    echo "❌ MySQL服务启动超时"
    exit 1
}

# 初始化数据库表结构
init_database() {
    echo "📋 初始化MySQL数据库表结构..."
    
    if docker exec -i tag_system_mysql mysql -u root -proot123 < init_database.sql; then
        echo "✅ 数据库表结构初始化完成"
        
        # 显示初始化结果
        echo "📊 数据库初始化统计:"
        docker exec tag_system_mysql mysql -u root -proot123 -e "
            USE tag_system;
            SELECT '标签分类数量:' as info, COUNT(*) as count FROM tag_category WHERE is_active = 1;
            SELECT '标签定义数量:' as info, COUNT(*) as count FROM tag_definition WHERE is_active = 1;  
            SELECT '标签规则数量:' as info, COUNT(*) as count FROM tag_rules WHERE is_active = 1;
        " 2>/dev/null
        
        return 0
    else
        echo "❌ 数据库表结构初始化失败"
        return 1
    fi
}

# 生成测试数据
generate_test_data() {
    echo "🧪 生成测试数据到S3存储..."
    
    # 返回项目根目录
    cd ../..
    
    # 生成测试数据
    if python -c "
import sys
sys.path.append('.')
from environments.local.test_data_generator import generate_test_data
generate_test_data()
" 2>/dev/null; then
        echo "✅ 测试数据生成完成"
        return 0
    else
        echo "❌ 测试数据生成失败，尝试使用内置数据生成器..."
        # 如果失败，尝试运行健康检查来触发内置数据生成
        if python main.py --env local --mode health --log-level ERROR > /dev/null 2>&1; then
            echo "✅ 内置测试数据生成完成"
            return 0
        else
            echo "⚠️ 测试数据生成失败，但可以继续运行"
            return 0
        fi
    fi
}

# 清理现有数据
clean_data() {
    echo "🧹 清理现有数据..."
    
    # 清理数据库
    echo "  - 清理MySQL数据..."
    docker exec tag_system_mysql mysql -u root -proot123 -e "
        DROP DATABASE IF EXISTS tag_system;
        CREATE DATABASE IF NOT EXISTS tag_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    " 2>/dev/null || echo "⚠️ 数据库清理失败"
    
    # 清理MinIO存储桶（如果存在）
    echo "  - 清理S3存储..."
    cd ../..
    python -c "
import boto3
from botocore.exceptions import ClientError
try:
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    try:
        # 删除存储桶中的所有对象
        response = s3_client.list_objects_v2(Bucket='test-data-lake')
        if 'Contents' in response:
            objects = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket='test-data-lake', Delete={'Objects': objects})
        
        # 删除存储桶
        s3_client.delete_bucket(Bucket='test-data-lake')
        print('  ✅ S3存储清理完成')
    except ClientError:
        print('  ℹ️ S3存储桶不存在，跳过清理')
except Exception as e:
    print(f'  ⚠️ S3清理失败: {e}')
" 2>/dev/null || echo "  ⚠️ S3清理跳过"
    
    cd environments/local
    echo "✅ 数据清理完成"
}

# 检查服务状态
check_services() {
    echo "🔍 检查服务状态..."
    
    # 检查Docker容器
    if ! docker-compose ps | grep -q "Up"; then
        echo "❌ Docker服务未启动，请先运行: ./setup.sh start"
        exit 1
    fi
    
    echo "✅ Docker服务运行正常"
}

# 显示完成信息
show_completion_info() {
    echo ""
    echo "🎉 数据初始化完成！"
    echo "================================="
    echo ""
    echo "📊 初始化内容:"
    echo "  ✅ MySQL数据库表结构和基础数据"
    echo "  ✅ S3存储桶和测试数据文件"
    echo "  ✅ 1000个模拟用户数据"
    echo ""
    echo "🎯 现在可以运行:"
    echo "  cd ../../"
    echo "  python main.py --env local --mode health    # 健康检查"
    echo "  python main.py --env local --mode full      # 全量计算"
    echo ""
}

# 主流程
main() {
    case "${1:-init}" in
        "init")
            check_services
            wait_for_mysql
            init_database
            generate_test_data
            show_completion_info
            ;;
        "clean")
            check_services
            wait_for_mysql
            clean_data
            echo "✅ 数据清理完成，运行 ./init_data.sh 重新初始化"
            ;;
        "reset")
            check_services
            wait_for_mysql
            clean_data
            init_database
            generate_test_data
            show_completion_info
            ;;
        "db-only")
            check_services
            wait_for_mysql
            init_database
            echo "✅ 数据库初始化完成"
            ;;
        "data-only")
            generate_test_data
            echo "✅ 测试数据生成完成"
            ;;
        *)
            echo "用法: $0 {init|clean|reset|db-only|data-only}"
            echo ""
            echo "  init     - 初始化数据库和测试数据（默认）"
            echo "  clean    - 清理所有数据"
            echo "  reset    - 清理并重新初始化所有数据"
            echo "  db-only  - 仅初始化数据库表结构"
            echo "  data-only- 仅生成测试数据"
            exit 1
            ;;
    esac
}

main "$@"