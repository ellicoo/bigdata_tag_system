#!/bin/bash

# Hive表数据初始化脚本

set -e

echo "🗄️ 初始化Hive表数据"
echo "================================="

# 检查MinIO服务状态
wait_for_minio() {
    echo "⏳ 等待MinIO服务启动..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            echo "✅ MinIO服务已就绪"
            return 0
        fi
        
        echo "   尝试 $attempt/$max_attempts ..."
        sleep 2
        ((attempt++))
    done
    
    echo "❌ MinIO服务启动超时"
    exit 1
}

# 检查Spark依赖
check_spark_dependencies() {
    echo "🔍 检查Spark依赖..."
    
    local jars_dir="./jars"
    local required_jars=(
        "hadoop-aws-3.3.4.jar"
        "aws-java-sdk-bundle-1.12.262.jar"
        "mysql-connector-j-8.0.33.jar"
    )
    
    for jar in "${required_jars[@]}"; do
        if [ ! -f "$jars_dir/$jar" ]; then
            echo "❌ 缺少必需的JAR文件: $jar"
            echo "请确保 $jars_dir 目录包含所有必需的JAR文件"
            exit 1
        fi
    done
    
    echo "✅ Spark依赖检查通过"
}

# 初始化Hive表
init_hive_tables() {
    echo "📊 开始初始化Hive表结构和数据..."
    
    # 返回项目根目录
    cd ../..
    
    # 设置Python路径
    export PYTHONPATH="$PWD:$PYTHONPATH"
    
    # 运行Hive表初始化
    if python environments/local/init_hive_tables.py; then
        echo "✅ Hive表初始化完成"
        return 0
    else
        echo "❌ Hive表初始化失败"
        return 1
    fi
}

# 验证Hive表数据
verify_hive_tables() {
    echo "🔍 验证Hive表数据..."
    
    # 返回项目根目录（如果还没有的话）
    cd ../.. 2>/dev/null || true
    
    echo "📋 运行健康检查验证数据读取..."
    if python main.py --env local --mode health --log-level INFO; then
        echo "✅ Hive表数据验证通过"
        return 0
    else
        echo "❌ Hive表数据验证失败"
        return 1
    fi
}

# 显示表统计信息
show_table_stats() {
    echo ""
    echo "📊 Hive表统计信息:"
    echo "================================="
    
    # 使用Python脚本快速统计
    cd ../.. 2>/dev/null || true
    
    python -c "
import sys
sys.path.append('.')
try:
    from src.common.config.manager import ConfigManager
    from pyspark.sql import SparkSession
    import os
    
    # 加载配置
    config = ConfigManager.load_config('local')
    
    # 简化的Spark配置用于快速查询
    spark = SparkSession.builder \
        .appName('TableStats') \
        .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000') \
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()
    
    bucket_name = 'test-data-lake'
    tables = ['user_basic_info', 'user_asset_summary', 'user_activity_summary']
    
    for table in tables:
        try:
            df = spark.read.parquet(f's3a://{bucket_name}/warehouse/{table}')
            count = df.count()
            print(f'  ✅ {table}: {count:,} 条记录')
        except Exception as e:
            print(f'  ❌ {table}: 读取失败 - {str(e)[:50]}...')
    
    spark.stop()
    
except Exception as e:
    print(f'  ⚠️ 统计信息获取失败: {e}')
" 2>/dev/null || echo "  ⚠️ 无法获取统计信息，但表已创建"
}

# 清理Hive数据
clean_hive_data() {
    echo "🧹 清理Hive数据..."
    
    # 清理MinIO中的数据
    cd ../.. 2>/dev/null || true
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
    
    bucket_name = 'test-data-lake'
    
    # 删除warehouse目录下的所有对象
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='warehouse/')
        if 'Contents' in response:
            objects = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
            print('✅ Hive数据清理完成')
        else:
            print('ℹ️ 没有需要清理的Hive数据')
    except ClientError:
        print('ℹ️ 存储桶不存在，跳过清理')
        
except Exception as e:
    print(f'⚠️ Hive数据清理失败: {e}')
" 2>/dev/null || echo "⚠️ Hive数据清理跳过"
    
    cd environments/local
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
    echo "🎉 Hive表初始化完成！"
    echo "================================="
    echo ""
    echo "📊 已创建的Hive表:"
    echo "  ✅ user_basic_info - 用户基础信息表"
    echo "  ✅ user_asset_summary - 用户资产汇总表"  
    echo "  ✅ user_activity_summary - 用户活动汇总表"
    echo "  ✅ user_transaction_detail - 用户交易明细表"
    echo ""
    echo "🎯 支持的标签任务数据源:"
    echo "  - 高净值用户: user_asset_summary"
    echo "  - VIP客户: user_basic_info"
    echo "  - 年轻用户: user_basic_info" 
    echo "  - 活跃交易者: user_activity_summary"
    echo "  - 低风险用户: user_activity_summary"
    echo "  - 新用户: user_basic_info"
    echo "  - 现金富裕用户: user_asset_summary"
    echo ""
    echo "🚀 现在可以运行真实的S3/Hive数据测试:"
    echo "  cd ../../"
    echo "  python main.py --env local --mode health           # 健康检查"
    echo "  python main.py --env local --mode full-parallel   # 全量并行计算"
    echo "  python main.py --env local --mode tags-parallel --tag-ids 1,2,3  # 指定标签测试"
    echo ""
}

# 主流程
main() {
    case "${1:-init}" in
        "init")
            check_services
            wait_for_minio
            check_spark_dependencies
            init_hive_tables
            verify_hive_tables
            show_table_stats
            show_completion_info
            ;;
        "clean")
            check_services
            clean_hive_data
            echo "✅ Hive数据清理完成，运行 ./init_hive_data.sh 重新初始化"
            ;;
        "reset")
            check_services
            clean_hive_data
            wait_for_minio
            check_spark_dependencies
            init_hive_tables
            verify_hive_tables
            show_table_stats
            show_completion_info
            ;;
        "verify")
            check_services
            verify_hive_tables
            show_table_stats
            ;;
        "stats")
            check_services
            show_table_stats
            ;;
        *)
            echo "用法: $0 {init|clean|reset|verify|stats}"
            echo ""
            echo "  init     - 初始化Hive表和数据（默认）"
            echo "  clean    - 清理Hive数据"
            echo "  reset    - 清理并重新初始化Hive数据"
            echo "  verify   - 验证Hive表数据"
            echo "  stats    - 显示表统计信息"
            exit 1
            ;;
    esac
}

main "$@"