#!/usr/bin/env python3
"""
S3数据快速检查脚本
快速查看S3中各表的基本信息和数据样本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """创建Spark会话"""
    # 获取JAR文件路径
    jars_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "environments/local/jars")
    jar_files = [
        os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
        os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1034.jar")
    ]
    existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
    
    spark = SparkSession.builder \
        .appName("QuickS3Check") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", ",".join(existing_jars) if existing_jars else "") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def quick_check_table(spark, table_name: str, s3_path: str):
    """快速检查表"""
    logger.info(f"\n📋 {table_name}")
    logger.info("-" * 50)
    
    try:
        df = spark.read.parquet(s3_path)
        count = df.count()
        cols = len(df.columns)
        
        logger.info(f"📊 记录数: {count:,}")
        logger.info(f"📊 字段数: {cols}")
        logger.info(f"📋 字段名: {', '.join(df.columns)}")
        
        # 显示前3行数据
        logger.info("📝 数据样本:")
        df.show(3, truncate=False)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 检查失败: {str(e)}")
        return False


def main():
    """主函数"""
    logger.info("🚀 S3数据快速检查")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # 定义要检查的表
        tables = [
            ("用户基础信息", "s3a://test-data-lake/warehouse/user_basic_info"),
            ("用户资产汇总", "s3a://test-data-lake/warehouse/user_asset_summary"),
            ("用户活动汇总", "s3a://test-data-lake/warehouse/user_activity_summary"),
            ("用户交易明细", "s3a://test-data-lake/warehouse/user_transaction_detail")
        ]
        
        success_count = 0
        for table_name, s3_path in tables:
            if quick_check_table(spark, table_name, s3_path):
                success_count += 1
        
        logger.info(f"\n🎉 检查完成: {success_count}/{len(tables)} 个表成功")
        
    except Exception as e:
        logger.error(f"❌ 检查过程出错: {str(e)}")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()