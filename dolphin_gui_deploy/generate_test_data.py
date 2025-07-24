#!/usr/bin/env python3
"""
建表参考而已，测试数据会通过generate_test_data.py脚本产生
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

def create_spark_session():
    """创建Spark会话 - 基于现有HiveToKafka.py模式"""
    spark = SparkSession.builder \
        .appName("TagSystemTestDataGenerator") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def generate_test_data(spark, dt='2025-01-20'):
    """生成测试数据并写入Hive表"""
    
    print(f"🚀 生成测试数据，日期: {dt}")
    
    # 生成用户基本信息测试数据
    user_basic_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        age = random.randint(18, 65)
        user_level = random.choice(['VIP1', 'VIP2', 'VIP3', 'NORMAL'])
        kyc_status = random.choice(['verified', 'pending', 'rejected'])
        registration_date = (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
        risk_score = random.uniform(0, 100)
        
        user_basic_data.append((user_id, age, user_level, kyc_status, registration_date, risk_score))
    
    # 创建DataFrame并写入Hive
    user_basic_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True), 
        StructField("user_level", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("risk_score", DoubleType(), True)
    ])
    
    user_basic_df = spark.createDataFrame(user_basic_data, user_basic_schema)
    user_basic_df = user_basic_df.withColumn("dt", lit(dt))
    
    # 写入Hive表
    user_basic_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_test.user_basic_info")
    
    print("✅ 用户基本信息测试数据生成完成")
    
    # 生成用户资产数据
    user_asset_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        total_asset = random.uniform(1000, 1000000)
        cash_balance = random.uniform(100, total_asset * 0.5)
        
        user_asset_data.append((user_id, total_asset, cash_balance))
    
    user_asset_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("total_asset_value", DoubleType(), True),
        StructField("cash_balance", DoubleType(), True)
    ])
    
    user_asset_df = spark.createDataFrame(user_asset_data, user_asset_schema)
    user_asset_df = user_asset_df.withColumn("dt", lit(dt))
    
    user_asset_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_test.user_asset_summary")
    
    print("✅ 用户资产测试数据生成完成")
    
    # 生成用户活动数据
    user_activity_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        trade_count = random.randint(0, 100)
        last_login = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
        
        user_activity_data.append((user_id, trade_count, last_login))
    
    user_activity_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("trade_count_30d", IntegerType(), True),
        StructField("last_login_date", StringType(), True)
    ])
    
    user_activity_df = spark.createDataFrame(user_activity_data, user_activity_schema)
    user_activity_df = user_activity_df.withColumn("dt", lit(dt))
    
    user_activity_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_test.user_activity_summary")
    
    print("✅ 用户活动测试数据生成完成")
    
    # 验证数据
    print("\n📊 数据验证:")
    print(f"用户基本信息表记录数: {spark.table('tag_test.user_basic_info').count()}")
    print(f"用户资产表记录数: {spark.table('tag_test.user_asset_summary').count()}")
    print(f"用户活动表记录数: {spark.table('tag_test.user_activity_summary').count()}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # 创建数据库
        spark.sql("CREATE DATABASE IF NOT EXISTS tag_test")
        print("✅ 数据库 tag_test 创建成功")
        
        # 生成测试数据
        generate_test_data(spark)
        
        print("🎉 测试数据生成完成！")
        
    finally:
        spark.stop()
