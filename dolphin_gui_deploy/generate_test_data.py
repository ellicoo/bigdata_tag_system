#!/usr/bin/env python3
"""
测试数据生成器 - 支持 json_demo.txt 中的所有5个表及完整字段
基于现有 HiveToKafka.py 模式，生成多样化数据以匹配不同标签条件
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
    """生成完整的5个表的测试数据，匹配 json_demo.txt 中的所有字段"""
    
    print(f"🚀 生成完整测试数据，日期: {dt}")
    
    # 1. 生成用户基本信息测试数据（完整字段）
    user_basic_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        age = random.randint(18, 80)
        user_level = random.choice(['VIP1', 'VIP2', 'VIP3', 'VIP4', 'VIP5', 'NORMAL'])
        registration_date = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
        birthday = (datetime(1943 + age, random.randint(1, 12), random.randint(1, 28))).strftime('%Y-%m-%d')
        
        first_name = random.choice(["John", "Jane", "Mike", "Lisa", "Tom", "Alice", "Bob", "Carol", None])
        last_name = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", None])
        middle_name = random.choice(["A", "B", "C", None, None, None])  # 多数为空
        
        phone_prefix = random.choice(["+86", "+1", "+44"])
        phone_number = f"{phone_prefix}{random.choice(['138', '139', '186'])}{random.randint(10000000, 99999999)}"
        
        email_domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "temp.com"])
        email = f"{user_id}@{email_domain}"
        
        is_vip = random.choice([True, False])
        is_banned = random.choice([True, False, False, False])  # 大部分不被封禁
        
        kyc_status = random.choice(["verified", "pending", "rejected"])
        account_status = random.choice(["active", "suspended", "banned", "normal"])
        primary_status = random.choice(["gold", "silver", "bronze", None])
        secondary_status = random.choice(["premium", "standard", None, None])  # 多数为空
        
        user_basic_data.append((user_id, age, user_level, registration_date, birthday,
                               first_name, last_name, middle_name, phone_number, email,
                               is_vip, is_banned, kyc_status, account_status, primary_status, secondary_status))
    
    user_basic_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True), 
        StructField("user_level", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("birthday", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("is_vip", BooleanType(), True),
        StructField("is_banned", BooleanType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("account_status", StringType(), True),
        StructField("primary_status", StringType(), True),
        StructField("secondary_status", StringType(), True)
    ])
    
    user_basic_df = spark.createDataFrame(user_basic_data, user_basic_schema)
    user_basic_df = user_basic_df.withColumn("dt", lit(dt))
    
    user_basic_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_system.user_basic_info")
    
    print("✅ 用户基本信息测试数据生成完成")
    
    # 2. 生成用户资产数据（增加 debt_amount 字段）
    user_asset_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        total_asset = random.choice([0, 100000, 50000, 200000, 1000, 500000])  # 包含精确匹配值
        cash_balance = random.choice([0, 50000, 25000, 75000, 100000])  # 包含精确匹配值
        debt_amount = random.choice([None, 0, 10000, 5000]) if random.random() > 0.3 else None
        
        user_asset_data.append((user_id, float(total_asset), float(cash_balance), 
                              float(debt_amount) if debt_amount is not None else None))
    
    user_asset_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("total_asset_value", DoubleType(), True),
        StructField("cash_balance", DoubleType(), True),
        StructField("debt_amount", DoubleType(), True)
    ])
    
    user_asset_df = spark.createDataFrame(user_asset_data, user_asset_schema)
    user_asset_df = user_asset_df.withColumn("dt", lit(dt))
    
    user_asset_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_system.user_asset_summary")
    
    print("✅ 用户资产测试数据生成完成")
    
    # 3. 生成用户活动数据（增加 last_trade_date 字段）
    user_activity_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        trade_count = random.choice([0, 5, 10, 15, 1, 2])  # 包含精确匹配值
        
        # 确保有用户匹配特定日期条件
        if i < 50:  # 前50个用户有特定日期
            last_login = datetime(2025, 1, 1)
        elif i < 100:
            last_login = datetime(2025, 7, 15)
        else:
            last_login = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))
        
        # last_trade_date 部分为空
        last_trade = None if random.random() > 0.7 else last_login - timedelta(days=random.randint(0, 30))
        
        user_activity_data.append((user_id, trade_count, last_login.strftime('%Y-%m-%d'), 
                                  last_trade.strftime('%Y-%m-%d') if last_trade else None))
    
    user_activity_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("trade_count_30d", IntegerType(), True),
        StructField("last_login_date", StringType(), True),
        StructField("last_trade_date", StringType(), True)
    ])
    
    user_activity_df = spark.createDataFrame(user_activity_data, user_activity_schema)
    user_activity_df = user_activity_df.withColumn("dt", lit(dt))
    
    user_activity_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_system.user_activity_summary")
    
    print("✅ 用户活动测试数据生成完成")
    
    # 4. 生成用户风险档案数据（新增表）
    user_risk_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        risk_score = random.choice([10, 20, 30, 40, 50])  # 包含 <= 30 的值
        user_risk_data.append((user_id, risk_score))
    
    user_risk_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("risk_score", IntegerType(), True)
    ])
    
    user_risk_df = spark.createDataFrame(user_risk_data, user_risk_schema)
    user_risk_df = user_risk_df.withColumn("dt", lit(dt))
    
    user_risk_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_system.user_risk_profile")
    
    print("✅ 用户风险档案测试数据生成完成")
    
    # 5. 生成用户偏好数据（新增表）
    user_preferences_data = []
    product_options = ["stocks", "bonds", "forex", "savings", "checking", "premium", "gold", "platinum", "high_risk", "speculative"]
    service_options = ["advisory", "trading", "research", "premium_support"]
    
    for i in range(1000):
        user_id = f"user_{i:06d}"
        
        # 确保不同的列表组合用于测试不同操作符
        interested = random.sample(product_options[:4], random.randint(1, 3))
        owned = random.sample(["savings", "checking", "premium"], random.randint(0, 2))
        blacklisted = ["forex"] if random.random() > 0.8 else []
        active = random.sample(["premium", "gold", "silver"], random.randint(0, 2))
        expired = random.sample(["premium", "platinum"], random.randint(0, 1))
        optional = [] if random.random() > 0.6 else random.sample(service_options, 1)
        required = random.sample(service_options, random.randint(1, 2))
        
        user_preferences_data.append((user_id, interested, owned, blacklisted, active, expired, optional, required))
    
    user_preferences_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("interested_products", ArrayType(StringType()), True),
        StructField("owned_products", ArrayType(StringType()), True),
        StructField("blacklisted_products", ArrayType(StringType()), True),
        StructField("active_products", ArrayType(StringType()), True),
        StructField("expired_products", ArrayType(StringType()), True),
        StructField("optional_services", ArrayType(StringType()), True),
        StructField("required_services", ArrayType(StringType()), True)
    ])
    
    user_preferences_df = spark.createDataFrame(user_preferences_data, user_preferences_schema)
    user_preferences_df = user_preferences_df.withColumn("dt", lit(dt))
    
    user_preferences_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("tag_system.user_preferences")
    
    print("✅ 用户偏好测试数据生成完成")
    
    # 验证数据
    print("\n📊 数据验证:")
    tables = ["user_basic_info", "user_asset_summary", "user_activity_summary", "user_risk_profile", "user_preferences"]
    for table in tables:
        count = spark.table(f'tag_system.{table}').count()
        print(f"   📊 tag_system.{table}: {count} 条记录")
    
    print("🎯 完整的5个表测试数据生成完成，已确保多样性匹配所有标签条件")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # 创建数据库
        spark.sql("CREATE DATABASE IF NOT EXISTS tag_system")
        print("✅ 数据库 tag_system 创建成功")
        
        # 生成测试数据
        generate_test_data(spark)
        
        print("🎉 测试数据生成完成！")
        
    finally:
        spark.stop()
