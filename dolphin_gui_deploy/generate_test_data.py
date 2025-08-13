#!/usr/bin/env python3
"""
DWS层用户指标测试数据生成器 - 支持49个用户指标
基于 model/dws_user_index.sql 中的7个DWS表结构，生成符合所有指标类型的多样化测试数据
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

def create_spark_session():
    """创建Spark会话 - DWS数据生成优化配置"""
    spark = SparkSession.builder \
        .appName("DWSUserIndexTestDataGenerator") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    return spark

def generate_dws_test_data(spark, dt='2025-01-20', user_count=1000):
    """生成完整的DWS层用户指标测试数据，覆盖所有49个指标"""
    
    print(f"🚀 生成DWS层用户指标测试数据，日期: {dt}, 用户数: {user_count}")
    
    # 创建数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS dws_user")
    print("✅ 数据库 dws_user 创建成功")
    
    # 1. 生成用户基础画像表 dws_user_profile_df
    profile_data = []
    countries = ["CN", "US", "JP", "KR", "SG", "HK", "TW"]
    channels = ["organic", "advertisement", "referral", "social_media"]
    methods = ["email", "mobile", "google", "apple"]
    levels = ["VIP0", "VIP1", "VIP2", "VIP3", "VIP4", "VIP5"]
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 注册基础信息
        register_time = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime('%Y-%m-%d %H:%M:%S')
        register_source_channel = random.choice(channels)
        register_method = random.choice(methods)
        register_country = random.choice(countries)
        
        # 身份认证信息  
        is_kyc_completed = random.choice(["true", "false"])
        kyc_country = random.choice(countries)
        is_2fa_enabled = random.choice(["true", "false"])
        user_level = random.choice(levels)
        is_agent = random.choice(["true", "false"])
        
        profile_data.append((user_id, register_time, register_source_channel, register_method, register_country,
                           is_kyc_completed, kyc_country, is_2fa_enabled, user_level, is_agent))
    
    profile_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("register_time", StringType(), True),
        StructField("register_source_channel", StringType(), True),
        StructField("register_method", StringType(), True),
        StructField("register_country", StringType(), True),
        StructField("is_kyc_completed", StringType(), True),
        StructField("kyc_country", StringType(), True),
        StructField("is_2fa_enabled", StringType(), True),
        StructField("user_level", StringType(), True),
        StructField("is_agent", StringType(), True)
    ])
    
    profile_df = spark.createDataFrame(profile_data, profile_schema)
    profile_df = profile_df.withColumn("dt", lit(dt))
    
    profile_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_profile_df")
    
    print("✅ 用户基础画像数据生成完成")
    
    # 2. 生成用户资产财务表 dws_user_asset_df
    asset_data = []
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 充值提现相关 - 数值类型指标
        total_deposit_amount = str(random.choice([0, 50000, 100000, 200000, 500000, 1000000]))
        total_withdraw_amount = str(random.choice([0, 25000, 75000, 150000, 300000]))
        net_deposit_amount = str(float(total_deposit_amount) - float(total_withdraw_amount))
        withdraw_count_30d = str(random.randint(0, 20))
        deposit_fail_count = str(random.randint(0, 5))
        
        # 当前资产状况 - 按账户类型细分
        spot_position_value = str(random.choice([0, 5000, 25000, 50000, 100000]))
        contract_position_value = str(random.choice([0, 10000, 50000, 100000, 200000]))
        finance_position_value = str(random.choice([0, 20000, 50000, 100000, 300000]))
        onchain_position_value = str(random.choice([0, 5000, 10000, 25000, 50000]))
        current_total_position_value = str(float(spot_position_value) + float(contract_position_value) + float(finance_position_value) + float(onchain_position_value))
        
        # 可用余额 - 按账户类型细分
        spot_available_balance = str(random.choice([0, 2000, 10000, 20000, 50000]))
        contract_available_balance = str(random.choice([0, 3000, 15000, 30000, 60000]))
        onchain_available_balance = str(random.choice([0, 1000, 5000, 10000, 20000]))
        available_balance = str(float(spot_available_balance) + float(contract_available_balance) + float(onchain_available_balance))
        
        # 锁仓金额 - 按账户类型细分
        spot_locked_amount = str(random.choice([0, 500, 2000, 5000, 10000]))
        contract_locked_amount = str(random.choice([0, 1000, 5000, 10000, 20000]))
        finance_locked_amount = str(random.choice([0, 2000, 10000, 20000, 50000]))
        onchain_locked_amount = str(random.choice([0, 200, 1000, 2000, 5000]))
        locked_amount = str(float(spot_locked_amount) + float(contract_locked_amount) + float(finance_locked_amount) + float(onchain_locked_amount))
        
        # 时间相关 - 日期类型指标
        base_time = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400))
        last_deposit_time = base_time.strftime('%Y-%m-%d %H:%M:%S')
        last_withdraw_time = (base_time - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')
        
        asset_data.append((user_id, total_deposit_amount, total_withdraw_amount, net_deposit_amount,
                         last_deposit_time, last_withdraw_time, withdraw_count_30d, deposit_fail_count,
                         spot_position_value, contract_position_value, finance_position_value, onchain_position_value, current_total_position_value,
                         spot_available_balance, contract_available_balance, onchain_available_balance, available_balance,
                         spot_locked_amount, contract_locked_amount, finance_locked_amount, onchain_locked_amount, locked_amount))
    
    asset_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("total_deposit_amount", StringType(), True),
        StructField("total_withdraw_amount", StringType(), True),
        StructField("net_deposit_amount", StringType(), True),
        StructField("last_deposit_time", StringType(), True),
        StructField("last_withdraw_time", StringType(), True),
        StructField("withdraw_count_30d", StringType(), True),
        StructField("deposit_fail_count", StringType(), True),
        # 当前资产状况 - 按账户类型细分
        StructField("spot_position_value", StringType(), True),
        StructField("contract_position_value", StringType(), True),
        StructField("finance_position_value", StringType(), True),
        StructField("onchain_position_value", StringType(), True),
        StructField("current_total_position_value", StringType(), True),
        # 可用余额 - 按账户类型细分
        StructField("spot_available_balance", StringType(), True),
        StructField("contract_available_balance", StringType(), True),
        StructField("onchain_available_balance", StringType(), True),
        StructField("available_balance", StringType(), True),
        # 锁仓金额 - 按账户类型细分
        StructField("spot_locked_amount", StringType(), True),
        StructField("contract_locked_amount", StringType(), True),
        StructField("finance_locked_amount", StringType(), True),
        StructField("onchain_locked_amount", StringType(), True),
        StructField("locked_amount", StringType(), True)
    ])
    
    asset_df = spark.createDataFrame(asset_data, asset_schema)
    asset_df = asset_df.withColumn("dt", lit(dt))
    
    asset_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_asset_df")
    
    print("✅ 用户资产财务数据生成完成")
    
    # 3. 生成用户交易行为表 dws_user_trading_df
    trading_data = []
    trading_styles = ["高杠杆", "低频", "套保", "激进", "保守"]
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 交易金额统计 - 按业务类型细分
        spot_trading_volume = str(random.choice([0, 50000, 200000, 500000, 1000000]))
        contract_trading_volume = str(random.choice([0, 100000, 300000, 800000, 2000000]))
        total_trading_volume = str(float(spot_trading_volume) + float(contract_trading_volume))
        
        # 最近30日交易额 - 按业务类型细分
        spot_recent_30d_volume = str(random.choice([0, 20000, 80000, 200000, 400000]))
        contract_recent_30d_volume = str(random.choice([0, 30000, 120000, 300000, 600000]))
        recent_30d_trading_volume = str(float(spot_recent_30d_volume) + float(contract_recent_30d_volume))
        
        # 交易次数统计 - 按业务类型细分
        spot_trade_count = str(random.randint(0, 100))
        contract_trade_count = str(random.randint(0, 50))
        finance_trade_count = str(random.randint(0, 20))
        onchain_trade_count = str(random.randint(0, 10))
        
        # 时间相关 - 日期类型指标
        base_time = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400))
        first_trade_time = base_time.strftime('%Y-%m-%d %H:%M:%S')
        last_trade_time = (base_time + timedelta(days=random.randint(1, 100))).strftime('%Y-%m-%d %H:%M:%S')
        
        # 交易行为特征 - 布尔类型指标
        has_contract_trading = random.choice(["true", "false"])
        has_finance_management = random.choice(["true", "false"])
        has_pending_orders = random.choice(["true", "false"])
        
        # 枚举类型指标
        contract_trading_style = random.choice(trading_styles)
        
        trading_data.append((user_id, spot_trading_volume, contract_trading_volume, total_trading_volume,
                           spot_recent_30d_volume, contract_recent_30d_volume, recent_30d_trading_volume,
                           spot_trade_count, contract_trade_count, finance_trade_count, onchain_trade_count,
                           first_trade_time, last_trade_time, has_contract_trading, contract_trading_style, has_finance_management, has_pending_orders))
    
    trading_schema = StructType([
        StructField("user_id", StringType(), True),
        # 交易金额统计 - 按业务类型细分
        StructField("spot_trading_volume", StringType(), True),
        StructField("contract_trading_volume", StringType(), True),
        StructField("total_trading_volume", StringType(), True),
        # 最近30日交易额 - 按业务类型细分
        StructField("spot_recent_30d_volume", StringType(), True),
        StructField("contract_recent_30d_volume", StringType(), True),
        StructField("recent_30d_trading_volume", StringType(), True),
        # 交易次数统计 - 按业务类型细分
        StructField("spot_trade_count", StringType(), True),
        StructField("contract_trade_count", StringType(), True),
        StructField("finance_trade_count", StringType(), True),
        StructField("onchain_trade_count", StringType(), True),
        # 交易时间相关
        StructField("first_trade_time", StringType(), True),
        StructField("last_trade_time", StringType(), True),
        # 交易行为特征
        StructField("has_contract_trading", StringType(), True),
        StructField("contract_trading_style", StringType(), True),
        StructField("has_finance_management", StringType(), True),
        StructField("has_pending_orders", StringType(), True)
    ])
    
    trading_df = spark.createDataFrame(trading_data, trading_schema)
    trading_df = trading_df.withColumn("dt", lit(dt))
    
    trading_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_trading_df")
    
    print("✅ 用户交易行为数据生成完成")
    
    # 4. 生成用户活跃行为表 dws_user_activity_df
    activity_data = []
    os_list = ["iOS", "Android", "Windows", "macOS", "Linux"]
    email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "163.com", "qq.com"]
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 时间维度指标 - 数值类型
        days_since_register = str(random.randint(30, 1800))
        days_since_last_login = str(random.randint(0, 90))
        login_count_7d = str(random.randint(0, 50))
        
        # 时间相关 - 日期类型指标
        base_time = datetime.now() - timedelta(days=int(days_since_last_login))
        last_login_time = base_time.strftime('%Y-%m-%d %H:%M:%S')
        last_activity_time = (base_time + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S')
        
        # 设备和地理信息 - 字符串类型指标
        login_ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        country_region_code = f"+{random.choice([86, 1, 44, 81, 82, 65])}"
        email_suffix = random.choice(email_domains)
        operating_system = random.choice(os_list)
        
        activity_data.append((user_id, days_since_register, days_since_last_login, last_login_time, last_activity_time,
                            login_count_7d, login_ip_address, country_region_code, email_suffix, operating_system))
    
    activity_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("days_since_register", StringType(), True),
        StructField("days_since_last_login", StringType(), True),
        StructField("last_login_time", StringType(), True),
        StructField("last_activity_time", StringType(), True),
        StructField("login_count_7d", StringType(), True),
        StructField("login_ip_address", StringType(), True),
        StructField("country_region_code", StringType(), True),
        StructField("email_suffix", StringType(), True),
        StructField("operating_system", StringType(), True)
    ])
    
    activity_df = spark.createDataFrame(activity_data, activity_schema)
    activity_df = activity_df.withColumn("dt", lit(dt))
    
    activity_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_activity_df")
    
    print("✅ 用户活跃行为数据生成完成")
    
    # 5. 生成用户风险风控表 dws_user_risk_df
    risk_data = []
    channel_sources = ["自然流量", "广告投放", "邀请好友", "合作伙伴", "搜索引擎"]
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 风险标识 - 布尔类型指标
        is_blacklist_user = random.choice(["true", "false"])
        is_high_risk_ip = random.choice(["true", "false"])
        
        # 渠道风险 - 枚举类型指标
        channel_source = random.choice(channel_sources)
        
        risk_data.append((user_id, is_blacklist_user, is_high_risk_ip, channel_source))
    
    risk_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("is_blacklist_user", StringType(), True),
        StructField("is_high_risk_ip", StringType(), True),
        StructField("channel_source", StringType(), True)
    ])
    
    risk_df = spark.createDataFrame(risk_data, risk_schema)
    risk_df = risk_df.withColumn("dt", lit(dt))
    
    risk_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_risk_df")
    
    print("✅ 用户风险风控数据生成完成")
    
    # 6. 生成用户营销激励表 dws_user_marketing_df
    marketing_data = []
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 激励统计 - 数值类型指标
        red_packet_count = str(random.randint(0, 50))
        successful_invites_count = str(random.randint(0, 20))
        commission_rate = str(round(random.uniform(0, 5), 2))
        total_commission_amount = str(random.choice([0, 100, 500, 1000, 2000, 5000]))
        
        marketing_data.append((user_id, red_packet_count, successful_invites_count, commission_rate, total_commission_amount))
    
    marketing_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("red_packet_count", StringType(), True),
        StructField("successful_invites_count", StringType(), True),
        StructField("commission_rate", StringType(), True),
        StructField("total_commission_amount", StringType(), True)
    ])
    
    marketing_df = spark.createDataFrame(marketing_data, marketing_schema)
    marketing_df = marketing_df.withColumn("dt", lit(dt))
    
    marketing_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_marketing_df")
    
    print("✅ 用户营销激励数据生成完成")
    
    # 7. 生成用户行为偏好表 dws_user_behavior_df - 列表类型指标
    behavior_data = []
    coins = ["BTC", "ETH", "USDT", "BNB", "SOL", "XRP", "ADA", "DOT", "MATIC", "AVAX"]
    devices = ["Chrome_Win10", "Safari_macOS", "Chrome_Android", "Safari_iOS", "Firefox_Linux"]
    activities = ["signup_bonus", "trading_contest", "staking_event", "referral_program", "vip_upgrade"]
    rewards = ["体验金", "空投", "返佣", "手续费减免", "VIP权益"]
    coupons = ["赠金券", "手续费抵扣券", "交易券", "体验券", "升级券"]
    
    for i in range(user_count):
        user_id = f"user_{i:06d}"
        
        # 列表类型指标 - 使用array<string>类型
        current_holding_coins = random.sample(coins, random.randint(1, 5))
        traded_coins_list = random.sample(coins, random.randint(2, 8))
        device_fingerprint_list = random.sample(devices, random.randint(1, 3))
        participated_activity_ids = random.sample(activities, random.randint(0, 4))
        reward_claim_history = random.sample(rewards, random.randint(0, 3))
        used_coupon_types = random.sample(coupons, random.randint(0, 3))
        
        behavior_data.append((user_id, current_holding_coins, traded_coins_list, device_fingerprint_list,
                            participated_activity_ids, reward_claim_history, used_coupon_types))
    
    behavior_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("current_holding_coins", ArrayType(StringType()), True),
        StructField("traded_coins_list", ArrayType(StringType()), True),
        StructField("device_fingerprint_list", ArrayType(StringType()), True),
        StructField("participated_activity_ids", ArrayType(StringType()), True),
        StructField("reward_claim_history", ArrayType(StringType()), True),
        StructField("used_coupon_types", ArrayType(StringType()), True)
    ])
    
    behavior_df = spark.createDataFrame(behavior_data, behavior_schema)
    behavior_df = behavior_df.withColumn("dt", lit(dt))
    
    behavior_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .saveAsTable("dws_user.dws_user_behavior_df")
    
    print("✅ 用户行为偏好数据生成完成")
    
    # 验证数据
    print("\n📊 DWS层数据验证:")
    tables = [
        "dws_user_profile_df", 
        "dws_user_asset_df", 
        "dws_user_trading_df", 
        "dws_user_activity_df", 
        "dws_user_risk_df", 
        "dws_user_marketing_df", 
        "dws_user_behavior_df"
    ]
    
    for table in tables:
        count = spark.table(f'dws_user.{table}').count()
        print(f"   📊 dws_user.{table}: {count} 条记录")
    
    print("🎯 完整的DWS层7个表测试数据生成完成，已覆盖所有49个用户指标")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # 生成DWS层测试数据
        generate_dws_test_data(spark)
        
        print("🎉 DWS层测试数据生成完成！")
        
    finally:
        spark.stop()
