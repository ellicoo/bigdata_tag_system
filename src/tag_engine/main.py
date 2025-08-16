#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算系统命令行入口
支持多种执行模式和参数配置
"""
import sys
import os
import argparse
from typing import List, Optional, Dict
from pyspark.sql import SparkSession

# 动态添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # 向上两级到项目根目录
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.tag_engine.engine.TagEngine import TagEngine


def create_spark_session(app_name: str = "TagComputeEngine") -> SparkSession:
    """创建Spark会话
    
    Args:
        app_name: 应用程序名称
        
    Returns:
        SparkSession: Spark会话
    """
    print(f"🚀 创建Spark会话: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark会话创建完成，版本: {spark.version}")
    return spark


def load_mysql_config() -> Dict[str, str]:
    """加载MySQL配置
    
    Returns:
        Dict: MySQL配置字典
    """
    # 从环境变量或配置文件加载
    # 海豚调度器环境使用统一配置
    import os

    # return {
    #     "host": os.getenv("MYSQL_HOST",
    #                       "rm-3ns765y13i6wf0hp3.mysql.rds.aliyuncs.com"),
    #     "port": int(os.getenv("MYSQL_PORT", "3358")),
    #     "database": os.getenv("MYSQL_DATABASE", "biz_user"),
    #     "user": os.getenv("MYSQL_USER", "dev_rw"),
    #     "password": os.getenv("MYSQL_PASSWORD", "nLjE49a20!h6vhHF"),
    #     "charset": "utf8mb4"
    # }

    return {
        "host": os.getenv("MYSQL_HOST",
                          "cex-mysql-ex-test-cluster.cluster-c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com"),
        "port": int(os.getenv("MYSQL_PORT", "3358")),
        "database": os.getenv("MYSQL_DATABASE", "biz_statistics"),
        "user": os.getenv("MYSQL_USER", "ex_test_rw"),
        "password": os.getenv("MYSQL_PASSWORD", "NqaBacRMzCKRRqfEWb"),
        "charset": "utf8mb4"
    }


def parse_tag_ids(tag_ids_str: Optional[str]) -> Optional[List[int]]:
    """解析标签ID字符串
    
    Args:
        tag_ids_str: 逗号分隔的标签ID字符串
        
    Returns:
        List[int]: 标签ID列表，None表示所有标签
    """
    if not tag_ids_str:
        return None
    
    try:
        tag_ids = [int(tag_id.strip()) for tag_id in tag_ids_str.split(",")]
        return tag_ids
    except ValueError as e:
        print(f"❌ 标签ID解析失败: {e}")
        return None


def generate_comprehensive_test_data(spark) -> bool:
    """生成完整的测试数据，匹配新的DWS层表结构
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: 生成是否成功
    """
    try:
        # 创建DWS数据库
        spark.sql("CREATE DATABASE IF NOT EXISTS dws_user")
        print("✅ 数据库 dws_user 创建成功")
        
        print("🏗️ 创建DWS层表结构...")
        
        # 1. 用户基础画像表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_profile_df (
                user_id STRING,
                register_time STRING,
                register_source_channel STRING,
                register_method STRING,
                register_country STRING,
                is_kyc_completed STRING,
                kyc_country STRING,
                is_2fa_enabled STRING,
                user_level STRING,
                is_agent STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 2. 用户资产财务表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_asset_df (
                user_id STRING,
                total_deposit_amount STRING,
                total_withdraw_amount STRING,
                net_deposit_amount STRING,
                last_deposit_time STRING,
                last_withdraw_time STRING,
                withdraw_count_30d STRING,
                deposit_fail_count STRING,
                spot_position_value STRING,
                contract_position_value STRING,
                finance_position_value STRING,
                onchain_position_value STRING,
                current_total_position_value STRING,
                spot_available_balance STRING,
                contract_available_balance STRING,
                onchain_available_balance STRING,
                available_balance STRING,
                spot_locked_amount STRING,
                contract_locked_amount STRING,
                finance_locked_amount STRING,
                onchain_locked_amount STRING,
                locked_amount STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 3. 用户交易行为表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_trading_df (
                user_id STRING,
                spot_trading_volume STRING,
                contract_trading_volume STRING,
                spot_recent_30d_volume STRING,
                contract_recent_30d_volume STRING,
                spot_trade_count STRING,
                contract_trade_count STRING,
                finance_trade_count STRING,
                onchain_trade_count STRING,
                first_trade_time STRING,
                last_trade_time STRING,
                has_contract_trading STRING,
                contract_trading_style STRING,
                has_finance_management STRING,
                has_pending_orders STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 4. 用户活跃行为表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_activity_df (
                user_id STRING,
                days_since_register STRING,
                days_since_last_login STRING,
                last_login_time STRING,
                last_activity_time STRING,
                login_count_7d STRING,
                login_ip_address STRING,
                country_region_code STRING,
                email_suffix STRING,
                operating_system STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 5. 用户风险风控表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_risk_df (
                user_id STRING,
                is_blacklist_user STRING,
                is_high_risk_ip STRING,
                channel_source STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 6. 用户营销激励表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_marketing_df (
                user_id STRING,
                red_packet_count STRING,
                successful_invites_count STRING,
                commission_rate STRING,
                total_commission_amount STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 7. 用户行为偏好表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS dws_user.dws_user_behavior_df (
                user_id STRING,
                current_holding_coins ARRAY<STRING>,
                traded_coins_list ARRAY<STRING>,
                device_fingerprint_list ARRAY<STRING>,
                participated_activity_ids ARRAY<STRING>,
                reward_claim_history ARRAY<STRING>,
                used_coupon_types ARRAY<STRING>
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        print("✅ DWS层表结构创建完成")
        
        # 生成DWS层测试数据
        print("📊 生成DWS层测试数据...")
        
        import random
        from datetime import datetime, timedelta
        
        # 生成1000个用户的数据
        user_count = 1000
        
        # 1. 用户基础画像数据
        profile_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            register_time = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d %H:%M:%S")
            
            profile_data.append((
                user_id,
                register_time,
                random.choice(["官网", "APP", "推荐", "广告"]),
                random.choice(["邮箱", "手机"]),
                random.choice(["CN", "US", "SG", "JP"]),
                random.choice(["true", "false"]),
                random.choice(["CN", "US", "SG", "JP"]),
                random.choice(["true", "false"]),
                random.choice(["VIP1", "VIP2", "VIP3", "VIP4", "NORMAL"]),
                random.choice(["true", "false"])
            ))
        
        profile_df = spark.createDataFrame(profile_data, [
            "user_id", "register_time", "register_source_channel", "register_method", "register_country",
            "is_kyc_completed", "kyc_country", "is_2fa_enabled", "user_level", "is_agent"
        ])
        
        # 2. 用户资产数据
        asset_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            # 生成资产数据，使用字符串类型
            spot_position = str(random.choice([0, 5000, 25000, 50000, 100000]))
            contract_position = str(random.choice([0, 10000, 50000, 100000, 200000]))
            finance_position = str(random.choice([0, 20000, 50000, 100000, 300000]))
            onchain_position = str(random.choice([0, 5000, 10000, 25000, 50000]))
            
            total_position = str(int(spot_position) + int(contract_position) + int(finance_position) + int(onchain_position))
            
            asset_data.append((
                user_id,
                str(random.randint(0, 1000000)),  # total_deposit_amount
                str(random.randint(0, 500000)),   # total_withdraw_amount
                str(random.randint(-100000, 500000)),  # net_deposit_amount
                (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d %H:%M:%S"),
                (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d %H:%M:%S"),
                str(random.randint(0, 10)),       # withdraw_count_30d
                str(random.randint(0, 5)),        # deposit_fail_count
                spot_position,
                contract_position,
                finance_position,
                onchain_position,
                total_position,
                str(random.randint(0, 50000)),    # spot_available_balance
                str(random.randint(0, 100000)),   # contract_available_balance
                str(random.randint(0, 25000)),    # onchain_available_balance
                str(random.randint(0, 175000)),   # available_balance
                str(random.randint(0, 10000)),    # spot_locked_amount
                str(random.randint(0, 50000)),    # contract_locked_amount
                str(random.randint(0, 100000)),   # finance_locked_amount
                str(random.randint(0, 25000)),    # onchain_locked_amount
                str(random.randint(0, 185000))    # locked_amount
            ))
        
        asset_df = spark.createDataFrame(asset_data, [
            "user_id", "total_deposit_amount", "total_withdraw_amount", "net_deposit_amount",
            "last_deposit_time", "last_withdraw_time", "withdraw_count_30d", "deposit_fail_count",
            "spot_position_value", "contract_position_value", "finance_position_value", "onchain_position_value", "current_total_position_value",
            "spot_available_balance", "contract_available_balance", "onchain_available_balance", "available_balance",
            "spot_locked_amount", "contract_locked_amount", "finance_locked_amount", "onchain_locked_amount", "locked_amount"
        ])
        
        # 3. 用户交易行为数据
        trading_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            trading_data.append((
                user_id,
                str(random.randint(0, 500000)),   # spot_trading_volume
                str(random.randint(0, 1000000)),  # contract_trading_volume
                str(random.randint(0, 50000)),    # spot_recent_30d_volume
                str(random.randint(0, 100000)),   # contract_recent_30d_volume
                str(random.randint(0, 100)),      # spot_trade_count
                str(random.randint(0, 200)),      # contract_trade_count
                str(random.randint(0, 50)),       # finance_trade_count
                str(random.randint(0, 20)),       # onchain_trade_count
                (datetime.now() - timedelta(days=random.randint(30, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
                (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(["true", "false"]),
                random.choice(["激进", "稳健", "保守"]),
                random.choice(["true", "false"]),
                random.choice(["true", "false"])
            ))
        
        trading_df = spark.createDataFrame(trading_data, [
            "user_id", "spot_trading_volume", "contract_trading_volume", "spot_recent_30d_volume", "contract_recent_30d_volume",
            "spot_trade_count", "contract_trade_count", "finance_trade_count", "onchain_trade_count",
            "first_trade_time", "last_trade_time", "has_contract_trading", "contract_trading_style", "has_finance_management", "has_pending_orders"
        ])
        
        # 4. 用户活跃行为数据
        activity_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            activity_data.append((
                user_id,
                str(random.randint(1, 1500)),     # days_since_register
                str(random.randint(0, 30)),       # days_since_last_login
                (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S"),
                (datetime.now() - timedelta(hours=random.randint(1, 72))).strftime("%Y-%m-%d %H:%M:%S"),
                str(random.randint(0, 10)),       # login_count_7d
                f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                random.choice(["CN", "US", "SG", "JP", "UK"]),
                random.choice(["gmail.com", "yahoo.com", "qq.com", "163.com"]),
                random.choice(["Windows", "macOS", "iOS", "Android", "Linux"])
            ))
        
        activity_df = spark.createDataFrame(activity_data, [
            "user_id", "days_since_register", "days_since_last_login", "last_login_time", "last_activity_time",
            "login_count_7d", "login_ip_address", "country_region_code", "email_suffix", "operating_system"
        ])
        
        # 5. 用户风险数据
        risk_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            risk_data.append((
                user_id,
                random.choice(["true", "false", "false", "false"]),  # 大部分不是黑名单
                random.choice(["true", "false", "false"]),           # 少数高风险IP
                random.choice(["官网", "推荐", "广告", "合作伙伴"])
            ))
        
        risk_df = spark.createDataFrame(risk_data, [
            "user_id", "is_blacklist_user", "is_high_risk_ip", "channel_source"
        ])
        
        # 6. 用户营销数据
        marketing_data = []
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            marketing_data.append((
                user_id,
                str(random.randint(0, 20)),       # red_packet_count
                str(random.randint(0, 10)),       # successful_invites_count
                str(random.choice(["0.01", "0.02", "0.05", "0.1"])),  # commission_rate
                str(random.randint(0, 10000))     # total_commission_amount
            ))
        
        marketing_df = spark.createDataFrame(marketing_data, [
            "user_id", "red_packet_count", "successful_invites_count", "commission_rate", "total_commission_amount"
        ])
        
        # 7. 用户行为偏好数据
        behavior_data = []
        coins = ["BTC", "ETH", "BNB", "USDT", "ADA", "DOT", "LINK", "UNI"]
        activities = ["新人活动", "交易赛", "理财活动", "推荐活动"]
        rewards = ["现金", "代币", "手续费减免", "VIP权益"]
        coupons = ["交易券", "理财券", "手续费券"]
        
        for i in range(user_count):
            user_id = f"user_{i+1:05d}"
            
            behavior_data.append((
                user_id,
                random.sample(coins, random.randint(1, 4)),
                random.sample(coins, random.randint(2, 6)),
                [f"device_{random.randint(1000, 9999)}" for _ in range(random.randint(1, 3))],
                random.sample(activities, random.randint(0, 2)),
                random.sample(rewards, random.randint(0, 3)),
                random.sample(coupons, random.randint(0, 2))
            ))
        
        behavior_df = spark.createDataFrame(behavior_data, [
            "user_id", "current_holding_coins", "traded_coins_list", "device_fingerprint_list",
            "participated_activity_ids", "reward_claim_history", "used_coupon_types"
        ])
        
        # 插入数据
        print("💾 插入DWS层测试数据...")
        profile_df.write.mode("overwrite").insertInto("dws_user.dws_user_profile_df")
        asset_df.write.mode("overwrite").insertInto("dws_user.dws_user_asset_df")
        trading_df.write.mode("overwrite").insertInto("dws_user.dws_user_trading_df")
        activity_df.write.mode("overwrite").insertInto("dws_user.dws_user_activity_df")
        risk_df.write.mode("overwrite").insertInto("dws_user.dws_user_risk_df")
        marketing_df.write.mode("overwrite").insertInto("dws_user.dws_user_marketing_df")
        behavior_df.write.mode("overwrite").insertInto("dws_user.dws_user_behavior_df")
        
        # 验证数据
        print("🔍 验证生成的DWS层测试数据...")
        dws_tables = ["dws_user_profile_df", "dws_user_asset_df", "dws_user_trading_df", 
                      "dws_user_activity_df", "dws_user_risk_df", "dws_user_marketing_df", "dws_user_behavior_df"]
        for table in dws_tables:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM dws_user.{table}").collect()[0]['cnt']
            print(f"   📊 dws_user.{table}: {count} 条记录")
        
        print("🎯 DWS层测试数据生成完成，已确保多样性匹配所有标签条件")
        return True
        
    except Exception as e:
        print(f"❌ 测试数据生成失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="标签计算系统")
    parser.add_argument(
        "--mode", 
        choices=["health", "full", "specific", "task-all", "task-tags", "generate-test-data", "list-tasks"],
        default="health",
        help="执行模式：health(健康检查)、full/task-all(全量计算)、specific/task-tags(指定标签)、generate-test-data(生成测试数据)、list-tasks(列出任务)"
    )
    parser.add_argument(
        "--tag-ids",
        type=str,
        help="指定标签ID列表，逗号分隔，如: 1,2,3"
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="TagComputeEngine",
        help="Spark应用程序名称"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="详细输出模式"
    )
    
    args = parser.parse_args()
    
    # 设置详细输出
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("🏷️  大数据标签计算系统")
    print("=" * 60)
    print(f"执行模式: {args.mode}")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"脚本目录: {current_dir}")
    print(f"项目根目录: {project_root}")
    print(f"Python路径前3项: {sys.path[:3]}")
    
    if args.tag_ids:
        print(f"指定标签: {args.tag_ids}")
    
    spark = None
    tag_engine = None
    
    try:
        # 1. 创建Spark会话
        spark = create_spark_session(args.app_name)
        
        # 2. 加载配置
        mysql_config = load_mysql_config()
        print(f"MySQL配置: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        
        # 3. 创建标签引擎（HiveMeta内部自动处理当天分区）
        tag_engine = TagEngine(spark, mysqlConfig=mysql_config)
        
        # 4. 根据模式执行相应操作
        success = False
        
        if args.mode == "health":
            print("\n🔍 执行健康检查...")
            success = tag_engine.healthCheck()
            
        elif args.mode in ["full", "task-all"]:
            print("\n🚀 执行全量标签计算...")
            success, failed_tag_ids = tag_engine.computeTags(mode="task-all")
            if failed_tag_ids:
                print(f"⚠️  {len(failed_tag_ids)} 个标签因表加载失败而跳过: {failed_tag_ids}")
            else:
                print("✅ 所有标签计算成功")
            
        elif args.mode in ["specific", "task-tags"]:
            tag_ids = parse_tag_ids(args.tag_ids)
            if tag_ids is None:
                print("❌ 指定标签模式需要提供 --tag-ids 参数")
                sys.exit(1)
            
            print(f"\n🎯 执行指定标签计算: {tag_ids}")
            success, failed_tag_ids = tag_engine.computeTags(mode="task-tags", tagIds=tag_ids)
            if failed_tag_ids:
                print(f"⚠️  {len(failed_tag_ids)} 个标签因表加载失败而跳过: {failed_tag_ids}")
            else:
                print("✅ 所有指定标签计算成功")
            
        elif args.mode == "generate-test-data":
            print("\n🧪 生成测试数据...")
            # 先创建数据库
            spark.sql("CREATE DATABASE IF NOT EXISTS tag_system")
            spark.sql("CREATE DATABASE IF NOT EXISTS dws_user")
            print("✅ 数据库 tag_system 和 dws_user 创建成功")
            
            # 使用部署包中的测试数据生成器
            try:
                # 尝试导入部署包中的测试数据生成器（海豚调度器环境）
                from generate_test_data import generate_test_data
                generate_test_data(spark)
                success = True
                print("✅ 测试数据生成完成")
            except ImportError:
                # 如果找不到部署包的生成器，使用内置生成器
                print("🔄 使用内置测试数据生成器...")
                success = generate_comprehensive_test_data(spark)
                if success:
                    print("✅ 内置测试数据生成完成")
                else:
                    print("❌ 内置测试数据生成失败")
            
        elif args.mode == "list-tasks":
            print("\n📋 列出可用标签任务...")
            from src.tag_engine.meta.MysqlMeta import MysqlMeta
            mysql_meta = MysqlMeta(spark, mysql_config)
            
            try:
                tags = mysql_meta.loadTagRules()
                print("可用标签任务:")
                for tag in tags.collect():
                    print(f"  {tag.tag_id}: {tag.tag_name if hasattr(tag, 'tag_name') else '未知标签'}")
                success = True
            except Exception as e:
                print(f"❌ 获取标签列表失败: {e}")
                success = False
        
        # 5. 输出结果
        print("\n" + "=" * 60)
        if success:
            print("✅ 任务执行成功")
            exit_code = 0
        else:
            print("❌ 任务执行失败")
            exit_code = 1
        
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️  任务被用户中断")
        exit_code = 2
        
    except Exception as e:
        print(f"\n❌ 系统异常: {e}")
        import traceback
        if args.verbose:
            traceback.print_exc()
        exit_code = 3
        
    finally:
        # 清理资源
        if tag_engine:
            tag_engine.cleanup()
        
        if spark:
            print("🧹 关闭Spark会话...")
            spark.stop()
        
        print("👋 程序退出")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()