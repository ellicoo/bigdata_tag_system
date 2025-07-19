#!/usr/bin/env python3
"""
标签规则验证测试脚本
验证S3数据是否满足各个标签的计算规则
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, current_date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TagRulesValidator:
    """标签规则验证器"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """创建Spark会话"""
        jars_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "environments/local/jars")
        jar_files = [
            os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1034.jar")
        ]
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        
        spark = SparkSession.builder \
            .appName("TagRulesValidator") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars", ",".join(existing_jars) if existing_jars else "") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    
    def load_data(self):
        """加载所有S3数据表"""
        logger.info("📖 加载S3数据表...")
        
        self.user_basic = self.spark.read.parquet("s3a://test-data-lake/warehouse/user_basic_info")
        self.user_asset = self.spark.read.parquet("s3a://test-data-lake/warehouse/user_asset_summary") 
        self.user_activity = self.spark.read.parquet("s3a://test-data-lake/warehouse/user_activity_summary")
        
        logger.info(f"✅ 用户基础信息: {self.user_basic.count():,} 条记录")
        logger.info(f"✅ 用户资产汇总: {self.user_asset.count():,} 条记录")
        logger.info(f"✅ 用户活动汇总: {self.user_activity.count():,} 条记录")
    
    def validate_tag_1_high_net_worth(self):
        """验证标签1: 高净值用户 (total_asset_value >= 100000)"""
        logger.info("\n🏷️ 标签1: 高净值用户")
        logger.info("📋 规则: total_asset_value >= 100000")
        
        # 应用规则
        qualified_users = self.user_asset.filter(col("total_asset_value") >= 100000)
        count = qualified_users.count()
        total = self.user_asset.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "total_asset_value", "cash_balance").show(5)
        
        return count
    
    def validate_tag_2_active_trader(self):
        """验证标签2: 活跃交易者 (trade_count_30d > 10)"""
        logger.info("\n🏷️ 标签2: 活跃交易者")
        logger.info("📋 规则: trade_count_30d > 10")
        
        qualified_users = self.user_activity.filter(col("trade_count_30d") > 10)
        count = qualified_users.count()
        total = self.user_activity.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "trade_count_30d", "last_login_date").show(5)
        
        return count
    
    def validate_tag_3_low_risk(self):
        """验证标签3: 低风险用户 (risk_score <= 30)"""
        logger.info("\n🏷️ 标签3: 低风险用户")
        logger.info("📋 规则: risk_score <= 30")
        
        qualified_users = self.user_activity.filter(col("risk_score") <= 30)
        count = qualified_users.count()
        total = self.user_activity.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "risk_score", "trade_count_30d").show(5)
        
        return count
    
    def validate_tag_4_new_user(self):
        """验证标签4: 新用户 (registration_date >= 30天前)"""
        logger.info("\n🏷️ 标签4: 新用户")
        logger.info("📋 规则: registration_date >= 30天前")
        
        qualified_users = self.user_basic.filter(
            col("registration_date") >= date_sub(current_date(), 30)
        )
        count = qualified_users.count()
        total = self.user_basic.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "registration_date", "user_level").show(5)
        
        return count
    
    def validate_tag_5_vip_user(self):
        """验证标签5: VIP用户 (user_level in ['VIP2', 'VIP3'] AND kyc_status = 'verified')"""
        logger.info("\n🏷️ 标签5: VIP用户")
        logger.info("📋 规则: user_level in ['VIP2', 'VIP3'] AND kyc_status = 'verified'")
        
        qualified_users = self.user_basic.filter(
            (col("user_level").isin(["VIP2", "VIP3"])) & 
            (col("kyc_status") == "verified")
        )
        count = qualified_users.count()
        total = self.user_basic.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "user_level", "kyc_status", "age").show(5)
        
        return count
    
    def validate_tag_6_cash_rich(self):
        """验证标签6: 现金富裕用户 (cash_balance >= 60000)"""
        logger.info("\n🏷️ 标签6: 现金富裕用户")
        logger.info("📋 规则: cash_balance >= 60000")
        
        qualified_users = self.user_asset.filter(col("cash_balance") >= 60000)
        count = qualified_users.count()
        total = self.user_asset.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "cash_balance", "total_asset_value").show(5)
        
        return count
    
    def validate_tag_7_young_user(self):
        """验证标签7: 年轻用户 (age <= 30)"""
        logger.info("\n🏷️ 标签7: 年轻用户")
        logger.info("📋 规则: age <= 30")
        
        qualified_users = self.user_basic.filter(col("age") <= 30)
        count = qualified_users.count()
        total = self.user_basic.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "age", "user_level", "registration_date").show(5)
        
        return count
    
    def validate_tag_8_recent_active(self):
        """验证标签8: 最近活跃用户 (last_login_date >= 7天前)"""
        logger.info("\n🏷️ 标签8: 最近活跃用户")
        logger.info("📋 规则: last_login_date >= 7天前")
        
        # 首先检查字段是否存在
        if "last_login_date" not in self.user_activity.columns:
            logger.error("❌ last_login_date 字段不存在于 user_activity_summary 表中")
            return 0
        
        qualified_users = self.user_activity.filter(
            col("last_login_date") >= date_sub(current_date(), 7)
        )
        count = qualified_users.count()
        total = self.user_activity.count()
        percentage = (count / total) * 100
        
        logger.info(f"✅ 符合条件用户: {count:,} / {total:,} ({percentage:.1f}%)")
        
        # 显示样本数据
        logger.info("📊 符合条件的用户样本:")
        qualified_users.select("user_id", "last_login_date", "trade_count_30d", "login_count_30d").show(5)
        
        return count
    
    def run_validation(self):
        """运行所有标签规则验证"""
        logger.info("🔍 开始标签规则验证...")
        logger.info("=" * 80)
        
        try:
            # 加载数据
            self.load_data()
            
            # 验证所有标签规则
            results = {}
            results[1] = self.validate_tag_1_high_net_worth()
            results[2] = self.validate_tag_2_active_trader()
            results[3] = self.validate_tag_3_low_risk()
            results[4] = self.validate_tag_4_new_user()
            results[5] = self.validate_tag_5_vip_user()
            results[6] = self.validate_tag_6_cash_rich()
            results[7] = self.validate_tag_7_young_user()
            results[8] = self.validate_tag_8_recent_active()
            
            # 汇总结果
            logger.info("\n📊 标签规则验证汇总")
            logger.info("=" * 80)
            
            tag_names = {
                1: "高净值用户",
                2: "活跃交易者", 
                3: "低风险用户",
                4: "新用户",
                5: "VIP用户",
                6: "现金富裕用户",
                7: "年轻用户",
                8: "最近活跃用户"
            }
            
            total_qualified = sum(results.values())
            for tag_id, count in results.items():
                percentage = (count / total_qualified) * 100 if total_qualified > 0 else 0
                logger.info(f"🏷️ 标签{tag_id} ({tag_names[tag_id]}): {count:,} 个用户 ({percentage:.1f}%)")
            
            # 验证结果
            min_users = min(results.values())
            if min_users < 10:
                logger.warning(f"⚠️ 某些标签匹配用户数较少，最少: {min_users}")
            else:
                logger.info(f"✅ 所有标签都有足够的匹配用户，最少: {min_users}")
            
            logger.info(f"\n🎉 标签规则验证完成！")
            logger.info(f"📊 总计: {len(results)} 个标签，{total_qualified:,} 个符合条件的用户")
            
        except Exception as e:
            logger.error(f"❌ 验证过程中发生错误: {str(e)}")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        if self.spark:
            self.spark.stop()
            logger.info("🧹 Spark会话已关闭")


def main():
    """主函数"""
    logger.info("🚀 启动标签规则验证工具")
    logger.info("📝 用途: 验证S3数据是否满足各个标签的计算规则")
    logger.info("⚠️ 注意: 请确保设置了NO_PROXY环境变量")
    
    validator = TagRulesValidator()
    validator.run_validation()


if __name__ == "__main__":
    main()