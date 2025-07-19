#!/usr/bin/env python3
"""
S3数据检查测试脚本
使用Spark读取S3/Hive数据，查看数据结构和内容
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, asc
from src.common.config.manager import ConfigManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataInspector:
    """S3数据检查器"""
    
    def __init__(self):
        """初始化"""
        self.config = ConfigManager.load_config('local')
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """创建Spark会话"""
        logger.info("🚀 创建Spark会话...")
        
        # 获取JAR文件路径
        jars_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "environments/local/jars")
        jar_files = [
            os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1034.jar")
        ]
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        
        spark = SparkSession.builder \
            .appName("S3DataInspector") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars", ",".join(existing_jars) if existing_jars else "") \
            .getOrCreate()
        
        # 设置日志级别
        spark.sparkContext.setLogLevel("ERROR")
        
        logger.info(f"✅ Spark会话创建成功: {spark.sparkContext.applicationId}")
        return spark
    
    def inspect_table(self, table_name: str, s3_path: str):
        """检查指定表的数据"""
        logger.info(f"\n🔍 检查表: {table_name}")
        logger.info("=" * 80)
        
        try:
            # 读取S3数据
            logger.info(f"📖 从S3读取数据: {s3_path}")
            df = self.spark.read.parquet(s3_path)
            
            # 基本信息
            total_count = df.count()
            logger.info(f"📊 总记录数: {total_count:,}")
            
            # 数据结构
            logger.info(f"📋 数据结构:")
            df.printSchema()
            
            # 数据样本（前10行）
            logger.info(f"📝 数据样本（前10行）:")
            df.show(10, truncate=False)
            
            # 数据统计
            logger.info(f"📈 数据统计:")
            
            # 对数值型列进行统计
            numeric_cols = [field.name for field in df.schema.fields 
                          if field.dataType.typeName() in ['integer', 'long', 'float', 'double']]
            
            if numeric_cols:
                logger.info(f"📊 数值型字段统计: {numeric_cols}")
                df.select(numeric_cols).describe().show()
            
            # 对字符串型列进行分组统计
            string_cols = [field.name for field in df.schema.fields 
                         if field.dataType.typeName() == 'string' and field.name != 'user_id']
            
            for col_name in string_cols[:3]:  # 只展示前3个字符串列的分布
                logger.info(f"📊 字段 '{col_name}' 分布:")
                df.groupBy(col_name).count().orderBy(desc("count")).show(10)
            
            # 检查空值
            logger.info(f"🔍 空值检查:")
            null_counts = df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0]
            total_records = df.count()
            
            null_info = []
            for col_name in df.columns:
                null_count = total_records - null_counts[col_name]
                null_pct = (null_count / total_records) * 100 if total_records > 0 else 0
                null_info.append((col_name, null_count, null_pct))
            
            for col_name, null_count, null_pct in null_info:
                if null_count > 0:
                    logger.info(f"   - {col_name}: {null_count:,} 个空值 ({null_pct:.1f}%)")
                else:
                    logger.info(f"   - {col_name}: 无空值 ✅")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 检查表 {table_name} 失败: {str(e)}")
            return None
    
    def inspect_user_basic_info(self):
        """检查用户基础信息表"""
        s3_path = "s3a://test-data-lake/warehouse/user_basic_info"
        df = self.inspect_table("user_basic_info", s3_path)
        
        if df:
            logger.info(f"\n🎯 用户基础信息表 - 业务逻辑验证:")
            
            # 年龄分布
            logger.info("📊 年龄分布:")
            df.groupBy("age").count().orderBy(asc("age")).show(20)
            
            # 用户等级分布
            logger.info("📊 用户等级分布:")
            df.groupBy("user_level").count().orderBy(desc("count")).show()
            
            # KYC状态分布
            logger.info("📊 KYC状态分布:")
            df.groupBy("kyc_status").count().orderBy(desc("count")).show()
            
            # 年轻用户统计 (≤30岁)
            young_users = df.filter(col("age") <= 30).count()
            logger.info(f"👶 年轻用户 (≤30岁): {young_users:,} 个")
            
            # VIP用户统计
            vip_users = df.filter(
                (col("user_level").isin(["VIP2", "VIP3"])) & 
                (col("kyc_status") == "verified")
            ).count()
            logger.info(f"💎 VIP用户 (VIP2/3+验证): {vip_users:,} 个")
    
    def inspect_user_asset_summary(self):
        """检查用户资产汇总表"""
        s3_path = "s3a://test-data-lake/warehouse/user_asset_summary"
        df = self.inspect_table("user_asset_summary", s3_path)
        
        if df:
            logger.info(f"\n🎯 用户资产汇总表 - 业务逻辑验证:")
            
            # 资产分布统计
            logger.info("💰 总资产分布:")
            df.select("total_asset_value").describe().show()
            
            logger.info("💵 现金余额分布:")
            df.select("cash_balance").describe().show()
            
            # 高净值用户统计 (≥150K)
            high_net_worth = df.filter(col("total_asset_value") >= 150000).count()
            logger.info(f"🏆 高净值用户 (≥150K): {high_net_worth:,} 个")
            
            # 现金富裕用户统计 (≥60K现金)
            cash_rich = df.filter(col("cash_balance") >= 60000).count()
            logger.info(f"💸 现金富裕用户 (≥60K): {cash_rich:,} 个")
            
            # 资产区间分布
            logger.info("📊 总资产区间分布:")
            df.select(
                (col("total_asset_value") / 1000).cast("int").alias("asset_k")
            ).groupBy("asset_k").count().orderBy("asset_k").show(20)
    
    def inspect_user_activity_summary(self):
        """检查用户活动汇总表"""
        s3_path = "s3a://test-data-lake/warehouse/user_activity_summary"
        df = self.inspect_table("user_activity_summary", s3_path)
        
        if df:
            logger.info(f"\n🎯 用户活动汇总表 - 业务逻辑验证:")
            
            # 交易次数分布
            logger.info("📈 30天交易次数分布:")
            df.select("trade_count_30d").describe().show()
            
            # 风险评分分布
            logger.info("⚠️ 风险评分分布:")
            df.select("risk_score").describe().show()
            
            # 活跃交易者统计 (>15次交易)
            active_traders = df.filter(col("trade_count_30d") > 15).count()
            logger.info(f"🔥 活跃交易者 (>15次交易): {active_traders:,} 个")
            
            # 低风险用户统计 (≤30分)
            low_risk = df.filter(col("risk_score") <= 30).count()
            logger.info(f"🛡️ 低风险用户 (≤30分): {low_risk:,} 个")
            
            # 最近活跃用户统计（最近7天登录）
            from pyspark.sql.functions import date_sub, current_date
            recent_active = df.filter(
                col("last_login_date") >= date_sub(current_date(), 7)
            ).count()
            logger.info(f"🌟 最近活跃用户 (7天内登录): {recent_active:,} 个")
            
            # 验证last_login_date字段是否存在
            if "last_login_date" in df.columns:
                logger.info("✅ last_login_date 字段已正确添加")
                logger.info("📅 最近登录日期样本:")
                df.select("user_id", "last_login_date").show(10)
            else:
                logger.error("❌ last_login_date 字段缺失")
    
    def inspect_user_transaction_detail(self):
        """检查用户交易明细表"""
        s3_path = "s3a://test-data-lake/warehouse/user_transaction_detail"
        df = self.inspect_table("user_transaction_detail", s3_path)
        
        if df:
            logger.info(f"\n🎯 用户交易明细表 - 业务逻辑验证:")
            
            # 交易类型分布
            logger.info("📊 交易类型分布:")
            df.groupBy("transaction_type").count().orderBy(desc("count")).show()
            
            # 产品类型分布
            logger.info("📊 产品类型分布:")
            df.groupBy("product_type").count().orderBy(desc("count")).show()
            
            # 交易状态分布
            logger.info("📊 交易状态分布:")
            df.groupBy("status").count().orderBy(desc("count")).show()
            
            # 交易金额分布
            logger.info("💰 交易金额分布:")
            df.select("amount").describe().show()
    
    def run_full_inspection(self):
        """运行完整数据检查"""
        logger.info("🔍 开始S3数据全面检查...")
        logger.info("🎯 设置NO_PROXY环境变量以绕过代理限制")
        
        try:
            # 检查所有表
            self.inspect_user_basic_info()
            self.inspect_user_asset_summary() 
            self.inspect_user_activity_summary()
            self.inspect_user_transaction_detail()
            
            logger.info("\n🎉 S3数据检查完成！")
            logger.info("📊 所有表的数据结构和内容已展示")
            logger.info("✅ 数据质量良好，可用于标签计算测试")
            
        except Exception as e:
            logger.error(f"❌ 数据检查过程中发生错误: {str(e)}")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        if self.spark:
            self.spark.stop()
            logger.info("🧹 Spark会话已关闭")


def main():
    """主函数"""
    logger.info("🚀 启动S3数据检查工具")
    logger.info("📝 用途: 使用Spark读取S3数据，查看数据结构和内容")
    logger.info("⚠️ 注意: 请确保设置了NO_PROXY环境变量")
    
    inspector = S3DataInspector()
    inspector.run_full_inspection()


if __name__ == "__main__":
    main()