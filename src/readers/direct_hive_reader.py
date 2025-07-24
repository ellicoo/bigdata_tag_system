"""
直接Hive表读取器
基于您提供的读表方式：spark.sql("MSCK REPAIR TABLE xxx") + spark.table("xxx").where("dt = 'xxx'")
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.config.base import BaseConfig


class DirectHiveReader:
    """直接Hive表读取器 - 基于您的HiveToKafka.py方式"""
    
    def __init__(self, spark: SparkSession, config: BaseConfig):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 获取Hive表名配置
        self.table_names = config.get_hive_table_names()
        
    def repair_and_read_table(self, table_name: str, dt: str = None) -> DataFrame:
        """
        修复分区并读取Hive表 - 基于您的方式
        
        Args:
            table_name: Hive表名（如 tag_test.user_basic_info）
            dt: 数据日期分区
            
        Returns:
            DataFrame: 读取的数据
        """
        try:
            # 1. 修复分区（基于您的方式）
            repair_sql = f"MSCK REPAIR TABLE {table_name}"
            self.logger.info(f"🔧 修复分区: {repair_sql}")
            self.spark.sql(repair_sql)
            
            # 2. 读取表数据（基于您的方式）
            if dt:
                df = self.spark.table(table_name).where(f"dt = '{dt}'")
                self.logger.info(f"📊 读取表 {table_name}，日期分区: {dt}")
            else:
                df = self.spark.table(table_name)
                self.logger.info(f"📊 读取表 {table_name}，全量数据")
            
            # 3. 验证数据
            count = df.count()
            self.logger.info(f"✅ 表 {table_name} 读取成功，记录数: {count}")
            
            if count == 0:
                self.logger.warning(f"⚠️ 表 {table_name} 无数据")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 读取表 {table_name} 失败: {str(e)}")
            raise
    
    def read_user_basic_info(self, dt: str = None) -> DataFrame:
        """读取用户基本信息表"""
        dt = dt or self.config.data_date
        table_name = self.table_names["user_basic_info"]
        
        df = self.repair_and_read_table(table_name, dt)
        
        # 验证必需字段
        required_fields = ['user_id', 'age', 'user_level', 'kyc_status', 'registration_date', 'risk_score']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            self.logger.warning(f"⚠️ 用户基本信息表缺少字段: {missing_fields}")
        
        return df
    
    def read_user_asset_summary(self, dt: str = None) -> DataFrame:
        """读取用户资产汇总表"""
        dt = dt or self.config.data_date
        table_name = self.table_names["user_asset_summary"]
        
        df = self.repair_and_read_table(table_name, dt)
        
        # 验证必需字段
        required_fields = ['user_id', 'total_asset_value', 'cash_balance']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            self.logger.warning(f"⚠️ 用户资产表缺少字段: {missing_fields}")
        
        return df
    
    def read_user_activity_summary(self, dt: str = None) -> DataFrame:
        """读取用户活动汇总表"""
        dt = dt or self.config.data_date
        table_name = self.table_names["user_activity_summary"]
        
        df = self.repair_and_read_table(table_name, dt)
        
        # 验证必需字段
        required_fields = ['user_id', 'trade_count_30d', 'last_login_date']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            self.logger.warning(f"⚠️ 用户活动表缺少字段: {missing_fields}")
        
        return df
    
    def read_all_user_data(self, dt: str = None) -> Dict[str, DataFrame]:
        """
        读取所有用户数据表
        
        Returns:
            Dict[str, DataFrame]: 包含所有用户数据的字典
        """
        dt = dt or self.config.data_date
        self.logger.info(f"🚀 开始读取所有用户数据，日期: {dt}")
        
        try:
            # 并行读取所有表
            user_basic = self.read_user_basic_info(dt)
            user_asset = self.read_user_asset_summary(dt)
            user_activity = self.read_user_activity_summary(dt)
            
            # 缓存数据以提高性能
            if self.config.enable_cache:
                user_basic.cache()
                user_asset.cache()
                user_activity.cache()
                self.logger.info("📦 数据已缓存到内存")
            
            result = {
                "user_basic_info": user_basic,
                "user_asset_summary": user_asset,
                "user_activity_summary": user_activity
            }
            
            self.logger.info("✅ 所有用户数据读取完成")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 读取用户数据失败: {str(e)}")
            raise
    
    def get_available_partitions(self, table_name: str) -> List[str]:
        """
        获取表的可用分区
        
        Args:
            table_name: 表名
            
        Returns:
            List[str]: 分区列表
        """
        try:
            partitions_df = self.spark.sql(f"SHOW PARTITIONS {table_name}")
            partitions = [row[0] for row in partitions_df.collect()]
            
            # 提取dt分区值
            dt_partitions = []
            for partition in partitions:
                if partition.startswith('dt='):
                    dt_value = partition.replace('dt=', '')
                    dt_partitions.append(dt_value)
            
            self.logger.info(f"📅 表 {table_name} 可用分区: {dt_partitions}")
            return dt_partitions
            
        except Exception as e:
            self.logger.error(f"❌ 获取分区失败: {str(e)}")
            return []
    
    def validate_table_access(self) -> bool:
        """验证Hive表访问权限"""
        try:
            self.logger.info("🔍 验证Hive表访问权限...")
            
            # 检查数据库是否存在
            databases = self.spark.sql("SHOW DATABASES").collect()
            db_names = [row[0] for row in databases]
            
            if self.config.hive_database not in db_names:
                self.logger.error(f"❌ 数据库 {self.config.hive_database} 不存在")
                self.logger.info(f"📋 可用数据库: {db_names}")
                return False
            
            # 检查表是否存在
            tables = self.spark.sql(f"SHOW TABLES IN {self.config.hive_database}").collect()
            table_names = [row[1] for row in tables]  # row[1]是表名
            
            required_tables = ['user_basic_info', 'user_asset_summary', 'user_activity_summary']
            missing_tables = [table for table in required_tables if table not in table_names]
            
            if missing_tables:
                self.logger.error(f"❌ 缺少必需的表: {missing_tables}")
                self.logger.info(f"📋 可用表: {table_names}")
                return False
            
            self.logger.info("✅ Hive表访问权限验证通过")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Hive表访问验证失败: {str(e)}")
            return False
    
    def show_table_schema(self, table_name: str = None):
        """显示表结构"""
        tables_to_show = [table_name] if table_name else list(self.table_names.values())
        
        for table in tables_to_show:
            try:
                self.logger.info(f"📋 表 {table} 结构:")
                schema_df = self.spark.sql(f"DESCRIBE {table}")
                schema_df.show(truncate=False)
                
                # 显示样例数据
                sample_df = self.spark.table(table).limit(3)
                self.logger.info(f"📊 表 {table} 样例数据:")
                sample_df.show(truncate=False)
                
            except Exception as e:
                self.logger.error(f"❌ 无法显示表 {table} 结构: {str(e)}")