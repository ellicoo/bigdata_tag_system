"""
批处理统一数据加载器
整合原有的HiveDataReader和RuleReader功能
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, IntegerType

from src.common.config.base import BaseConfig, S3Config, MySQLConfig

logger = logging.getLogger(__name__)


class BatchDataLoader:
    """批处理统一数据加载器"""
    
    def __init__(self, spark: SparkSession, config: BaseConfig):
        self.spark = spark
        self.config = config
        self.hive_loader = HiveDataLoader(spark, config.s3)
        self.rule_loader = RuleDataLoader(spark, config.mysql)
        
    def load_user_data(self, source_name: str, fields: List[str] = None) -> DataFrame:
        """加载用户数据"""
        return self.hive_loader.read_hive_table(source_name, fields)
        
    def load_rules(self) -> DataFrame:
        """加载标签规则"""
        return self.rule_loader.get_active_rules_df()
        
    def preload_existing_tags(self) -> DataFrame:
        """预加载现有标签"""
        return self.rule_loader.load_existing_user_tags()


class HiveDataLoader:
    """Hive数据加载器（原HiveDataReader功能）"""
    
    def __init__(self, spark: SparkSession, s3_config: S3Config):
        self.spark = spark
        self.s3_config = s3_config
        
    def read_hive_table(self, table_path: str, selected_columns: List[str] = None) -> Optional[DataFrame]:
        """
        从S3读取Hive表数据
        
        Args:
            table_path: 表路径，例如 'user_basic_info/'
            selected_columns: 需要选择的列，为None时选择所有列
            
        Returns:
            DataFrame或None
        """
        try:
            logger.info(f"📖 从S3读取Hive表: {table_path}")
            
            # 构建完整的S3路径
            full_path = f"s3a://{self.s3_config.bucket}/warehouse/{table_path.rstrip('/')}"
            logger.info(f"📍 S3路径: {full_path}")
            
            # 读取Parquet格式数据
            df = self.spark.read.parquet(full_path)
            
            # 列选择优化
            if selected_columns:
                # 验证列是否存在
                available_columns = df.columns
                valid_columns = [col for col in selected_columns if col in available_columns]
                
                if len(valid_columns) != len(selected_columns):
                    missing_columns = set(selected_columns) - set(valid_columns)
                    logger.warning(f"⚠️ 缺失列: {missing_columns}")
                
                if valid_columns:
                    df = df.select(*valid_columns)
                    logger.info(f"📊 已选择列: {valid_columns}")
                else:
                    logger.error("❌ 没有有效的列可选择")
                    return None
            
            record_count = df.count()
            logger.info(f"✅ 成功读取 {table_path}，记录数: {record_count}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取Hive表失败 {table_path}: {str(e)}")
            return None
    
    def read_multiple_tables(self, table_configs: Dict[str, List[str]]) -> Dict[str, DataFrame]:
        """
        批量读取多个表
        
        Args:
            table_configs: {表名: [列名列表]} 的字典
            
        Returns:
            {表名: DataFrame} 的字典
        """
        results = {}
        
        for table_name, columns in table_configs.items():
            df = self.read_hive_table(table_name, columns)
            if df is not None:
                results[table_name] = df
            else:
                logger.warning(f"⚠️ 跳过表 {table_name}（读取失败）")
        
        logger.info(f"📊 批量读取完成，成功加载 {len(results)}/{len(table_configs)} 个表")
        return results


class RuleDataLoader:
    """规则数据加载器（原RuleReader功能）"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        self._rules_df = None
        self._initialized = False
    
    def initialize(self):
        """初始化规则数据"""
        if self._initialized:
            logger.info("规则读取器已初始化，使用缓存数据")
            return
        
        logger.info("🔄 开始一次性加载完整规则数据（JOIN包含标签定义）...")
        
        try:
            self._load_rules_df()
            self._initialized = True
            logger.info("✅ 规则读取器初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 规则读取器初始化失败: {str(e)}")
            raise
    
    def _load_rules_df(self):
        """加载标签规则DataFrame并persist"""
        logger.info("📖 加载标签规则...")
        
        query = """
        (SELECT 
            tr.rule_id,
            tr.tag_id,
            tr.rule_conditions,
            tr.is_active as rule_active,
            td.tag_name,
            td.tag_category,
            td.description as tag_description,
            td.is_active as tag_active
         FROM tag_rules tr 
         JOIN tag_definition td ON tr.tag_id = td.tag_id 
         WHERE tr.is_active = 1 AND td.is_active = 1) as active_rules
        """
        
        self._rules_df = self.spark.read.jdbc(
            url=self.mysql_config.jdbc_url,
            table=query,
            properties=self.mysql_config.connection_properties
        ).persist(StorageLevel.MEMORY_AND_DISK)
        
        # 触发持久化并获取统计
        rule_count = self._rules_df.count()
        logger.info(f"✅ 完整规则DataFrame已persist(内存&磁盘)，共 {rule_count} 条（包含标签定义）")
    
    def get_active_rules_df(self) -> DataFrame:
        """获取活跃标签规则DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._rules_df
    
    def get_tag_definitions_df(self) -> DataFrame:
        """获取标签定义DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._rules_df.select("tag_id", "tag_name", "tag_category", "tag_description").distinct()
    
    def load_existing_user_tags(self) -> Optional[DataFrame]:
        """预缓存MySQL中的现有标签数据"""
        try:
            logger.info("🔄 预缓存MySQL现有用户标签数据-persist(内存&磁盘)...")
            
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            if existing_df.count() == 0:
                logger.info("MySQL中暂无现有用户标签数据")
                return None
            else:
                # JSON转换：将tag_ids从字符串转为数组
                processed_df = existing_df.select(
                    "user_id",
                    from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                    "tag_details",
                    "created_time", 
                    "updated_time"
                )
                
                cached_df = processed_df.persist(StorageLevel.MEMORY_AND_DISK)
                # 触发缓存
                cached_count = cached_df.count()
                logger.info(f"✅ 预缓存MySQL现有用户标签数据完成（含JSON转换），共 {cached_count} 条记录")
                return cached_df
            
        except Exception as e:
            logger.warning(f"⚠️ 预缓存MySQL现有用户标签数据失败: {str(e)}")
            return None
    
    def cleanup(self):
        """清理缓存，释放资源"""
        logger.info("🧹 清理规则读取器缓存...")
        
        try:
            if self._rules_df is not None:
                logger.info("🧹 释放规则DataFrame persist缓存")
                self._rules_df.unpersist()
                
            # 清空引用
            self._rules_df = None
            self._initialized = False
            
            logger.info("✅ 规则读取器缓存清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 规则读取器缓存清理异常: {str(e)}")