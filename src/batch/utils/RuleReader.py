"""
规则数据读取器 - 重构为驼峰命名风格
从MySQL读取标签规则和定义，支持DataFrame级别的缓存
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, IntegerType

from src.batch.config.BaseConfig import MySQLConfig

logger = logging.getLogger(__name__)


class RuleReader:
    """规则数据读取器（原RuleDataLoader功能）"""
    
    def __init__(self, spark: SparkSession, mysqlConfig: MySQLConfig):
        self.spark = spark
        self.mysqlConfig = mysqlConfig
        self._rulesCache = None
        self._existingTagsCache = None
    
    def getActiveRulesDataFrame(self) -> DataFrame:
        """
        获取活跃规则的DataFrame
        
        Returns:
            DataFrame: 包含所有活跃规则的DataFrame
        """
        if self._rulesCache is not None:
            return self._rulesCache
        
        try:
            logger.info("📖 从MySQL读取标签规则...")
            
            # 构建MySQL连接属性
            mysqlProps = {
                "user": self.mysqlConfig.username,
                "password": self.mysqlConfig.password,
                "driver": "com.mysql.cj.jdbc.Driver",
                "useSSL": "false",
                "allowPublicKeyRetrieval": "true",
                "connectionCollation": "utf8mb4_unicode_ci"
            }
            
            # 使用JOIN查询获取规则和定义
            query = """
                (SELECT 
                    tr.rule_id,
                    tr.tag_id,
                    tr.rule_conditions,
                    tr.is_active,
                    td.tag_name,
                    td.description as tag_description,
                    td.tag_category
                FROM tag_rules tr
                JOIN tag_definition td ON tr.tag_id = td.tag_id
                WHERE tr.is_active = 1 AND td.is_active = 1
                ORDER BY tr.tag_id
                ) as rules_query
            """
            
            df = self.spark.read.jdbc(
                url=self.mysqlConfig.jdbcUrl,
                table=query,
                properties=mysqlProps
            )
            
            # 不需要解析JSON，直接使用原始字符串
            
            # 缓存到内存和磁盘
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            
            recordCount = df.count()
            logger.info(f"✅ 成功读取 {recordCount} 条活跃规则并缓存到内存")
            
            self._rulesCache = df
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取规则数据失败: {str(e)}")
            raise
    
    def getRuleByTagId(self, tagId: int) -> Optional[Dict[str, Any]]:
        """
        根据标签ID获取规则
        
        Args:
            tagId: 标签ID
            
        Returns:
            Dict[str, Any]: 规则字典，失败时返回None
        """
        try:
            rulesDataFrame = self.getActiveRulesDataFrame()
            ruleRow = rulesDataFrame.filter(col("tag_id") == tagId).first()
            
            if ruleRow is None:
                logger.warning(f"⚠️ 未找到标签ID {tagId} 的规则")
                return None
            
            # 转换为字典
            ruleDict = ruleRow.asDict()
            return ruleDict
            
        except Exception as e:
            logger.error(f"❌ 获取标签 {tagId} 规则失败: {str(e)}")
            return None
    
    def loadExistingUserTags(self) -> Optional[DataFrame]:
        """
        加载现有用户标签数据
        
        Returns:
            DataFrame: 现有标签数据，失败时返回None
        """
        if self._existingTagsCache is not None:
            logger.info("✅ 使用缓存的现有标签数据")
            return self._existingTagsCache
        
        try:
            logger.info("📖 从MySQL读取现有用户标签...")
            
            mysqlProps = {
                "user": self.mysqlConfig.username,
                "password": self.mysqlConfig.password,
                "driver": "com.mysql.cj.jdbc.Driver",
                "useSSL": "false",
                "allowPublicKeyRetrieval": "true",
                "connectionCollation": "utf8mb4_unicode_ci"
            }
            
            # 读取user_tags表
            query = """
                (SELECT user_id, tag_ids, tag_details, updated_time
                FROM user_tags
                WHERE tag_ids IS NOT NULL AND tag_ids != '[]'
                ) as existing_tags_query
            """
            
            df = self.spark.read.jdbc(
                url=self.mysqlConfig.jdbcUrl,
                table=query,
                properties=mysqlProps
            )
            
            # JSON转换：tag_ids从字符串转为IntegerArray
            arraySchema = ArrayType(IntegerType(), True)
            df = df.withColumn("tag_ids_array", from_json(col("tag_ids"), arraySchema))
            
            # 缓存数据
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            
            recordCount = df.count()
            logger.info(f"✅ 成功读取 {recordCount} 条现有标签记录")
            
            self._existingTagsCache = df
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ 读取现有标签数据失败（可能是首次运行）: {str(e)}")
            return None
    
    def getTagDefinitions(self) -> DataFrame:
        """
        获取所有标签定义
        
        Returns:
            DataFrame: 标签定义数据
        """
        try:
            logger.info("📖 从MySQL读取标签定义...")
            
            mysqlProps = {
                "user": self.mysqlConfig.username,
                "password": self.mysqlConfig.password,
                "driver": "com.mysql.cj.jdbc.Driver",
                "useSSL": "false",
                "allowPublicKeyRetrieval": "true",
                "connectionCollation": "utf8mb4_unicode_ci"
            }
            
            df = self.spark.read.jdbc(
                url=self.mysqlConfig.jdbcUrl,
                table="tag_definition",
                properties=mysqlProps
            )
            
            activeDefinitions = df.filter(col("is_active") == 1)
            recordCount = activeDefinitions.count()
            logger.info(f"✅ 成功读取 {recordCount} 条活跃标签定义")
            
            return activeDefinitions
            
        except Exception as e:
            logger.error(f"❌ 读取标签定义失败: {str(e)}")
            raise
    
    def getTagCategories(self) -> DataFrame:
        """
        获取所有标签分类
        
        Returns:
            DataFrame: 标签分类数据
        """
        try:
            logger.info("📖 从MySQL读取标签分类...")
            
            mysqlProps = {
                "user": self.mysqlConfig.username,
                "password": self.mysqlConfig.password,
                "driver": "com.mysql.cj.jdbc.Driver",
                "useSSL": "false",
                "allowPublicKeyRetrieval": "true",
                "connectionCollation": "utf8mb4_unicode_ci"
            }
            
            df = self.spark.read.jdbc(
                url=self.mysqlConfig.jdbcUrl,
                table="tag_category",
                properties=mysqlProps
            )
            
            activeCategories = df.filter(col("is_active") == 1)
            recordCount = activeCategories.count()
            logger.info(f"✅ 成功读取 {recordCount} 条活跃标签分类")
            
            return activeCategories
            
        except Exception as e:
            logger.error(f"❌ 读取标签分类失败: {str(e)}")
            raise
    
    def validateConnection(self) -> bool:
        """
        验证MySQL连接是否正常
        
        Returns:
            bool: 连接是否正常
        """
        try:
            logger.info("🔍 验证MySQL连接...")
            
            mysqlProps = {
                "user": self.mysqlConfig.username,
                "password": self.mysqlConfig.password,
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # 执行简单查询测试连接
            testQuery = "(SELECT 1 as test_connection) as connection_test"
            testDataFrame = self.spark.read.jdbc(
                url=self.mysqlConfig.jdbcUrl,
                table=testQuery,
                properties=mysqlProps
            )
            
            testDataFrame.count()  # 触发实际执行
            logger.info("✅ MySQL连接验证成功")
            return True
            
        except Exception as e:
            logger.error(f"❌ MySQL连接验证失败: {str(e)}")
            return False
    
    def clearCache(self):
        """清理缓存数据"""
        try:
            if self._rulesCache is not None:
                self._rulesCache.unpersist()
                self._rulesCache = None
            
            if self._existingTagsCache is not None:
                self._existingTagsCache.unpersist()
                self._existingTagsCache = None
            
            logger.info("🧹 规则读取器缓存清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 缓存清理异常: {str(e)}")
    
    def getStatistics(self) -> Dict[str, int]:
        """
        获取规则和标签的统计信息
        
        Returns:
            Dict[str, int]: 统计信息
        """
        try:
            rulesDataFrame = self.getActiveRulesDataFrame()
            definitionsDataFrame = self.getTagDefinitions()
            categoriesDataFrame = self.getTagCategories()
            existingTagsDataFrame = self.loadExistingUserTags()
            
            stats = {
                'active_rules': rulesDataFrame.count(),
                'tag_definitions': definitionsDataFrame.count(),
                'tag_categories': categoriesDataFrame.count(),
                'users_with_tags': existingTagsDataFrame.count() if existingTagsDataFrame else 0
            }
            
            logger.info(f"📊 规则统计: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {str(e)}")
            return {}