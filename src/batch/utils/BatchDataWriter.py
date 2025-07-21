"""
批处理数据写入器 - 重构为驼峰命名风格
支持UPSERT操作和时间戳控制，提供数据预处理、分区优化和写入验证
"""

import logging
import json
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, array_distinct, size, when, isnan, isnull

from src.batch.config.BaseConfig import MySQLConfig

logger = logging.getLogger(__name__)


class BatchDataWriter:
    """批处理数据写入器（原OptimizedMySQLWriter功能）"""
    
    def __init__(self, mysqlConfig: MySQLConfig):
        self.mysqlConfig = mysqlConfig
        self.logger = logging.getLogger(__name__)
    
    def writeTaggedUsers(self, taggedUsersDataFrame: DataFrame, enableValidation: bool = True) -> bool:
        """
        写入标签用户数据到MySQL
        
        Args:
            taggedUsersDataFrame: 标签用户DataFrame，必须包含user_id, tag_id列
            enableValidation: 是否启用写入验证
            
        Returns:
            bool: 写入是否成功
        """
        try:
            self.logger.info("🚀 开始写入标签用户数据...")
            
            # 1. 数据预处理和验证
            processedDataFrame = self._preprocessTaggedData(taggedUsersDataFrame)
            if processedDataFrame is None or processedDataFrame.count() == 0:
                self.logger.warning("⚠️ 没有有效数据需要写入")
                return True
            
            # 2. 动态分区优化
            optimizedDataFrame = self._optimizePartitions(processedDataFrame)
            
            # 3. 执行UPSERT写入
            self._executeUpsertWrite(optimizedDataFrame)
            
            # 4. 写入验证（可选）
            if enableValidation:
                validationResult = self._validateWriteResult(optimizedDataFrame)
                if not validationResult:
                    self.logger.error("❌ 写入验证失败")
                    return False
            
            self.logger.info("✅ 标签用户数据写入完成")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 写入标签用户数据失败: {str(e)}")
            return False
    
    def _preprocessTaggedData(self, rawDataFrame: DataFrame) -> Optional[DataFrame]:
        """
        预处理标签数据
        
        Args:
            rawDataFrame: 原始标签数据
            
        Returns:
            DataFrame: 预处理后的数据
        """
        try:
            self.logger.info("🔄 开始数据预处理...")
            
            # 验证必需字段
            requiredFields = ['user_id', 'tag_id']
            missingFields = set(requiredFields) - set(rawDataFrame.columns)
            if missingFields:
                self.logger.error(f"❌ 缺少必需字段: {missingFields}")
                return None
            
            # 过滤空值和重复数据
            cleanedDataFrame = rawDataFrame.filter(
                col("user_id").isNotNull() & 
                col("tag_id").isNotNull() & 
                ~col("user_id").isin("", "null", "NULL")
            ).dropDuplicates(["user_id", "tag_id"])
            
            # 按用户聚合标签
            aggregatedDataFrame = cleanedDataFrame.groupBy("user_id").agg(
                array_distinct(collect_list("tag_id")).alias("tag_ids"),
                collect_list("tag_detail").alias("tag_details_list")
            )
            
            # 构建最终格式
            finalDataFrame = aggregatedDataFrame.select(
                col("user_id"),
                col("tag_ids"),
                when(size(col("tag_details_list")) > 0, 
                     col("tag_details_list")).otherwise(None).alias("tag_details")
            )
            
            originalCount = rawDataFrame.count()
            processedCount = finalDataFrame.count()
            
            self.logger.info(f"📊 数据预处理完成: {originalCount} -> {processedCount} 用户")
            return finalDataFrame
            
        except Exception as e:
            self.logger.error(f"❌ 数据预处理失败: {str(e)}")
            return None
    
    def _optimizePartitions(self, dataFrame: DataFrame) -> DataFrame:
        """
        动态分区优化
        
        Args:
            dataFrame: 输入数据
            
        Returns:
            DataFrame: 优化分区后的数据
        """
        try:
            recordCount = dataFrame.count()
            
            # 根据数据量动态调整分区数
            if recordCount < 1000:
                partitionCount = 1
            elif recordCount < 10000:
                partitionCount = 2
            elif recordCount < 100000:
                partitionCount = 4
            else:
                partitionCount = 8
            
            optimizedDataFrame = dataFrame.repartition(partitionCount)
            self.logger.info(f"🔧 分区优化: {recordCount} 记录使用 {partitionCount} 个分区")
            
            return optimizedDataFrame
            
        except Exception as e:
            self.logger.warning(f"⚠️ 分区优化失败，使用原数据: {str(e)}")
            return dataFrame
    
    def _executeUpsertWrite(self, dataFrame: DataFrame):
        """
        执行UPSERT写入操作
        
        Args:
            dataFrame: 要写入的数据
        """
        def upsertPartition(iterator):
            """分区级别的UPSERT操作"""
            import pymysql
            
            connection = None
            try:
                # 建立MySQL连接
                connection = pymysql.connect(
                    host=self.mysqlConfig.host,
                    port=self.mysqlConfig.port,
                    user=self.mysqlConfig.username,
                    password=self.mysqlConfig.password,
                    database=self.mysqlConfig.database,
                    charset='utf8mb4',
                    autocommit=False
                )
                
                cursor = connection.cursor()
                batchSize = 100
                batch = []
                
                for row in iterator:
                    userId = row['user_id']
                    tagIds = json.dumps(row['tag_ids']) if row['tag_ids'] else '[]'
                    tagDetails = json.dumps(row['tag_details']) if row['tag_details'] else '{}'
                    
                    batch.append((userId, tagIds, tagDetails))
                    
                    if len(batch) >= batchSize:
                        self._executeBatch(cursor, batch)
                        batch = []
                
                # 处理剩余数据
                if batch:
                    self._executeBatch(cursor, batch)
                
                connection.commit()
                
            except Exception as e:
                if connection:
                    connection.rollback()
                logger.error(f"❌ 分区UPSERT失败: {str(e)}")
                raise
                
            finally:
                if connection:
                    connection.close()
            
            return iter([])
        
        # 执行分区级别的UPSERT
        self.logger.info("💾 执行UPSERT写入操作...")
        dataFrame.foreachPartition(upsertPartition)
    
    def _executeBatch(self, cursor, batch: List[tuple]):
        """
        执行批量UPSERT
        
        Args:
            cursor: 数据库游标
            batch: 批量数据
        """
        if not batch:
            return
        
        # 优化的UPSERT SQL - 智能时间戳控制
        upsertSql = """
            INSERT INTO user_tags (user_id, tag_ids, tag_details, created_time, updated_time)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                updated_time = CASE 
                    WHEN JSON_EXTRACT(tag_ids, '$') <> JSON_EXTRACT(VALUES(tag_ids), '$')
                    THEN CURRENT_TIMESTAMP 
                    ELSE updated_time 
                END,
                tag_ids = VALUES(tag_ids),
                tag_details = VALUES(tag_details)
        """
        
        cursor.executemany(upsertSql, batch)
    
    def _validateWriteResult(self, writtenDataFrame: DataFrame) -> bool:
        """
        验证写入结果
        
        Args:
            writtenDataFrame: 已写入的数据
            
        Returns:
            bool: 验证是否成功
        """
        try:
            self.logger.info("🔍 开始写入验证...")
            
            # 获取已写入的用户ID集合
            writtenUserIds = set(
                row['user_id'] for row in writtenDataFrame.select("user_id").distinct().collect()
            )
            
            if not writtenUserIds:
                self.logger.warning("⚠️ 没有用户ID需要验证")
                return True
            
            # 从数据库读取验证
            from src.batch.utils.RuleReader import RuleReader
            from pyspark.sql import SparkSession
            
            spark = SparkSession.getActiveSession()
            ruleReader = RuleReader(spark, self.mysqlConfig)
            
            existingTagsDataFrame = ruleReader.loadExistingUserTags()
            if existingTagsDataFrame is None:
                self.logger.warning("⚠️ 无法读取现有标签数据进行验证")
                return True
            
            # 验证写入的用户是否在数据库中
            dbUserIds = set(
                row['user_id'] for row in existingTagsDataFrame.select("user_id").distinct().collect()
            )
            
            missingUsers = writtenUserIds - dbUserIds
            if missingUsers:
                self.logger.warning(f"⚠️ 部分用户未在数据库中找到: {len(missingUsers)} 个用户")
                # 抽样显示缺失用户
                sampleMissing = list(missingUsers)[:5]
                self.logger.warning(f"   示例缺失用户: {sampleMissing}")
                return False
            
            self.logger.info(f"✅ 写入验证成功: {len(writtenUserIds)} 个用户全部验证通过")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 写入验证失败: {str(e)}")
            return False
    
    def getWriteStatistics(self, dataFrame: DataFrame) -> Dict[str, Any]:
        """
        获取写入统计信息
        
        Args:
            dataFrame: 数据DataFrame
            
        Returns:
            Dict[str, Any]: 统计信息
        """
        try:
            totalUsers = dataFrame.count()
            totalTags = dataFrame.agg({"tag_ids": "sum"}).collect()[0][0] or 0
            
            stats = {
                'total_users': totalUsers,
                'total_tags': totalTags,
                'avg_tags_per_user': round(totalTags / totalUsers, 2) if totalUsers > 0 else 0
            }
            
            self.logger.info(f"📊 写入统计: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ 获取写入统计失败: {str(e)}")
            return {}
    
    def testConnection(self) -> bool:
        """
        测试MySQL连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            import pymysql
            
            connection = pymysql.connect(
                host=self.mysqlConfig.host,
                port=self.mysqlConfig.port,
                user=self.mysqlConfig.username,
                password=self.mysqlConfig.password,
                database=self.mysqlConfig.database,
                charset='utf8mb4'
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            connection.close()
            
            if result:
                self.logger.info("✅ MySQL连接测试成功")
                return True
            else:
                self.logger.error("❌ MySQL连接测试失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ MySQL连接测试异常: {str(e)}")
            return False