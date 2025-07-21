"""
Hive数据读取器 - 重构为驼峰命名风格
从S3读取Hive表数据，支持列选择优化和批量读取
"""

import logging
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame

from src.batch.config.BaseConfig import S3Config
from src.batch.bean.HiveTableMeta import HiveTableMeta

logger = logging.getLogger(__name__)


class HiveDataReader:
    """Hive数据读取器（原HiveDataLoader功能）"""
    
    def __init__(self, spark: SparkSession, s3Config: S3Config):
        self.spark = spark
        self.s3Config = s3Config
        
    def readHiveTable(self, tablePath: str, selectedColumns: List[str] = None) -> Optional[DataFrame]:
        """
        从S3读取Hive表数据
        
        Args:
            tablePath: 表路径，例如 'user_basic_info/'
            selectedColumns: 需要选择的列，为None时选择所有列
            
        Returns:
            DataFrame或None
        """
        try:
            logger.info(f"📖 从S3读取Hive表: {tablePath}")
            
            # 构建完整的S3路径
            fullPath = f"s3a://{self.s3Config.bucket}/warehouse/{tablePath.rstrip('/')}"
            logger.info(f"📍 S3路径: {fullPath}")
            
            # 读取Parquet格式数据
            df = self.spark.read.parquet(fullPath)
            
            # 列选择优化
            if selectedColumns:
                # 验证列是否存在
                availableColumns = df.columns
                validColumns = [col for col in selectedColumns if col in availableColumns]
                
                if len(validColumns) != len(selectedColumns):
                    missingColumns = set(selectedColumns) - set(validColumns)
                    logger.warning(f"⚠️ 缺失列: {missingColumns}")
                
                if validColumns:
                    df = df.select(*validColumns)
                    logger.info(f"📊 已选择列: {validColumns}")
                else:
                    logger.error("❌ 没有有效的列可选择")
                    return None
            
            recordCount = df.count()
            logger.info(f"✅ 成功读取 {tablePath}，记录数: {recordCount}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取Hive表失败 {tablePath}: {str(e)}")
            return None
    
    def readMultipleTables(self, tableConfigs: Dict[str, List[str]]) -> Dict[str, DataFrame]:
        """
        批量读取多个表
        
        Args:
            tableConfigs: {表名: [列名列表]} 的字典
            
        Returns:
            {表名: DataFrame} 的字典
        """
        result = {}
        
        for tableName, columns in tableConfigs.items():
            logger.info(f"📊 批量读取表: {tableName}")
            df = self.readHiveTable(tableName, columns)
            if df is not None:
                result[tableName] = df
            else:
                logger.warning(f"⚠️ 表 {tableName} 读取失败，跳过")
        
        logger.info(f"✅ 批量读取完成，成功读取 {len(result)}/{len(tableConfigs)} 个表")
        return result
    
    def validateTableExists(self, tablePath: str) -> bool:
        """
        验证表是否存在
        
        Args:
            tablePath: 表路径
            
        Returns:
            bool: 表是否存在
        """
        try:
            fullPath = f"s3a://{self.s3Config.bucket}/warehouse/{tablePath.rstrip('/')}"
            df = self.spark.read.parquet(fullPath)
            df.count()  # 触发读取操作
            return True
        except Exception as e:
            logger.debug(f"表 {tablePath} 不存在或不可读: {str(e)}")
            return False
    
    def getTableSchema(self, tablePath: str) -> Optional[List[str]]:
        """
        获取表的列名列表
        
        Args:
            tablePath: 表路径
            
        Returns:
            List[str]: 列名列表，失败时返回None
        """
        try:
            fullPath = f"s3a://{self.s3Config.bucket}/warehouse/{tablePath.rstrip('/')}"
            df = self.spark.read.parquet(fullPath)
            return df.columns
        except Exception as e:
            logger.error(f"❌ 获取表结构失败 {tablePath}: {str(e)}")
            return None
    
    def getTableStats(self, tablePath: str) -> Optional[Dict[str, int]]:
        """
        获取表的统计信息
        
        Args:
            tablePath: 表路径
            
        Returns:
            Dict[str, int]: 统计信息（记录数、列数），失败时返回None
        """
        try:
            df = self.readHiveTable(tablePath)
            if df is None:
                return None
            
            recordCount = df.count()
            columnCount = len(df.columns)
            
            stats = {
                'record_count': recordCount,
                'column_count': columnCount
            }
            
            logger.info(f"📊 表 {tablePath} 统计: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ 获取表统计失败 {tablePath}: {str(e)}")
            return None
    
    def getTableMeta(self, tablePath: str) -> Optional[HiveTableMeta]:
        """
        获取表元数据Bean对象
        
        Args:
            tablePath: 表路径
            
        Returns:
            HiveTableMeta: 表元数据Bean，失败时返回None
        """
        try:
            logger.info(f"📊 获取表元数据: {tablePath}")
            
            # 获取表的基本信息
            columns = self.getTableSchema(tablePath)
            if columns is None:
                return None
            
            # 创建元数据Bean
            tableMeta = HiveTableMeta.fromBasicInfo(
                tableName=tablePath.split('/')[-1],
                tablePath=f"s3a://{self.s3Config.bucket}/warehouse/{tablePath.rstrip('/')}",
                columns=columns
            )
            
            # 获取详细统计信息
            try:
                df = self.readHiveTable(tablePath)
                if df is not None:
                    tableMeta.recordCount = df.count()
                    
                    # 添加一些样本数据
                    sampleRows = df.limit(3).collect()
                    for row in sampleRows:
                        tableMeta.addSampleRow(row.asDict())
                        
            except Exception as e:
                logger.warning(f"⚠️ 获取表 {tablePath} 统计信息失败: {str(e)}")
            
            logger.info(f"✅ 表元数据获取成功: {tableMeta}")
            return tableMeta
            
        except Exception as e:
            logger.error(f"❌ 获取表元数据失败 {tablePath}: {str(e)}")
            return None
    
    def readHiveTableWithMeta(self, tablePath: str, selectedColumns: List[str] = None) -> tuple[Optional[DataFrame], Optional[HiveTableMeta]]:
        """
        从 S3 读取 Hive 表数据并返回元数据
        
        Args:
            tablePath: 表路径，例如 'user_basic_info/'
            selectedColumns: 需要选择的列，为None时选择所有列
            
        Returns:
            tuple[DataFrame, HiveTableMeta]: DataFrame和元数据Bean
        """
        try:
            logger.info(f"📚 读取Hive表并获取元数据: {tablePath}")
            
            # 读取DataFrame
            df = self.readHiveTable(tablePath, selectedColumns)
            
            # 获取元数据
            tableMeta = self.getTableMeta(tablePath)
            
            if df is not None and tableMeta is not None:
                # 更新元数据中的统计信息
                if selectedColumns:
                    tableMeta.statistics['selected_columns'] = selectedColumns
                    tableMeta.statistics['selected_column_count'] = len(selectedColumns)
                
                logger.info(f"✅ 成功读取表和元数据: {tablePath}")
            
            return df, tableMeta
            
        except Exception as e:
            logger.error(f"❌ 读取表和元数据失败 {tablePath}: {str(e)}")
            return None, None