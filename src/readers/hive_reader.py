import logging
from datetime import datetime, timedelta
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from src.config.base import S3Config

logger = logging.getLogger(__name__)


class HiveDataReader:
    """Hive数据读取器 - 从S3读取Hive表数据"""
    
    def __init__(self, spark: SparkSession, s3_config: S3Config):
        self.spark = spark
        self.s3_config = s3_config
        self._cached_tables = {}  # 缓存已读取的表
    
    def read_table_data(self, table_name: str, required_fields: Optional[str] = None, 
                       partition_filter: Optional[str] = None) -> DataFrame:
        """
        从S3读取Hive表数据
        
        Args:
            table_name: 表名
            required_fields: 需要的字段，逗号分隔
            partition_filter: 分区过滤条件
        """
        try:
            # 构建S3路径
            s3_path = f"s3a://{self.s3_config.bucket}/warehouse/{table_name}/"
            logger.info(f"开始读取表: {table_name} from {s3_path}")
            
            # 读取parquet文件
            df = self.spark.read.parquet(s3_path)
            
            # 应用分区过滤
            if partition_filter:
                df = df.filter(partition_filter)
                logger.info(f"应用分区过滤: {partition_filter}")
            
            # 字段裁剪
            if required_fields:
                field_list = [f.strip() for f in required_fields.split(',')]
                # 确保user_id字段存在
                if 'user_id' not in field_list:
                    field_list.append('user_id')
                
                # 检查字段是否存在
                available_fields = df.columns
                valid_fields = [f for f in field_list if f in available_fields]
                missing_fields = [f for f in field_list if f not in available_fields]
                
                if missing_fields:
                    logger.warning(f"表 {table_name} 缺少字段: {missing_fields}")
                
                if valid_fields:
                    df = df.select(*valid_fields)
                    logger.info(f"字段裁剪为: {valid_fields}")
            
            # 缓存热点表
            if table_name in ['user_basic_info', 'user_asset_summary', 'user_activity_summary']:
                df = df.cache()
                logger.info(f"表 {table_name} 已缓存")
            
            row_count = df.count()
            logger.info(f"成功读取表 {table_name}，共 {row_count} 行")
            
            return df
            
        except Exception as e:
            logger.error(f"读取表 {table_name} 失败: {str(e)}")
            raise
    
    def read_incremental_data(self, table_name: str, date_field: str, 
                            days_back: int = 1, required_fields: Optional[str] = None) -> DataFrame:
        """
        读取增量数据
        
        Args:
            table_name: 表名
            date_field: 日期字段名
            days_back: 向前追溯天数
            required_fields: 需要的字段
        """
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # 构建分区过滤条件
        partition_filter = f"{date_field} >= '{start_date.strftime('%Y-%m-%d')}' AND {date_field} <= '{end_date.strftime('%Y-%m-%d')}'"
        
        logger.info(f"读取增量数据，时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        
        return self.read_table_data(table_name, required_fields, partition_filter)
    
    def read_user_basic_info(self, user_ids: Optional[List[str]] = None) -> DataFrame:
        """读取用户基础信息表"""
        df = self.read_table_data('user_basic_info')
        
        if user_ids:
            df = df.filter(F.col('user_id').isin(user_ids))
            logger.info(f"过滤指定用户，数量: {len(user_ids)}")
        
        return df
    
    def read_user_asset_summary(self, user_ids: Optional[List[str]] = None) -> DataFrame:
        """读取用户资产汇总表"""
        df = self.read_table_data('user_asset_summary')
        
        if user_ids:
            df = df.filter(F.col('user_id').isin(user_ids))
        
        return df
    
    def read_user_activity_summary(self, user_ids: Optional[List[str]] = None) -> DataFrame:
        """读取用户活动汇总表"""
        df = self.read_table_data('user_activity_summary')
        
        if user_ids:
            df = df.filter(F.col('user_id').isin(user_ids))
        
        return df
    
    def optimize_read_performance(self, df: DataFrame, table_name: str) -> DataFrame:
        """优化读取性能"""
        # 重分区优化
        if table_name in ['user_basic_info', 'user_asset_summary']:
            # 小表减少分区数
            df = df.coalesce(10)
        elif df.rdd.getNumPartitions() > 200:
            # 大表控制分区数
            df = df.repartition(200)
        
        return df
    
    def validate_data_quality(self, df: DataFrame, table_name: str) -> bool:
        """验证数据质量"""
        try:
            # 检查是否有数据
            if df.count() == 0:
                logger.warning(f"表 {table_name} 没有数据")
                return False
            
            # 检查user_id字段
            if 'user_id' not in df.columns:
                logger.error(f"表 {table_name} 缺少user_id字段")
                return False
            
            # 检查user_id空值
            null_count = df.filter(F.col('user_id').isNull()).count()
            if null_count > 0:
                logger.warning(f"表 {table_name} 有 {null_count} 行user_id为空")
            
            # 检查数据重复
            total_count = df.count()
            distinct_count = df.select('user_id').distinct().count()
            if total_count != distinct_count:
                logger.warning(f"表 {table_name} 存在重复数据，总行数: {total_count}，去重后: {distinct_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"数据质量验证失败: {str(e)}")
            return False
    
    def get_table_schema(self, table_name: str) -> List[str]:
        """获取表结构"""
        try:
            s3_path = f"s3a://{self.s3_config.bucket}/warehouse/{table_name}/"
            df = self.spark.read.parquet(s3_path).limit(1)
            return df.columns
        except Exception as e:
            logger.error(f"获取表 {table_name} 结构失败: {str(e)}")
            return []
    
    def clear_cache(self):
        """清理缓存"""
        self.spark.catalog.clearCache()
        self._cached_tables.clear()
        logger.info("已清理所有缓存")