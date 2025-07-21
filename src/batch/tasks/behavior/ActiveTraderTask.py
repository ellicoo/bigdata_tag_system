"""
活跃交易者标签任务 - 使用新的BaseTagTask抽象
展示如何子类指定S3 Hive表地址和字段
"""

from typing import Dict, List
from pyspark.sql import DataFrame
import logging

from src.batch.tasks.base.BaseTagTask import BaseTagTask

logger = logging.getLogger(__name__)


class ActiveTraderTask(BaseTagTask):
    """
    活跃交易者标签任务
    
    业务规则：
    - 30天交易次数 > 15次
    
    标签ID：2
    标签类别：行为类
    """
    
    def getRequiredFields(self) -> List[str]:
        """
        返回该标签需要的数据字段
        
        Returns:
            List[str]: 活跃交易者任务需要的字段
        """
        return [
            "user_id",           # 用户ID
            "trade_count_30d"    # 30天交易次数
        ]
    
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置
        任务自主指定完整的S3路径
        
        Returns:
            Dict[str, str]: Hive表配置，指定需要的表和完整S3路径
        """
        return {
            # 🎯 任务自主指定完整S3路径
            'user_activity_summary': 's3a://tag-system-data/warehouse/user_activity_summary/'
        }
    
    def preprocessData(self, rawData: DataFrame) -> DataFrame:
        """
        活跃交易者任务的数据预处理
        
        Args:
            rawData: 原始用户活动数据
            
        Returns:
            DataFrame: 预处理后的数据
        """
        # 过滤掉空值和无效数据
        cleanedData = rawData.filter(
            (rawData.trade_count_30d.isNotNull()) & 
            (rawData.trade_count_30d >= 0)
        )
        
        logger.info(f"活跃交易者任务数据预处理: 原始{rawData.count()}条 -> 清洗后{cleanedData.count()}条")
        return cleanedData
    
    def postProcessResult(self, taggedUsers: DataFrame) -> DataFrame:
        """
        活跃交易者任务的结果后处理
        
        Args:
            taggedUsers: 标签计算结果
            
        Returns:
            DataFrame: 后处理的结果
        """
        # 可以在这里添加特定的业务逻辑，比如按交易次数排序
        return taggedUsers.orderBy("user_id")
    
    def validateData(self, data: DataFrame) -> bool:
        """
        验证活跃交易者任务的数据
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 数据是否有效
        """
        # 调用父类验证
        if not super().validateData(data):
            return False
        
        # 活跃交易者任务特定的验证
        if data.count() == 0:
            logger.warning("活跃交易者任务: 没有用户活动数据")
            return False
        
        # 检查数据质量
        nullTradeCount = data.filter(data.trade_count_30d.isNull()).count()
        if nullTradeCount > 0:
            logger.warning(f"活跃交易者任务: 发现{nullTradeCount}条交易次数为空的记录")
        
        return True