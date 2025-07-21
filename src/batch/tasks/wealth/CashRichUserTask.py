"""
现金充足用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.batch.tasks.base.BaseTagTask import BaseTagTask


class CashRichUserTask(BaseTagTask):
    """现金充足用户标签任务 - 标签ID: 6"""
    
    def getRequiredFields(self) -> List[str]:
        """现金充足用户需要的数据字段"""
        return ['user_id', 'cash_balance', 'total_asset_value']
    
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置
        任务自主指定完整的S3路径
        """
        return {
            'user_asset_summary': 's3a://tag-system-data/warehouse/user_asset_summary/'
        }
    
    def preprocessData(self, rawData: DataFrame) -> DataFrame:
        """
        现金充足用户数据预处理
        - 过滤掉现金余额为空或负数的用户
        """
        return rawData.filter(
            col('cash_balance').isNotNull() & 
            (col('cash_balance') >= 0)
        )
    
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """现金充足用户结果后处理"""
        # 可以添加现金比例分析等业务逻辑
        return tagged_users