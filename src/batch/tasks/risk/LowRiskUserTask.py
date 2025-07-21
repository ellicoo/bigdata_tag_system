"""
低风险用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.batch.tasks.base.BaseTagTask import BaseTagTask


class LowRiskUserTask(BaseTagTask):
    """低风险用户标签任务 - 标签ID: 3"""
    
    def getRequiredFields(self) -> List[str]:
        """低风险用户需要的数据字段"""
        return ['user_id', 'risk_score']
    
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置
        任务自主指定完整的S3路径
        """
        return {
            'user_basic_info': 's3a://tag-system-data/warehouse/user_basic_info/'
        }
    
    def preprocessData(self, rawData: DataFrame) -> DataFrame:
        """
        低风险用户数据预处理
        - 过滤掉风险评分为空的用户
        - 确保风险评分在合理范围内
        """
        return rawData.filter(
            col('risk_score').isNotNull() & 
            (col('risk_score') >= 0) & 
            (col('risk_score') <= 100)
        )
    
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """低风险用户结果后处理"""
        # 可以添加风险等级细分等业务逻辑
        return tagged_users