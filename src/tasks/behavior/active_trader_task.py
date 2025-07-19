"""
活跃交易者标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..base_tag_task import BaseTagTask


class ActiveTraderTask(BaseTagTask):
    """活跃交易者标签任务 - 标签ID: 2"""
    
    def get_required_fields(self) -> List[str]:
        """活跃交易者需要的数据字段"""
        return ['user_id', 'trade_count_30d', 'last_login_date']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_activity_summary',
            'secondary': None
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        活跃交易者数据预处理
        - 过滤掉交易频率为空的用户
        - 确保交易频率不为负数
        """
        return raw_data.filter(
            col('trade_count_30d').isNotNull() & 
            (col('trade_count_30d') >= 0)
        )
    
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """活跃交易者结果后处理"""
        # 可以添加交易偏好分析等业务逻辑
        return tagged_users