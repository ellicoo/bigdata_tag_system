"""
高净值用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..base_tag_task import BaseTagTask


class HighNetWorthUserTask(BaseTagTask):
    """高净值用户标签任务 - 标签ID: 1"""
    
    def get_required_fields(self) -> List[str]:
        """高净值用户需要的数据字段"""
        return ['user_id', 'total_asset_value', 'cash_balance']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_asset_summary',  # 主要数据源
            'secondary': None  # 不需要辅助数据源
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        高净值用户数据预处理
        - 过滤掉总资产为空的用户
        - 确保数据质量
        """
        return raw_data.filter(
            col('total_asset_value').isNotNull() & 
            (col('total_asset_value') >= 0)
        )
    
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """
        高净值用户结果后处理
        可以添加额外的业务逻辑，比如风险等级计算
        """
        # 这里可以添加高净值用户特有的后处理逻辑
        # 例如：根据资产规模细分高净值等级
        return tagged_users