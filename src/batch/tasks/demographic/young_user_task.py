"""
年轻用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..base_tag_task import BaseTagTask


class YoungUserTask(BaseTagTask):
    """年轻用户标签任务 - 标签ID: 7"""
    
    def get_required_fields(self) -> List[str]:
        """年轻用户需要的数据字段"""
        return ['user_id', 'age']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_basic_info',
            'secondary': None
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        年轻用户数据预处理
        - 过滤掉年龄为空的用户
        - 确保年龄在合理范围内
        """
        return raw_data.filter(
            col('age').isNotNull() & 
            (col('age') >= 18) & 
            (col('age') <= 100)
        )
    
    def define_rules(self) -> Dict[str, Any]:
        """
        年轻用户规则定义
        - 年龄在 18-30 岁之间
        """
        return {
            "logic": "AND",
            "conditions": [
                {
                    "field": "age",
                    "operator": "in_range",
                    "value": [18, 30],
                    "type": "number"
                }
            ]
        }
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """年轻用户结果后处理"""
        # 可以添加年龄段细分等业务逻辑
        return tagged_users