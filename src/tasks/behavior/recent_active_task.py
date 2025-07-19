"""
最近活跃用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..base_tag_task import BaseTagTask


class RecentActiveUserTask(BaseTagTask):
    """最近活跃用户标签任务 - 标签ID: 8"""
    
    def get_required_fields(self) -> List[str]:
        """最近活跃用户需要的数据字段"""
        return ['user_id', 'last_login_date']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_activity_summary',
            'secondary': None
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        最近活跃用户数据预处理
        - 过滤掉最后登录日期为空的用户
        """
        return raw_data.filter(
            col('last_login_date').isNotNull()
        )
    
    def define_rules(self) -> Dict[str, Any]:
        """
        最近活跃用户规则定义
        - 最近7天内有登录
        """
        return {
            "logic": "AND",
            "conditions": [
                {
                    "field": "last_login_date",
                    "operator": "recent_days",
                    "value": 7,
                    "type": "date"
                }
            ]
        }
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """最近活跃用户结果后处理"""
        # 可以添加活跃度评分等业务逻辑
        return tagged_users