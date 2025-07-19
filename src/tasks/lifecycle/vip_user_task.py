"""
VIP客户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..base_tag_task import BaseTagTask


class VIPUserTask(BaseTagTask):
    """VIP客户标签任务 - 标签ID: 5"""
    
    def get_required_fields(self) -> List[str]:
        """VIP客户需要的数据字段"""
        return ['user_id', 'user_level', 'kyc_status']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_basic_info',
            'secondary': None
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        VIP客户数据预处理
        - 过滤掉用户等级为空的用户
        """
        return raw_data.filter(
            col('user_level').isNotNull() & 
            col('kyc_status').isNotNull()
        )
    
    def define_rules(self) -> Dict[str, Any]:
        """
        VIP客户规则定义
        - 用户等级为 VIP2 或 VIP3
        - KYC状态已验证
        """
        return {
            "logic": "AND",
            "conditions": [
                {
                    "field": "user_level",
                    "operator": "in",
                    "value": ["VIP2", "VIP3"],
                    "type": "string"
                },
                {
                    "field": "kyc_status",
                    "operator": "=",
                    "value": "verified",
                    "type": "string"
                }
            ]
        }
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """VIP客户结果后处理"""
        # 可以添加VIP等级细分等业务逻辑
        return tagged_users