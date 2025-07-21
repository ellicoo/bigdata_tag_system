"""
新注册用户标签任务
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.batch.tasks.base.BaseTagTask import BaseTagTask


class NewUserTask(BaseTagTask):
    """新注册用户标签任务 - 标签ID: 4"""
    
    def getRequiredFields(self) -> List[str]:
        """新注册用户需要的数据字段"""
        return ['user_id', 'registration_date']
    
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置
        任务自主指定完整的S3路径
        """
        return {
            'user_basic_info': 's3a://tag-system-data/warehouse/user_basic_info/'
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        新注册用户数据预处理
        - 过滤掉注册日期为空的用户
        """
        return rawData.filter(
            col('registration_date').isNotNull()
        )
    
    def define_rules(self) -> Dict[str, Any]:
        """
        新注册用户规则定义
        - 注册时间在最近30天内
        """
        return {
            "logic": "AND",
            "conditions": [
                {
                    "field": "registration_date",
                    "operator": "recent_days",
                    "value": 30,
                    "type": "date"
                }
            ]
        }
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """新注册用户结果后处理"""
        # 可以添加新用户引导等业务逻辑
        return tagged_users