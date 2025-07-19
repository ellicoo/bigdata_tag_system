"""
标签任务抽象基类
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


class BaseTagTask(ABC):
    """标签任务基类 - 每个标签实现自己的任务逻辑"""
    
    def __init__(self, task_config: Dict[str, Any]):
        self.task_config = task_config
        self.tag_id = task_config['tag_id']
        self.tag_name = task_config['tag_name']
        self.tag_category = task_config['tag_category']
    
    @abstractmethod
    def get_required_fields(self) -> List[str]:
        """
        返回该标签需要的数据字段
        
        Returns:
            List[str]: 必需的字段列表
        """
        pass
    
    @abstractmethod
    def get_data_sources(self) -> Dict[str, str]:
        """
        返回该标签需要的数据源配置
        
        Returns:
            Dict[str, str]: 数据源映射 {source_name: source_path}
        """
        pass
    
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        数据预处理 - 每个标签可以有自己的数据处理逻辑
        默认实现：直接返回原数据
        
        Args:
            raw_data: 原始数据DataFrame
            
        Returns:
            DataFrame: 预处理后的数据
        """
        return raw_data
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """
        结果后处理 - 可选的业务逻辑
        默认实现：直接返回结果
        
        Args:
            tagged_users: 标签计算结果
            
        Returns:
            DataFrame: 后处理的结果
        """
        return tagged_users
    
    def validate_data(self, data: DataFrame) -> bool:
        """
        验证数据是否满足任务需求
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 数据是否有效
        """
        required_fields = self.get_required_fields()
        missing_fields = set(required_fields) - set(data.columns)
        
        if missing_fields:
            logger.warning(f"标签任务 {self.tag_name} 缺少必需字段: {missing_fields}")
            return False
        
        return True
    
    def get_task_metadata(self) -> Dict[str, Any]:
        """
        获取任务元数据
        
        Returns:
            Dict[str, Any]: 任务元数据
        """
        return {
            'tag_id': self.tag_id,
            'tag_name': self.tag_name,
            'tag_category': self.tag_category,
            'required_fields': self.get_required_fields(),
            'data_sources': self.get_data_sources(),
            'task_class': self.__class__.__name__
        }
    
    def __str__(self) -> str:
        return f"TagTask({self.tag_id}: {self.tag_name})"
    
    def __repr__(self) -> str:
        return self.__str__()