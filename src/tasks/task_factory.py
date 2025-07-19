"""
标签任务工厂 - 管理所有标签任务的创建和注册
"""

from typing import Dict, Any, Type
import logging

from .base_tag_task import BaseTagTask

logger = logging.getLogger(__name__)


class TagTaskFactory:
    """标签任务工厂 - 管理所有标签任务"""
    
    _task_registry: Dict[int, Type[BaseTagTask]] = {}
    
    @classmethod
    def register_task(cls, tag_id: int, task_class: Type[BaseTagTask]):
        """
        注册标签任务类
        
        Args:
            tag_id: 标签ID
            task_class: 任务类
        """
        if not issubclass(task_class, BaseTagTask):
            raise ValueError(f"任务类 {task_class} 必须继承自 BaseTagTask")
        
        cls._task_registry[tag_id] = task_class
        logger.info(f"✅ 注册标签任务: {tag_id} -> {task_class.__name__}")
    
    @classmethod
    def create_task(cls, tag_id: int, task_config: Dict[str, Any]) -> BaseTagTask:
        """
        根据标签ID创建对应的任务实例
        
        Args:
            tag_id: 标签ID
            task_config: 任务配置
            
        Returns:
            BaseTagTask: 标签任务实例
        """
        if tag_id not in cls._task_registry:
            raise ValueError(f"未找到标签ID {tag_id} 对应的任务类。请先注册任务。")
        
        task_class = cls._task_registry[tag_id]
        return task_class(task_config)
    
    @classmethod
    def get_all_available_tasks(cls) -> Dict[int, str]:
        """
        获取所有可用的标签任务
        
        Returns:
            Dict[int, str]: {tag_id: task_class_name}
        """
        return {tag_id: task_class.__name__ for tag_id, task_class in cls._task_registry.items()}
    
    @classmethod
    def is_task_registered(cls, tag_id: int) -> bool:
        """
        检查标签任务是否已注册
        
        Args:
            tag_id: 标签ID
            
        Returns:
            bool: 是否已注册
        """
        return tag_id in cls._task_registry
    
    @classmethod
    def get_task_class(cls, tag_id: int) -> Type[BaseTagTask]:
        """
        获取标签任务类
        
        Args:
            tag_id: 标签ID
            
        Returns:
            Type[BaseTagTask]: 任务类
        """
        if tag_id not in cls._task_registry:
            raise ValueError(f"未找到标签ID {tag_id} 对应的任务类")
        
        return cls._task_registry[tag_id]
    
    @classmethod
    def clear_registry(cls):
        """清空任务注册表（主要用于测试）"""
        cls._task_registry.clear()
        logger.info("🧹 清空任务注册表")
    
    @classmethod
    def get_registry_info(cls) -> Dict[str, Any]:
        """
        获取注册表信息
        
        Returns:
            Dict[str, Any]: 注册表统计信息
        """
        return {
            'total_tasks': len(cls._task_registry),
            'registered_tag_ids': list(cls._task_registry.keys()),
            'task_classes': [task_class.__name__ for task_class in cls._task_registry.values()]
        }