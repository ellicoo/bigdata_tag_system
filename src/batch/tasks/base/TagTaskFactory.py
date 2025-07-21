"""
标签任务工厂 - 重构为驼峰命名风格
使用工厂模式 + 注册表模式，根据标签ID创建对应的任务实例
"""

import logging
from typing import Dict, Type, Any, Optional
from pyspark.sql import SparkSession

from src.batch.tasks.base.BaseTagTask import BaseTagTask
from src.batch.config.BaseConfig import BaseConfig

logger = logging.getLogger(__name__)


class TagTaskFactory:
    """标签任务工厂（原TaskFactory功能）"""
    
    _taskRegistry: Dict[int, Type[BaseTagTask]] = {}
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 确保任务已注册
        self._ensureTasksRegistered()
    
    @classmethod
    def registerTask(cls, tagId: int, taskClass: Type[BaseTagTask]):
        """
        注册标签任务类
        
        Args:
            tagId: 标签ID
            taskClass: 任务类（必须继承BaseTagTask）
        """
        # 验证任务类是否继承自BaseTagTask
        if not issubclass(taskClass, BaseTagTask):
            raise ValueError(f"任务类 {taskClass.__name__} 必须继承自 BaseTagTask")
        
        cls._taskRegistry[tagId] = taskClass
        logger.info(f"📝 注册任务: 标签ID {tagId} -> {taskClass.__name__}")
    
    @classmethod
    def getRegisteredTasks(cls) -> Dict[int, Type[BaseTagTask]]:
        """
        获取已注册的任务
        
        Returns:
            Dict[int, Type[BaseTagTask]]: 标签ID到任务类的映射
        """
        return cls._taskRegistry.copy()
    
    @classmethod
    def isTaskRegistered(cls, tagId: int) -> bool:
        """
        检查标签ID是否已注册
        
        Args:
            tagId: 标签ID
            
        Returns:
            bool: 是否已注册
        """
        return tagId in cls._taskRegistry
    
    def createTask(self, tagId: int, taskConfig: Dict[str, Any], 
                  spark: SparkSession, systemConfig: BaseConfig) -> Optional[BaseTagTask]:
        """
        创建标签任务实例
        
        Args:
            tagId: 标签ID
            taskConfig: 任务配置
            spark: Spark会话
            systemConfig: 系统配置
            
        Returns:
            BaseTagTask: 任务实例，失败时返回None
        """
        try:
            if tagId not in self._taskRegistry:
                self.logger.error(f"❌ 标签ID {tagId} 未注册对应的任务类")
                return None
            
            taskClass = self._taskRegistry[tagId]
            
            # 创建任务实例
            task = taskClass(taskConfig, spark, systemConfig)
            
            self.logger.info(f"✅ 成功创建任务: {task.tagName} (ID: {tagId})")
            return task
            
        except Exception as e:
            self.logger.error(f"❌ 创建标签 {tagId} 任务失败: {str(e)}")
            return None
    
    def createMultipleTasks(self, taskConfigs: Dict[int, Dict[str, Any]], 
                           spark: SparkSession, systemConfig: BaseConfig) -> Dict[int, BaseTagTask]:
        """
        批量创建多个任务实例
        
        Args:
            taskConfigs: {标签ID: 任务配置} 的映射
            spark: Spark会话
            systemConfig: 系统配置
            
        Returns:
            Dict[int, BaseTagTask]: {标签ID: 任务实例} 的映射
        """
        try:
            self.logger.info(f"🔧 批量创建 {len(taskConfigs)} 个任务实例...")
            
            tasks = {}
            successCount = 0
            
            for tagId, taskConfig in taskConfigs.items():
                task = self.createTask(tagId, taskConfig, spark, systemConfig)
                if task:
                    tasks[tagId] = task
                    successCount += 1
            
            self.logger.info(f"✅ 批量创建完成: 成功 {successCount}/{len(taskConfigs)} 个任务")
            return tasks
            
        except Exception as e:
            self.logger.error(f"❌ 批量创建任务失败: {str(e)}")
            return {}
    
    def validateTaskClass(self, taskClass: Type[BaseTagTask]) -> bool:
        """
        验证任务类是否符合要求
        
        Args:
            taskClass: 要验证的任务类
            
        Returns:
            bool: 是否符合要求
        """
        try:
            # 检查是否继承自BaseTagTask
            if not issubclass(taskClass, BaseTagTask):
                self.logger.error(f"❌ 任务类 {taskClass.__name__} 未继承 BaseTagTask")
                return False
            
            # 检查是否实现了必要的抽象方法
            requiredMethods = ['getRequiredFields', 'getHiveTableConfig']
            for method in requiredMethods:
                if not hasattr(taskClass, method):
                    self.logger.error(f"❌ 任务类 {taskClass.__name__} 未实现方法 {method}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 验证任务类失败: {str(e)}")
            return False
    
    def getTaskInfo(self, tagId: int) -> Optional[Dict[str, Any]]:
        """
        获取任务信息
        
        Args:
            tagId: 标签ID
            
        Returns:
            Dict[str, Any]: 任务信息，失败时返回None
        """
        try:
            if tagId not in self._taskRegistry:
                return None
            
            taskClass = self._taskRegistry[tagId]
            
            return {
                'tag_id': tagId,
                'task_class': taskClass.__name__,
                'module': taskClass.__module__,
                'file_path': taskClass.__module__.replace('.', '/') + '.py'
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取任务信息失败: {str(e)}")
            return None
    
    def getAllTasksInfo(self) -> Dict[int, Dict[str, Any]]:
        """
        获取所有任务信息
        
        Returns:
            Dict[int, Dict[str, Any]]: {标签ID: 任务信息} 的映射
        """
        try:
            tasksInfo = {}
            
            for tagId in self._taskRegistry:
                taskInfo = self.getTaskInfo(tagId)
                if taskInfo:
                    tasksInfo[tagId] = taskInfo
            
            return tasksInfo
            
        except Exception as e:
            self.logger.error(f"❌ 获取所有任务信息失败: {str(e)}")
            return {}
    
    def _ensureTasksRegistered(self):
        """确保所有任务已注册"""
        try:
            # 如果注册表为空，则触发任务注册
            if not self._taskRegistry:
                self.logger.info("📋 任务注册表为空，开始注册所有任务...")
                from src.batch.tasks.base.TaskRegistry import registerAllTasks
                registerAllTasks()
                self.logger.info(f"✅ 任务注册完成，共注册 {len(self._taskRegistry)} 个任务")
            
        except Exception as e:
            self.logger.error(f"❌ 确保任务注册失败: {str(e)}")
    
    def clearRegistry(self):
        """清空任务注册表"""
        try:
            clearedCount = len(self._taskRegistry)
            self._taskRegistry.clear()
            self.logger.info(f"🧹 清空任务注册表，清理了 {clearedCount} 个任务")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 清空任务注册表失败: {str(e)}")
    
    def getRegistryStatistics(self) -> Dict[str, Any]:
        """
        获取注册表统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        try:
            # 按任务类别统计
            categoryStats = {}
            totalTasks = len(self._taskRegistry)
            
            for tagId, taskClass in self._taskRegistry.items():
                # 从模块路径推断类别
                modulePath = taskClass.__module__
                if 'wealth' in modulePath:
                    category = 'wealth'
                elif 'behavior' in modulePath:
                    category = 'behavior'
                elif 'demographic' in modulePath:
                    category = 'demographic'
                elif 'lifecycle' in modulePath:
                    category = 'lifecycle'
                elif 'risk' in modulePath:
                    category = 'risk'
                else:
                    category = 'other'
                
                categoryStats[category] = categoryStats.get(category, 0) + 1
            
            return {
                'total_tasks': totalTasks,
                'category_breakdown': categoryStats,
                'registered_tag_ids': list(self._taskRegistry.keys())
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取注册表统计失败: {str(e)}")
            return {}