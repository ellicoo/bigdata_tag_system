"""
异步任务管理器 - 重构为驼峰命名风格
负责管理标签任务的异步执行和状态跟踪
"""

import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, Future
import logging
from enum import Enum

from src.batch.bean.TaskExecutionContext import TaskExecutionContext, TaskStatus
from src.batch.engine.BatchOrchestrator import BatchOrchestrator
from src.batch.config.ConfigManager import ConfigManager

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """任务优先级枚举"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class TaskManager:
    """任务管理器 - 负责异步任务调度和状态管理"""
    
    def __init__(self, maxWorkers: int = 3):
        self.tasks: Dict[str, TaskExecutionContext] = {}
        self.executor = ThreadPoolExecutor(max_workers=maxWorkers)
        self.futures: Dict[str, Future] = {}  # 存储Future对象用于取消任务
        self._lock = threading.Lock()
        
        # 任务统计
        self.totalSubmitted = 0
        self.totalCompleted = 0
        self.totalFailed = 0
        self.totalCancelled = 0
        
        logger.info(f"任务管理器初始化完成，最大工作线程数: {maxWorkers}")
        
    def submitTask(self, taskId: str, tagIds: List[int], userIds: Optional[List[str]] = None, 
                  mode: str = 'full', priority: str = 'normal', env: str = 'local') -> Dict[str, Any]:
        """
        提交异步任务
        
        Args:
            taskId: 任务ID
            tagIds: 标签ID列表
            userIds: 用户ID列表（可选）
            mode: 执行模式
            priority: 任务优先级
            env: 环境
            
        Returns:
            Dict[str, Any]: 提交结果
        """
        try:
            with self._lock:
                # 检查任务ID是否已存在
                if taskId in self.tasks:
                    return {
                        'success': False,
                        'error': f'Task ID {taskId} already exists'
                    }
                
                # 创建任务执行上下文
                context = TaskExecutionContext.createForTask(
                    taskId=taskId,
                    tagId=tagIds[0] if tagIds else 0,  # 主要标签ID
                    tagName=f"Multi-Tag-{len(tagIds)}",
                    userFilter=userIds
                )
                
                # 设置任务元数据
                context.metadata.update({
                    'tag_ids': tagIds,
                    'mode': mode,
                    'priority': priority,
                    'environment': env,
                    'total_tags': len(tagIds)
                })
                
                self.tasks[taskId] = context
                self.totalSubmitted += 1
            
            # 异步执行任务
            future = self.executor.submit(self._executeTask, taskId, tagIds, userIds, mode, env)
            self.futures[taskId] = future
            
            logger.info(f"任务 {taskId} 已提交到线程池，标签: {tagIds}, 优先级: {priority}")
            
            return {
                'success': True,
                'task_id': taskId,
                'message': '任务提交成功'
            }
            
        except Exception as e:
            logger.error(f"提交任务失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
        
    def _executeTask(self, taskId: str, tagIds: List[int], userIds: Optional[List[str]], 
                    mode: str, env: str) -> None:
        """
        执行标签任务
        
        Args:
            taskId: 任务ID
            tagIds: 标签ID列表
            userIds: 用户ID列表
            mode: 执行模式
            env: 环境
        """
        context = self.tasks.get(taskId)
        if not context:
            logger.error(f"任务 {taskId} 的上下文不存在")
            return
        
        try:
            # 开始执行
            context.startExecution()
            
            logger.info(f"开始执行任务 {taskId}: tagIds={tagIds}, userIds={userIds}, mode={mode}")
            
            # 加载配置
            config = ConfigManager.loadConfig(env)
            
            # 创建编排器并执行任务
            with BatchOrchestrator(config) as orchestrator:
                # 根据参数选择执行模式
                if userIds and len(userIds) > 0:
                    # 指定用户指定标签
                    success = orchestrator.executeSpecificUsersWorkflow(
                        userIds=userIds,
                        tagIds=tagIds
                    )
                else:
                    # 全量用户指定标签
                    success = orchestrator.executeSpecificTagsWorkflow(
                        tagIds=tagIds
                    )
                
                # 计算结果统计
                if success:
                    resultStats = {
                        'success': True,
                        'message': f'成功处理 {len(tagIds)} 个标签',
                        'tag_count': len(tagIds),
                        'user_filter_applied': userIds is not None
                    }
                    
                    context.completeExecution(taggedUsers=len(userIds) if userIds else 0)
                    context.metadata['result'] = resultStats
                    
                    with self._lock:
                        self.totalCompleted += 1
                    
                    logger.info(f"任务 {taskId} 执行完成: {resultStats}")
                else:
                    raise Exception("任务执行返回失败状态")
            
        except Exception as e:
            errorMsg = str(e)
            logger.error(f"任务 {taskId} 执行失败: {errorMsg}")
            
            context.failExecution(errorMsg)
            
            with self._lock:
                self.totalFailed += 1
        
        finally:
            # 清理Future引用
            with self._lock:
                self.futures.pop(taskId, None)
    
    def getTaskStatus(self, taskId: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        with self._lock:
            context = self.tasks.get(taskId)
            if context:
                return context.toDict()
            return None
    
    def listTasks(self, limit: int = 50, statusFilter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        列出所有任务
        
        Args:
            limit: 返回任务数量限制
            statusFilter: 状态过滤器
            
        Returns:
            List[Dict[str, Any]]: 任务列表
        """
        with self._lock:
            tasks = list(self.tasks.values())
            
            # 状态过滤
            if statusFilter:
                tasks = [task for task in tasks if task.status.value == statusFilter]
            
            # 按提交时间降序排列
            tasks.sort(key=lambda x: x.startTime or datetime.min, reverse=True)
            
            # 转换为字典格式
            return [task.toDict() for task in tasks[:limit]]
    
    def cancelTask(self, taskId: str) -> Dict[str, Any]:
        """
        取消任务
        
        Args:
            taskId: 任务ID
            
        Returns:
            Dict[str, Any]: 取消结果
        """
        try:
            with self._lock:
                context = self.tasks.get(taskId)
                if not context:
                    return {
                        'success': False,
                        'error': 'Task not found'
                    }
                
                # 检查任务是否可以取消
                if context.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    return {
                        'success': False,
                        'error': f'Task is already {context.status.value}'
                    }
                
                # 尝试取消Future
                future = self.futures.get(taskId)
                if future and not future.done():
                    cancelled = future.cancel()
                    if cancelled:
                        context.cancelExecution()
                        self.totalCancelled += 1
                        logger.info(f"任务 {taskId} 已成功取消")
                        return {
                            'success': True,
                            'message': '任务已成功取消'
                        }
                    else:
                        return {
                            'success': False,
                            'error': 'Task cannot be cancelled (already running)'
                        }
                else:
                    context.cancelExecution()
                    self.totalCancelled += 1
                    return {
                        'success': True,
                        'message': '任务已标记为取消'
                    }
                    
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def getSystemStats(self) -> Dict[str, Any]:
        """获取系统统计信息"""
        with self._lock:
            # 按状态统计任务
            statusCounts = {}
            runningTasks = []
            
            for context in self.tasks.values():
                status = context.status.value
                statusCounts[status] = statusCounts.get(status, 0) + 1
                
                if context.isRunning():
                    runningTasks.append({
                        'task_id': context.taskId,
                        'tag_name': context.tagName,
                        'duration': context.getDuration()
                    })
            
            return {
                'total_submitted': self.totalSubmitted,
                'total_completed': self.totalCompleted,
                'total_failed': self.totalFailed,
                'total_cancelled': self.totalCancelled,
                'status_counts': statusCounts,
                'active_tasks': len(runningTasks),
                'running_tasks': runningTasks,
                'thread_pool_size': self.executor._max_workers,
                'timestamp': datetime.now().isoformat()
            }
    
    def cleanupCompletedTasks(self, maxAgeHours: int = 24) -> int:
        """
        清理已完成的旧任务
        
        Args:
            maxAgeHours: 最大保留时间（小时）
            
        Returns:
            int: 清理的任务数量
        """
        cutoffTime = datetime.now() - timedelta(hours=maxAgeHours)
        
        with self._lock:
            tasksToRemove = []
            
            for taskId, context in self.tasks.items():
                if context.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    if context.startTime and context.startTime < cutoffTime:
                        tasksToRemove.append(taskId)
            
            # 移除过期任务
            for taskId in tasksToRemove:
                del self.tasks[taskId]
                self.futures.pop(taskId, None)  # 清理Future引用
            
            logger.info(f"清理了 {len(tasksToRemove)} 个过期任务")
            return len(tasksToRemove)
    
    def getTasksByStatus(self, status: TaskStatus) -> List[TaskExecutionContext]:
        """
        根据状态获取任务列表
        
        Args:
            status: 任务状态
            
        Returns:
            List[TaskExecutionContext]: 任务列表
        """
        with self._lock:
            return [context for context in self.tasks.values() if context.status == status]
    
    def getRunningTasksCount(self) -> int:
        """获取正在运行的任务数量"""
        with self._lock:
            return sum(1 for context in self.tasks.values() if context.isRunning())
    
    def shutdown(self):
        """关闭任务管理器"""
        logger.info("正在关闭任务管理器...")
        
        try:
            # 等待所有正在运行的任务完成（最多等待30秒）
            runningCount = self.getRunningTasksCount()
            if runningCount > 0:
                logger.info(f"等待 {runningCount} 个运行中的任务完成...")
            
            self.executor.shutdown(wait=True, timeout=30)
            
            # 清理资源
            with self._lock:
                self.futures.clear()
            
            logger.info("任务管理器已关闭")
            
        except Exception as e:
            logger.error(f"关闭任务管理器异常: {str(e)}")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, excType, excVal, excTb):
        """上下文管理器退出"""
        self.shutdown()
        return False