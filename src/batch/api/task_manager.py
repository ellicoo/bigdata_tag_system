"""
异步任务管理器
负责管理标签任务的异步执行和状态跟踪
"""

import asyncio
import threading
from datetime import datetime
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor
import logging

from ..scheduler.tag_scheduler import TagScheduler

logger = logging.getLogger(__name__)

class TaskManager:
    """任务管理器 - 负责异步任务调度和状态管理"""
    
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=3)  # 限制并发任务数
        self._lock = threading.Lock()
        
    def submit_task(self, task_id: str, tag_ids: List[int], user_ids: Optional[List[str]] = None, 
                   mode: str = 'full', env: str = 'local') -> None:
        """
        提交异步任务
        
        Args:
            task_id: 任务ID
            tag_ids: 标签ID列表
            user_ids: 用户ID列表（可选）
            mode: 执行模式
            env: 环境
        """
        with self._lock:
            # 记录任务状态
            self.tasks[task_id] = {
                'task_id': task_id,
                'tag_ids': tag_ids,
                'user_ids': user_ids,
                'mode': mode,
                'environment': env,
                'status': 'submitted',
                'submitted_at': datetime.now().isoformat(),
                'started_at': None,
                'completed_at': None,
                'error': None,
                'result': None
            }
        
        # 异步执行任务
        future = self.executor.submit(self._execute_task, task_id, tag_ids, user_ids, mode, env)
        
        logger.info(f"任务 {task_id} 已提交到线程池")
        
    def _execute_task(self, task_id: str, tag_ids: List[int], user_ids: Optional[List[str]], 
                     mode: str, env: str) -> None:
        """
        执行标签任务
        
        Args:
            task_id: 任务ID
            tag_ids: 标签ID列表
            user_ids: 用户ID列表
            mode: 执行模式
            env: 环境
        """
        try:
            # 更新任务状态为运行中
            self._update_task_status(task_id, 'running', started_at=datetime.now().isoformat())
            
            logger.info(f"开始执行任务 {task_id}: tag_ids={tag_ids}, user_ids={user_ids}, mode={mode}")
            
            # 创建调度器并执行任务
            scheduler = TagScheduler(env=env)
            
            # 根据参数选择执行模式
            if user_ids and len(user_ids) > 0:
                # 指定用户指定标签
                result = scheduler.scenario_task_specific_users_specific_tags(
                    user_ids=user_ids,
                    tag_ids=tag_ids
                )
            else:
                # 全量用户指定标签
                result = scheduler.scenario_task_all_users_specific_tags(
                    tag_ids=tag_ids
                )
            
            # 计算结果统计
            result_stats = self._calculate_result_stats(result)
            
            # 更新任务状态为完成
            self._update_task_status(
                task_id, 
                'completed', 
                completed_at=datetime.now().isoformat(),
                result=result_stats
            )
            
            logger.info(f"任务 {task_id} 执行完成: {result_stats}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"任务 {task_id} 执行失败: {error_msg}")
            
            # 更新任务状态为失败
            self._update_task_status(
                task_id, 
                'failed', 
                completed_at=datetime.now().isoformat(),
                error=error_msg
            )
    
    def _calculate_result_stats(self, result) -> Dict[str, Any]:
        """计算任务结果统计"""
        if result is None:
            return {'total_users': 0, 'message': 'No results'}
        
        try:
            # 如果result是DataFrame，计算统计信息
            if hasattr(result, 'count'):
                total_users = result.count()
                return {
                    'total_users': total_users,
                    'message': f'Successfully processed {total_users} users'
                }
            else:
                return {'message': 'Task completed successfully'}
        except Exception as e:
            logger.warning(f"计算结果统计失败: {str(e)}")
            return {'message': 'Task completed but stats calculation failed'}
    
    def _update_task_status(self, task_id: str, status: str, **kwargs) -> None:
        """更新任务状态"""
        with self._lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = status
                self.tasks[task_id].update(kwargs)
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        with self._lock:
            return self.tasks.get(task_id)
    
    def list_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        """列出所有任务"""
        with self._lock:
            # 按提交时间降序排列
            sorted_tasks = sorted(
                self.tasks.values(),
                key=lambda x: x['submitted_at'],
                reverse=True
            )
            return sorted_tasks[:limit]
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """清理已完成的旧任务"""
        from datetime import datetime, timedelta
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        with self._lock:
            tasks_to_remove = []
            for task_id, task_info in self.tasks.items():
                if task_info['status'] in ['completed', 'failed']:
                    submitted_at = datetime.fromisoformat(task_info['submitted_at'])
                    if submitted_at < cutoff_time:
                        tasks_to_remove.append(task_id)
            
            # 移除过期任务
            for task_id in tasks_to_remove:
                del self.tasks[task_id]
            
            logger.info(f"清理了 {len(tasks_to_remove)} 个过期任务")
            return len(tasks_to_remove)
    
    def shutdown(self):
        """关闭任务管理器"""
        logger.info("正在关闭任务管理器...")
        self.executor.shutdown(wait=True)
        logger.info("任务管理器已关闭")