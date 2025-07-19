"""
标签任务触发API接口
提供后端调用的REST API，支持异步任务触发
"""

import asyncio
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from flask import Flask, request, jsonify
from threading import Thread
import logging

from ..scheduler.tag_scheduler import TagScheduler
from ..engine.task_parallel_engine import TaskParallelEngine
from ..tasks.task_factory import TaskFactory
from .task_manager import TaskManager

logger = logging.getLogger(__name__)

class TagTriggerAPI:
    """标签任务触发API"""
    
    def __init__(self, env: str = 'local'):
        self.env = env
        self.app = Flask(__name__)
        self.task_manager = TaskManager()
        self._setup_routes()
        
    def _setup_routes(self):
        """设置API路由"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """健康检查接口"""
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'environment': self.env
            })
        
        @self.app.route('/api/v1/tags/trigger', methods=['POST'])
        def trigger_tag_tasks():
            """
            触发标签任务接口
            
            请求体:
            {
                "tag_ids": [1, 2, 3],           # 必需: 标签ID列表
                "user_ids": ["user1", "user2"], # 可选: 指定用户列表
                "mode": "full"                   # 可选: 执行模式 (full/incremental)
            }
            """
            try:
                data = request.get_json()
                
                # 验证请求参数
                if not data or 'tag_ids' not in data:
                    return jsonify({
                        'success': False,
                        'error': 'tag_ids is required',
                        'message': '标签ID列表是必需的'
                    }), 400
                
                tag_ids = data['tag_ids']
                user_ids = data.get('user_ids', None)
                mode = data.get('mode', 'full')
                
                # 验证标签ID
                if not isinstance(tag_ids, list) or not tag_ids:
                    return jsonify({
                        'success': False,
                        'error': 'tag_ids must be a non-empty list',
                        'message': '标签ID必须是非空列表'
                    }), 400
                
                # 验证标签ID对应的任务类是否存在
                available_tasks = self._get_available_tasks()
                invalid_tag_ids = [tag_id for tag_id in tag_ids if tag_id not in available_tasks]
                if invalid_tag_ids:
                    return jsonify({
                        'success': False,
                        'error': f'Invalid tag_ids: {invalid_tag_ids}',
                        'message': f'无效的标签ID: {invalid_tag_ids}',
                        'available_tag_ids': list(available_tasks.keys())
                    }), 400
                
                # 生成任务ID
                task_id = str(uuid.uuid4())
                
                # 异步提交任务
                self.task_manager.submit_task(
                    task_id=task_id,
                    tag_ids=tag_ids,
                    user_ids=user_ids,
                    mode=mode,
                    env=self.env
                )
                
                logger.info(f"收到标签任务触发请求: task_id={task_id}, tag_ids={tag_ids}, user_ids={user_ids}, mode={mode}")
                
                return jsonify({
                    'success': True,
                    'task_id': task_id,
                    'message': '标签任务已成功提交',
                    'data': {
                        'tag_ids': tag_ids,
                        'user_ids': user_ids,
                        'mode': mode,
                        'environment': self.env,
                        'submitted_at': datetime.now().isoformat()
                    }
                }), 202  # 202 Accepted
                
            except Exception as e:
                logger.error(f"标签任务触发失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '标签任务触发失败'
                }), 500
        
        @self.app.route('/api/v1/tasks/<task_id>/status', methods=['GET'])
        def get_task_status(task_id: str):
            """
            查询任务状态接口
            """
            try:
                status = self.task_manager.get_task_status(task_id)
                if status is None:
                    return jsonify({
                        'success': False,
                        'error': 'Task not found',
                        'message': '任务不存在'
                    }), 404
                
                return jsonify({
                    'success': True,
                    'task_id': task_id,
                    'status': status
                })
                
            except Exception as e:
                logger.error(f"查询任务状态失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '查询任务状态失败'
                }), 500
        
        @self.app.route('/api/v1/tasks', methods=['GET'])
        def list_tasks():
            """
            列出所有任务接口
            """
            try:
                tasks = self.task_manager.list_tasks()
                return jsonify({
                    'success': True,
                    'tasks': tasks
                })
                
            except Exception as e:
                logger.error(f"列出任务失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '列出任务失败'
                }), 500
        
        @self.app.route('/api/v1/tags/available', methods=['GET'])
        def get_available_tags():
            """
            获取可用标签接口
            """
            try:
                available_tasks = self._get_available_tasks()
                return jsonify({
                    'success': True,
                    'available_tags': [
                        {
                            'tag_id': tag_id,
                            'task_class': task_info['task_class'],
                            'description': task_info['description']
                        }
                        for tag_id, task_info in available_tasks.items()
                    ]
                })
                
            except Exception as e:
                logger.error(f"获取可用标签失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '获取可用标签失败'
                }), 500
    
    def _get_available_tasks(self) -> Dict[int, Dict[str, str]]:
        """获取可用的任务类和对应的标签ID"""
        task_tag_mapping = {
            1: {'task_class': 'HighNetWorthUserTask', 'description': '高净值用户'},
            2: {'task_class': 'ActiveTraderTask', 'description': '活跃交易者'},
            3: {'task_class': 'LowRiskUserTask', 'description': '低风险用户'},
            4: {'task_class': 'NewUserTask', 'description': '新注册用户'},
            5: {'task_class': 'VIPUserTask', 'description': 'VIP客户'},
            6: {'task_class': 'CashRichUserTask', 'description': '现金充足用户'},
            7: {'task_class': 'YoungUserTask', 'description': '年轻用户'},
            8: {'task_class': 'RecentActiveUserTask', 'description': '最近活跃用户'}
        }
        
        # 验证任务类是否存在
        available_tasks = {}
        for tag_id, task_info in task_tag_mapping.items():
            try:
                # 尝试获取任务类
                task_classes = TaskFactory.get_all_tasks()
                if any(cls.__name__ == task_info['task_class'] for cls in task_classes):
                    available_tasks[tag_id] = task_info
            except Exception as e:
                logger.warning(f"任务类 {task_info['task_class']} 不可用: {str(e)}")
        
        return available_tasks
    
    def run(self, host='0.0.0.0', port=5000, debug=False):
        """运行API服务器"""
        logger.info(f"启动标签任务触发API服务器: {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)