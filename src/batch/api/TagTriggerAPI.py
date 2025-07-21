"""
标签任务触发API接口 - 重构为驼峰命名风格
提供后端调用的REST API，支持异步任务触发
"""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from flask import Flask, request, jsonify
import logging

from .TaskManager import TaskManager
from src.batch.tasks.base.TagTaskFactory import TagTaskFactory

logger = logging.getLogger(__name__)


class TagTriggerAPI:
    """标签任务触发API"""
    
    def __init__(self, env: str = 'local'):
        self.env = env
        self.app = Flask(__name__)
        self.taskManager = TaskManager()
        self._setupRoutes()
        
    def _setupRoutes(self):
        """设置API路由"""
        
        @self.app.route('/health', methods=['GET'])
        def healthCheck():
            """健康检查接口"""
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'environment': self.env,
                'version': '2.0.0'
            })
        
        @self.app.route('/api/v1/tags/trigger', methods=['POST'])
        def triggerTagTasks():
            """
            触发标签任务接口
            
            请求体:
            {
                "tag_ids": [1, 2, 3],           # 必需: 标签ID列表
                "user_ids": ["user1", "user2"], # 可选: 指定用户列表
                "mode": "full",                  # 可选: 执行模式 (full/incremental)
                "priority": "normal"             # 可选: 任务优先级 (low/normal/high)
            }
            """
            try:
                data = request.get_json()
                
                # 验证请求参数
                validationResult = self._validateTriggerRequest(data)
                if not validationResult['valid']:
                    return jsonify({
                        'success': False,
                        'error': validationResult['error'],
                        'message': validationResult['message']
                    }), 400
                
                tagIds = data['tag_ids']
                userIds = data.get('user_ids', None)
                mode = data.get('mode', 'full')
                priority = data.get('priority', 'normal')
                
                # 验证标签ID对应的任务类是否存在
                availableTasks = self._getAvailableTasks()
                invalidTagIds = [tagId for tagId in tagIds if tagId not in availableTasks]
                if invalidTagIds:
                    return jsonify({
                        'success': False,
                        'error': f'Invalid tag_ids: {invalidTagIds}',
                        'message': f'无效的标签ID: {invalidTagIds}',
                        'available_tag_ids': list(availableTasks.keys())
                    }), 400
                
                # 生成任务ID
                taskId = str(uuid.uuid4())
                
                # 异步提交任务
                submitResult = self.taskManager.submitTask(
                    taskId=taskId,
                    tagIds=tagIds,
                    userIds=userIds,
                    mode=mode,
                    priority=priority,
                    env=self.env
                )
                
                if not submitResult['success']:
                    return jsonify({
                        'success': False,
                        'error': submitResult['error'],
                        'message': '任务提交失败'
                    }), 500
                
                logger.info(f"收到标签任务触发请求: taskId={taskId}, tagIds={tagIds}, userIds={userIds}, mode={mode}")
                
                return jsonify({
                    'success': True,
                    'task_id': taskId,
                    'message': '标签任务已成功提交',
                    'data': {
                        'tag_ids': tagIds,
                        'user_ids': userIds,
                        'mode': mode,
                        'priority': priority,
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
        
        @self.app.route('/api/v1/tasks/<taskId>/status', methods=['GET'])
        def getTaskStatus(taskId: str):
            """查询任务状态接口"""
            try:
                status = self.taskManager.getTaskStatus(taskId)
                if status is None:
                    return jsonify({
                        'success': False,
                        'error': 'Task not found',
                        'message': '任务不存在'
                    }), 404
                
                return jsonify({
                    'success': True,
                    'task_id': taskId,
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
        def listTasks():
            """列出所有任务接口"""
            try:
                # 获取查询参数
                limit = request.args.get('limit', 50, type=int)
                status = request.args.get('status', None)
                
                tasks = self.taskManager.listTasks(limit=limit, statusFilter=status)
                
                return jsonify({
                    'success': True,
                    'total': len(tasks),
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
        def getAvailableTags():
            """获取可用标签接口"""
            try:
                availableTasks = self._getAvailableTasks()
                
                return jsonify({
                    'success': True,
                    'total': len(availableTasks),
                    'available_tags': [
                        {
                            'tag_id': tagId,
                            'task_class': taskInfo['taskClass'],
                            'description': taskInfo['description'],
                            'category': taskInfo['category']
                        }
                        for tagId, taskInfo in availableTasks.items()
                    ]
                })
                
            except Exception as e:
                logger.error(f"获取可用标签失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '获取可用标签失败'
                }), 500
        
        @self.app.route('/api/v1/tasks/<taskId>/cancel', methods=['POST'])
        def cancelTask(taskId: str):
            """取消任务接口"""
            try:
                cancelResult = self.taskManager.cancelTask(taskId)
                
                if not cancelResult['success']:
                    return jsonify({
                        'success': False,
                        'error': cancelResult['error'],
                        'message': '任务取消失败'
                    }), 400
                
                return jsonify({
                    'success': True,
                    'task_id': taskId,
                    'message': '任务已成功取消'
                })
                
            except Exception as e:
                logger.error(f"取消任务失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '取消任务失败'
                }), 500
        
        @self.app.route('/api/v1/system/stats', methods=['GET'])
        def getSystemStats():
            """获取系统统计信息接口"""
            try:
                stats = self.taskManager.getSystemStats()
                availableTasks = self._getAvailableTasks()
                
                systemStats = {
                    'task_stats': stats,
                    'available_tags_count': len(availableTasks),
                    'environment': self.env,
                    'server_time': datetime.now().isoformat()
                }
                
                return jsonify({
                    'success': True,
                    'stats': systemStats
                })
                
            except Exception as e:
                logger.error(f"获取系统统计失败: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '获取系统统计失败'
                }), 500
    
    def _validateTriggerRequest(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """验证触发请求参数"""
        if not data:
            return {
                'valid': False,
                'error': 'Request body is required',
                'message': '请求体不能为空'
            }
        
        if 'tag_ids' not in data:
            return {
                'valid': False,
                'error': 'tag_ids is required',
                'message': '标签ID列表是必需的'
            }
        
        tagIds = data['tag_ids']
        if not isinstance(tagIds, list) or not tagIds:
            return {
                'valid': False,
                'error': 'tag_ids must be a non-empty list',
                'message': '标签ID必须是非空列表'
            }
        
        # 验证tagIds都是整数
        if not all(isinstance(tagId, int) for tagId in tagIds):
            return {
                'valid': False,
                'error': 'All tag_ids must be integers',
                'message': '所有标签ID必须是整数'
            }
        
        # 验证mode参数
        mode = data.get('mode', 'full')
        validModes = ['full', 'incremental']
        if mode not in validModes:
            return {
                'valid': False,
                'error': f'Invalid mode: {mode}. Valid modes: {validModes}',
                'message': f'无效的模式: {mode}. 有效模式: {validModes}'
            }
        
        # 验证priority参数
        priority = data.get('priority', 'normal')
        validPriorities = ['low', 'normal', 'high']
        if priority not in validPriorities:
            return {
                'valid': False,
                'error': f'Invalid priority: {priority}. Valid priorities: {validPriorities}',
                'message': f'无效的优先级: {priority}. 有效优先级: {validPriorities}'
            }
        
        return {'valid': True}
    
    def _getAvailableTasks(self) -> Dict[int, Dict[str, str]]:
        """获取可用的任务类和对应的标签ID"""
        taskTagMapping = {
            1: {'taskClass': 'HighNetWorthUserTask', 'description': '高净值用户', 'category': '财富类'},
            2: {'taskClass': 'ActiveTraderTask', 'description': '活跃交易者', 'category': '行为类'},
            3: {'taskClass': 'LowRiskUserTask', 'description': '低风险用户', 'category': '风险类'},
            4: {'taskClass': 'NewUserTask', 'description': '新注册用户', 'category': '生命周期类'},
            5: {'taskClass': 'VIPUserTask', 'description': 'VIP客户', 'category': '生命周期类'},
            6: {'taskClass': 'CashRichUserTask', 'description': '现金充足用户', 'category': '财富类'},
            7: {'taskClass': 'YoungUserTask', 'description': '年轻用户', 'category': '人口统计类'},
            8: {'taskClass': 'RecentActiveUserTask', 'description': '最近活跃用户', 'category': '行为类'}
        }
        
        # 验证任务类是否存在
        availableTasks = {}
        try:
            registeredTasks = TagTaskFactory.getRegisteredTasks()
            
            for tagId, taskInfo in taskTagMapping.items():
                if tagId in registeredTasks:
                    availableTasks[tagId] = taskInfo
                else:
                    logger.warning(f"标签ID {tagId} 对应的任务类 {taskInfo['taskClass']} 未注册")
                    
        except Exception as e:
            logger.error(f"获取注册任务失败: {str(e)}")
            # 降级：返回所有预定义的任务
            availableTasks = taskTagMapping
        
        return availableTasks
    
    def run(self, host='0.0.0.0', port=5000, debug=False):
        """运行API服务器"""
        logger.info(f"启动标签任务触发API服务器: {host}:{port}")
        logger.info(f"环境: {self.env}")
        logger.info(f"可用标签数: {len(self._getAvailableTasks())}")
        
        try:
            self.app.run(host=host, port=port, debug=debug, threaded=True)
        except KeyboardInterrupt:
            logger.info("API服务器被用户中断")
        except Exception as e:
            logger.error(f"API服务器运行异常: {str(e)}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """关闭API服务器"""
        logger.info("正在关闭API服务器...")
        try:
            self.taskManager.shutdown()
            logger.info("API服务器已关闭")
        except Exception as e:
            logger.error(f"关闭API服务器异常: {str(e)}")
    
    def getApp(self):
        """获取Flask应用实例（用于测试）"""
        return self.app