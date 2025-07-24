#!/usr/bin/env python3
"""
Spark任务执行器 - 专门为Java API触发设计
提供标准化的Spark任务接口，支持AWS Glue调用
"""

import sys
import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.entry.tag_system_api import TagSystemAPI


class SparkTaskExecutor:
    """
    Spark任务执行器
    专门为Java API调用设计的统一接口
    """
    
    def __init__(self, environment: str = 'dolphinscheduler', log_level: str = 'INFO'):
        """
        初始化Spark任务执行器
        
        Args:
            environment: 执行环境
            log_level: 日志级别
        """
        self.environment = environment
        self.log_level = log_level
        self.logger = self._setup_logging()
        
        self.logger.info(f"🚀 初始化Spark任务执行器 - 环境: {environment}")
    
    def _setup_logging(self) -> logging.Logger:
        """设置日志"""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            force=True
        )
        return logging.getLogger(f"SparkTaskExecutor.{self.environment}")
    
    def execute_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行单个Spark任务
        
        Args:
            task_config: 任务配置
                {
                    'task_type': 'health'|'task-all'|'task-tags'|'task-users'|'list-tasks',
                    'tag_ids': [1, 2, 3],  # 可选
                    'user_ids': ['user1', 'user2'],  # 可选
                    'parameters': {}  # 额外参数
                }
                
        Returns:
            Dict[str, Any]: 执行结果
                {
                    'success': True|False,
                    'task_id': 'task_xxx',
                    'execution_time': '2024-01-01T10:00:00',
                    'environment': 'glue-dev',
                    'message': '执行结果描述',
                    'data': {}  # 可选的返回数据
                }
        """
        task_id = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        execution_time = datetime.now().isoformat()
        
        self.logger.info(f"📋 执行Spark任务: {task_id}")
        
        result = {
            'success': False,
            'task_id': task_id,
            'execution_time': execution_time,
            'environment': self.environment,
            'message': '',
            'data': {}
        }
        
        try:
            task_type = task_config.get('task_type', 'health')
            tag_ids = task_config.get('tag_ids', [])
            user_ids = task_config.get('user_ids', [])
            parameters = task_config.get('parameters', {})
            
            self.logger.info(f"🎯 任务类型: {task_type}")
            
            # 使用TagSystemAPI执行任务
            with TagSystemAPI(environment=self.environment) as api:
                if task_type == 'health':
                    success = api.health_check()
                    result['message'] = '健康检查' + ('通过' if success else '失败')
                    
                elif task_type == 'task-all':
                    success = api.run_task_all_users_all_tags()
                    result['message'] = '全量任务执行' + ('成功' if success else '失败')
                    
                elif task_type == 'task-tags':
                    if not tag_ids:
                        result['message'] = '标签ID列表不能为空'
                        return result
                    
                    success = api.run_task_specific_tags(tag_ids)
                    result['message'] = f'指定标签任务执行{"成功" if success else "失败"} - 标签数: {len(tag_ids)}'
                    result['data']['tag_ids'] = tag_ids
                    
                elif task_type == 'task-users':
                    if not tag_ids or not user_ids:
                        result['message'] = '用户ID和标签ID列表不能为空'
                        return result
                    
                    success = api.run_task_specific_users_tags(user_ids, tag_ids)
                    result['message'] = f'指定用户标签任务执行{"成功" if success else "失败"} - 用户数: {len(user_ids)}, 标签数: {len(tag_ids)}'
                    result['data']['user_ids'] = user_ids
                    result['data']['tag_ids'] = tag_ids
                    
                elif task_type == 'list-tasks':
                    success = api.list_available_tasks()
                    result['message'] = '任务列表获取' + ('成功' if success else '失败')
                
                else:
                    result['message'] = f'不支持的任务类型: {task_type}'
                    return result
            
            result['success'] = success
            
            status = "成功" if success else "失败"
            self.logger.info(f"✅ 任务 {task_id} 执行{status}")
            
        except Exception as e:
            result['message'] = f'任务执行异常: {str(e)}'
            self.logger.error(f"❌ 任务 {task_id} 执行异常: {e}")
            self.logger.error("📋 异常详情:", exc_info=True)
        
        return result
    
    def execute_batch_tasks(self, task_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量执行Spark任务
        
        Args:
            task_configs: 任务配置列表
            
        Returns:
            Dict[str, Any]: 批量执行结果
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"🎯 执行批量Spark任务: {batch_id} - 任务数: {len(task_configs)}")
        
        results = []
        success_count = 0
        
        for i, config in enumerate(task_configs):
            self.logger.info(f"📋 执行第 {i+1}/{len(task_configs)} 个任务")
            
            task_result = self.execute_task(config)
            results.append(task_result)
            
            if task_result['success']:
                success_count += 1
        
        batch_result = {
            'batch_id': batch_id,
            'execution_time': datetime.now().isoformat(),
            'environment': self.environment,
            'total_tasks': len(task_configs),
            'success_tasks': success_count,
            'failed_tasks': len(task_configs) - success_count,
            'success_rate': success_count / len(task_configs) if task_configs else 0,
            'results': results
        }
        
        self.logger.info(f"🎉 批量任务执行完成 - 成功率: {batch_result['success_rate']:.2%}")
        
        return batch_result


# Java API友好的静态方法接口
class JavaAPIInterface:
    """Java API友好的静态接口"""
    
    @staticmethod
    def execute_health_check(environment: str = 'dolphinscheduler') -> str:
        """Java调用 - 健康检查"""
        executor = SparkTaskExecutor(environment)
        result = executor.execute_task({'task_type': 'health'})
        return json.dumps(result, ensure_ascii=False)
    
    @staticmethod
    def execute_all_tasks(environment: str = 'dolphinscheduler') -> str:
        """Java调用 - 执行所有任务"""
        executor = SparkTaskExecutor(environment)
        result = executor.execute_task({'task_type': 'task-all'})
        return json.dumps(result, ensure_ascii=False)
    
    @staticmethod
    def execute_specific_tags(tag_ids: str, environment: str = 'dolphinscheduler') -> str:
        """
        Java调用 - 执行指定标签
        
        Args:
            tag_ids: 逗号分隔的标签ID，如 "1,2,3,4,5"
            environment: 环境
        """
        try:
            tag_list = [int(x.strip()) for x in tag_ids.split(',') if x.strip()]
            executor = SparkTaskExecutor(environment)
            result = executor.execute_task({
                'task_type': 'task-tags',
                'tag_ids': tag_list
            })
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            error_result = {
                'success': False,
                'message': f'参数解析错误: {e}',
                'execution_time': datetime.now().isoformat(),
                'environment': environment
            }
            return json.dumps(error_result, ensure_ascii=False)
    
    @staticmethod
    def execute_specific_users_tags(user_ids: str, tag_ids: str, environment: str = 'dolphinscheduler') -> str:
        """
        Java调用 - 执行指定用户指定标签
        
        Args:
            user_ids: 逗号分隔的用户ID，如 "user1,user2,user3"
            tag_ids: 逗号分隔的标签ID，如 "1,2,3"
            environment: 环境
        """
        try:
            user_list = [x.strip() for x in user_ids.split(',') if x.strip()]
            tag_list = [int(x.strip()) for x in tag_ids.split(',') if x.strip()]
            
            executor = SparkTaskExecutor(environment)
            result = executor.execute_task({
                'task_type': 'task-users',
                'user_ids': user_list,
                'tag_ids': tag_list
            })
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            error_result = {
                'success': False,
                'message': f'参数解析错误: {e}',
                'execution_time': datetime.now().isoformat(),
                'environment': environment
            }
            return json.dumps(error_result, ensure_ascii=False)
    
    @staticmethod
    def execute_batch_from_json(config_json: str, environment: str = 'dolphinscheduler') -> str:
        """
        Java调用 - 从JSON配置批量执行任务
        
        Args:
            config_json: JSON格式的任务配置
            environment: 环境
            
        Example JSON:
            [
                {"task_type": "task-tags", "tag_ids": [1, 2, 3]},
                {"task_type": "task-users", "user_ids": ["user1"], "tag_ids": [4, 5]}
            ]
        """
        try:
            task_configs = json.loads(config_json)
            executor = SparkTaskExecutor(environment)
            result = executor.execute_batch_tasks(task_configs)
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            error_result = {
                'success': False,
                'message': f'JSON解析错误: {e}',
                'execution_time': datetime.now().isoformat(),
                'environment': environment
            }
            return json.dumps(error_result, ensure_ascii=False)


if __name__ == "__main__":
    # 测试用例
    print("🧪 测试Spark任务执行器...")
    
    # 测试单个任务
    executor = SparkTaskExecutor('local', 'INFO')
    
    # 健康检查
    result = executor.execute_task({'task_type': 'health'})
    print(f"健康检查结果: {result['success']}")
    
    if result['success']:
        # 指定标签任务
        result = executor.execute_task({
            'task_type': 'task-tags',
            'tag_ids': [1, 3, 5]
        })
        print(f"指定标签任务结果: {result['success']}")
        
        # 测试Java API接口
        json_result = JavaAPIInterface.execute_specific_tags("1,2,3", "local")
        print(f"Java API接口测试: {json.loads(json_result)['success']}")
    
    print("✅ 测试完成")