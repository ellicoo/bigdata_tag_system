#!/usr/bin/env python3
"""
大数据标签系统 - 函数式API接口
支持多环境：local, dolphinscheduler
不依赖命令行参数，可直接调用函数
"""

import sys
import os
import logging
from typing import List, Optional, Dict, Any

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.manager import ConfigManager
from src.scheduler.tag_scheduler import TagScheduler


class TagSystemAPI:
    """标签系统函数式API接口"""
    
    def __init__(self, environment: str = 'local', max_workers: int = 4, log_level: str = 'INFO'):
        """
        初始化标签系统API
        
        Args:
            environment: 运行环境 ('local', 'dolphinscheduler')
            max_workers: 最大并行工作线程数
            log_level: 日志级别 ('DEBUG', 'INFO', 'WARN', 'ERROR')
        """
        self.environment = environment
        self.max_workers = max_workers
        self.log_level = log_level
        self.scheduler = None
        self.config = None
        self.logger = None
        
        # 自动初始化
        self._initialize()
    
    def _initialize(self):
        """初始化系统"""
        try:
            # 验证环境
            supported_environments = ['local', 'dolphinscheduler']
            if self.environment not in supported_environments:
                raise ValueError(f"不支持的环境: {self.environment}. 支持的环境: {supported_environments}")
            
            # 设置日志
            self._setup_logging()
            
            # 加载配置
            self.config = ConfigManager.load_config(self.environment)
            self.logger.info(f"🚀 初始化标签系统API - 环境: {self.environment}")
            
            # 创建调度器
            self.scheduler = TagScheduler(self.config, self.max_workers)
            
            # 初始化调度器
            self.scheduler.initialize()
            self.logger.info("✅ 标签系统API初始化完成")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 标签系统API初始化失败: {e}")
            raise
    
    def _setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            force=True
        )
        self.logger = logging.getLogger(f"TagSystemAPI.{self.environment}")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        try:
            if self.scheduler:
                self.scheduler.cleanup()
                self.logger.info("🧹 资源清理完成")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 资源清理异常: {e}")
    
    def health_check(self) -> bool:
        """
        执行系统健康检查
        
        Returns:
            bool: 健康检查是否通过
        """
        try:
            self.logger.info("🏥 执行系统健康检查...")
            result = self.scheduler.health_check()
            
            if result:
                self.logger.info("✅ 系统健康检查通过")
            else:
                self.logger.error("❌ 系统健康检查失败")
                
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 健康检查异常: {e}")
            return False
    
    def run_task_all_users_all_tags(self) -> bool:
        """
        执行所有任务（全量用户，全量标签）
        
        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info("🎯 执行所有任务（全量用户，全量标签）")
            return self.scheduler.scenario_task_all_users_all_tags()
            
        except Exception as e:
            self.logger.error(f"❌ 全量任务执行失败: {e}")
            return False
    
    def run_task_specific_tags(self, tag_ids: List[int]) -> bool:
        """
        执行指定标签任务
        
        Args:
            tag_ids: 标签ID列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            if not tag_ids:
                self.logger.error("❌ 标签ID列表不能为空")
                return False
                
            self.logger.info(f"🎯 执行指定标签任务: {tag_ids}")
            result = self.scheduler.scenario_task_all_users_specific_tags(tag_ids)
            return result is not None
            
        except Exception as e:
            self.logger.error(f"❌ 指定标签任务执行失败: {e}")
            return False
    
    def run_task_specific_users_specific_tags(self, user_ids: List[str], tag_ids: List[int]) -> bool:
        """
        执行指定用户指定标签任务
        
        Args:
            user_ids: 用户ID列表
            tag_ids: 标签ID列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            if not user_ids or not tag_ids:
                self.logger.error("❌ 用户ID和标签ID列表不能为空")
                return False
                
            self.logger.info(f"🎯 执行指定用户指定标签任务 - 用户数: {len(user_ids)}, 标签数: {len(tag_ids)}")
            result = self.scheduler.scenario_task_specific_users_specific_tags(user_ids, tag_ids)
            return result is not None
            
        except Exception as e:
            self.logger.error(f"❌ 指定用户标签任务执行失败: {e}")
            return False
    
    def list_available_tasks(self) -> List[Dict[str, Any]]:
        """
        列出可用任务
        
        Returns:
            List[Dict]: 可用任务列表
        """
        try:
            self.logger.info("📋 获取可用任务列表")
            tasks = self.scheduler.get_available_tasks()
            task_list = [{'tag_id': k, 'task_name': v} for k, v in tasks.items()]
            self.logger.info(f"📋 共找到 {len(task_list)} 个可用任务")
            return task_list
            
        except Exception as e:
            self.logger.error(f"❌ 获取任务列表失败: {e}")
            return []


# 便捷函数接口
def run_health_check(environment: str = 'dolphinscheduler', log_level: str = 'INFO') -> bool:
    """执行健康检查"""
    with TagSystemAPI(environment, log_level=log_level) as api:
        return api.health_check()


def run_all_tasks(environment: str = 'dolphinscheduler', log_level: str = 'INFO') -> bool:
    """执行所有任务"""
    with TagSystemAPI(environment, log_level=log_level) as api:
        if api.health_check():
            return api.run_task_all_users_all_tags()
    return False


def run_specific_tags(tag_ids: List[int], environment: str = 'dolphinscheduler', log_level: str = 'INFO') -> bool:
    """执行指定标签"""
    with TagSystemAPI(environment, log_level=log_level) as api:
        if api.health_check():
            return api.run_task_specific_tags(tag_ids)
    return False


def run_specific_users_tags(user_ids: List[str], tag_ids: List[int], environment: str = 'dolphinscheduler', log_level: str = 'INFO') -> bool:
    """执行指定用户指定标签"""
    with TagSystemAPI(environment, log_level=log_level) as api:
        if api.health_check():
            return api.run_task_specific_users_specific_tags(user_ids, tag_ids)
    return False


def get_available_tasks(environment: str = 'dolphinscheduler', log_level: str = 'INFO') -> List[Dict[str, Any]]:
    """获取可用任务列表"""
    with TagSystemAPI(environment, log_level=log_level) as api:
        return api.list_available_tasks()


if __name__ == "__main__":
    # 测试用例
    print("🧪 测试标签系统API...")
    
    # 测试健康检查
    if run_health_check('local'):
        print("✅ 健康检查通过")
        
        # 测试获取任务列表
        tasks = get_available_tasks('local')
        print(f"📋 可用任务数: {len(tasks)}")
        
        # 测试指定标签
        success = run_specific_tags([1, 3, 5], 'local')
        print(f"🎯 指定标签测试: {'成功' if success else '失败'}")
    else:
        print("❌ 健康检查失败")