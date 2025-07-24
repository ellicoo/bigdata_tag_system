#!/usr/bin/env python3
"""
DolphinScheduler环境配置
专门为海豚调度器部署优化
"""

from src.config.base import BaseConfig, SparkConfig, MySQLConfig


class DolphinSchedulerConfig(BaseConfig):
    """海豚调度器环境配置"""
    
    def __init__(self):
        super().__init__(
            environment='dolphinscheduler',
            spark=SparkConfig(
                app_name="BigDataTagSystem-Dolphin",
                master="yarn",  # 海豚调度器通常使用yarn
                executor_memory="4g",
                driver_memory="2g",
                executor_cores=2,
                driver_cores=2,
                executor_instances=2
                # 不需要sql_warehouse_dir配置，集群自动处理Hive表读取
            ),
            mysql=MySQLConfig()  # 使用统一的MySQL配置
        )
        
        # 海豚调度器特有配置
        self.log_level = "INFO"
        self.enable_hive_support = True
        self.enable_dynamic_allocation = True
    
    @classmethod
    def create(cls):
        """创建配置实例"""
        return cls()