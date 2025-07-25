#!/usr/bin/env python3
"""
DolphinScheduler环境配置
专门为海豚调度器部署优化
"""

from src.config.base import BaseConfig, SparkConfig, MySQLConfig


class DolphinschedulerConfig(BaseConfig):
    """海豚调度器环境配置"""
    
    def __init__(self):
        super().__init__(
            environment='dolphinscheduler',
            spark=SparkConfig(
                app_name="BigDataTagSystem-Dolphin",
                master="yarn",  # 海豚调度器通常使用yarn
                executor_memory="4g",
                driver_memory="2g",
                max_result_size="2g",
                shuffle_partitions=400  # 增加分区数以适应大数据处理
                # 不需要额外配置，集群自动处理Hive表读取
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