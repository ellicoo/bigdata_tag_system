"""
配置管理器
"""

import os
import importlib
from typing import Optional
from .base import BaseConfig


class ConfigManager:
    """配置管理器"""
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[BaseConfig] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def load_config(cls, environment: str) -> BaseConfig:
        """加载指定环境的配置"""
        try:
            # 动态导入环境配置模块
            config_module = importlib.import_module(f'environments.{environment}.config')
            config_class = getattr(config_module, f'{environment.title().replace("-", "")}Config')
            
            # 创建配置实例
            config = config_class.create()
            
            # 验证配置
            if not config.validate():
                raise ValueError(f"配置验证失败: {environment}")
            
            cls._config = config
            return config
            
        except ImportError as e:
            raise ImportError(f"无法加载环境配置 '{environment}': {e}")
        except AttributeError as e:
            raise AttributeError(f"配置类不存在: {e}")
    
    @classmethod
    def get_config(cls) -> BaseConfig:
        """获取当前配置"""
        if cls._config is None:
            # 尝试从环境变量获取环境名
            env = os.getenv('TAG_SYSTEM_ENV', 'local')
            cls.load_config(env)
        return cls._config
    
    @classmethod
    def set_config(cls, config: BaseConfig):
        """设置配置"""
        cls._config = config
    
    @classmethod
    def reset(cls):
        """重置配置"""
        cls._config = None