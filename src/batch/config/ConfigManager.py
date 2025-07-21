"""
配置管理器 - 重构为驼峰命名风格
负责加载和管理不同环境的配置
"""

import os
import importlib
import logging
from typing import Optional, Dict, Any

from .BaseConfig import BaseConfig

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器"""
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[BaseConfig] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def loadConfig(cls, environment: str) -> BaseConfig:
        """
        加载指定环境的配置
        
        Args:
            environment: 环境名称 (local, glue-dev, glue-prod)
            
        Returns:
            BaseConfig: 配置实例
        """
        try:
            logger.info(f"🔧 加载环境配置: {environment}")
            
            # 动态导入环境配置模块
            configModule = importlib.import_module(f'environments.{environment}.config')
            
            # 构建配置类名 (local -> LocalConfig, glue-dev -> GluedevConfig)
            configClassName = f'{environment.title().replace("-", "")}Config'
            configClass = getattr(configModule, configClassName)
            
            # 创建配置实例
            config = configClass.create()
            
            # 验证配置
            if not config.validate():
                raise ValueError(f"配置验证失败: {environment}")
            
            cls._config = config
            logger.info(f"✅ 环境配置加载成功: {environment}")
            return config
            
        except ImportError as e:
            logger.error(f"❌ 无法加载环境配置 '{environment}': {e}")
            raise ImportError(f"无法加载环境配置 '{environment}': {e}")
        except AttributeError as e:
            logger.error(f"❌ 配置类不存在: {e}")
            raise AttributeError(f"配置类不存在: {e}")
        except Exception as e:
            logger.error(f"❌ 加载配置时发生异常: {e}")
            raise
    
    @classmethod
    def getConfig(cls) -> BaseConfig:
        """
        获取当前配置
        
        Returns:
            BaseConfig: 当前配置实例
        """
        if cls._config is None:
            # 尝试从环境变量获取环境名
            env = os.getenv('TAG_SYSTEM_ENV', 'local')
            logger.info(f"🔍 从环境变量获取环境: {env}")
            cls.loadConfig(env)
        return cls._config
    
    @classmethod
    def setConfig(cls, config: BaseConfig):
        """
        设置配置
        
        Args:
            config: 配置实例
        """
        cls._config = config
        logger.info(f"📝 配置已设置: {config.environment}")
    
    @classmethod
    def reset(cls):
        """重置配置"""
        cls._config = None
        logger.info("🔄 配置已重置")
    
    @classmethod
    def getCurrentEnvironment(cls) -> Optional[str]:
        """
        获取当前环境名称
        
        Returns:
            str: 环境名称，如果没有配置则返回None
        """
        if cls._config:
            return cls._config.environment
        return None
    
    @classmethod
    def validateCurrentConfig(cls) -> bool:
        """
        验证当前配置
        
        Returns:
            bool: 配置是否有效
        """
        try:
            if cls._config is None:
                logger.warning("⚠️ 没有加载任何配置")
                return False
            
            isValid = cls._config.validate()
            if isValid:
                logger.info("✅ 当前配置验证通过")
            else:
                logger.error("❌ 当前配置验证失败")
            
            return isValid
            
        except Exception as e:
            logger.error(f"❌ 配置验证异常: {e}")
            return False
    
    @classmethod
    def getConfigSummary(cls) -> Dict[str, Any]:
        """
        获取配置摘要信息
        
        Returns:
            Dict[str, Any]: 配置摘要
        """
        if cls._config is None:
            return {'status': 'no_config_loaded'}
        
        try:
            return {
                'status': 'loaded',
                'environment': cls._config.environment,
                'spark_master': cls._config.spark.master,
                's3_bucket': cls._config.s3.bucket,
                'mysql_host': cls._config.mysql.host,
                'batch_size': cls._config.batchSize,
                'cache_enabled': cls._config.enableCache,
                'log_level': cls._config.logLevel,
                'config_valid': cls._config.validate()
            }
            
        except Exception as e:
            logger.error(f"❌ 获取配置摘要失败: {e}")
            return {'status': 'error', 'error': str(e)}
    
    @classmethod
    def switchEnvironment(cls, newEnvironment: str) -> bool:
        """
        切换环境配置
        
        Args:
            newEnvironment: 新环境名称
            
        Returns:
            bool: 切换是否成功
        """
        try:
            oldEnvironment = cls.getCurrentEnvironment()
            logger.info(f"🔄 切换环境: {oldEnvironment} -> {newEnvironment}")
            
            cls.loadConfig(newEnvironment)
            
            logger.info(f"✅ 环境切换成功: {newEnvironment}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 环境切换失败: {e}")
            return False
    
    @classmethod
    def isEnvironmentSupported(cls, environment: str) -> bool:
        """
        检查环境是否支持
        
        Args:
            environment: 环境名称
            
        Returns:
            bool: 是否支持
        """
        supportedEnvironments = ['local', 'glue-dev', 'glue-prod']
        return environment in supportedEnvironments
    
    @classmethod
    def getSupportedEnvironments(cls) -> list:
        """
        获取支持的环境列表
        
        Returns:
            list: 支持的环境列表
        """
        return ['local', 'glue-dev', 'glue-prod']