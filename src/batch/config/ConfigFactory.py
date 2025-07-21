"""
配置工厂 - 创建不同环境的配置实例
提供配置创建的工厂方法和配置模板
"""

import os
import logging
from typing import Dict, Any, Optional

from .BaseConfig import BaseConfig, SparkConfig, S3Config, MySQLConfig

logger = logging.getLogger(__name__)


class ConfigFactory:
    """配置工厂类"""
    
    @staticmethod
    def createLocalConfig() -> BaseConfig:
        """
        创建本地环境配置
        
        Returns:
            BaseConfig: 本地环境配置实例
        """
        from .LocalConfig import LocalConfig
        return LocalConfig.create()
    
    @staticmethod
    def createGlueDevConfig() -> BaseConfig:
        """
        创建Glue开发环境配置
        
        Returns:
            BaseConfig: Glue开发环境配置实例
        """
        from .GlueDevConfig import GlueDevConfig
        return GlueDevConfig.create()
    
    @staticmethod
    def createGlueProdConfig() -> BaseConfig:
        """
        创建Glue生产环境配置
        
        Returns:
            BaseConfig: Glue生产环境配置实例
        """
        from .GlueProdConfig import GlueProdConfig
        return GlueProdConfig.create()
    
    @staticmethod
    def createConfigFromEnvironment(environment: str) -> BaseConfig:
        """
        根据环境名称创建配置
        
        Args:
            environment: 环境名称
            
        Returns:
            BaseConfig: 配置实例
        """
        factoryMethods = {
            'local': ConfigFactory.createLocalConfig,
            'glue-dev': ConfigFactory.createGlueDevConfig,
            'glue-prod': ConfigFactory.createGlueProdConfig
        }
        
        if environment not in factoryMethods:
            raise ValueError(f"不支持的环境: {environment}")
        
        return factoryMethods[environment]()
    
    @staticmethod
    def createCustomConfig(environment: str, 
                          sparkConfig: SparkConfig,
                          s3Config: S3Config,
                          mysqlConfig: MySQLConfig,
                          **kwargs) -> BaseConfig:
        """
        创建自定义配置
        
        Args:
            environment: 环境名称
            sparkConfig: Spark配置
            s3Config: S3配置
            mysqlConfig: MySQL配置
            **kwargs: 其他配置参数
            
        Returns:
            BaseConfig: 自定义配置实例
        """
        from .CustomConfig import CustomConfig
        
        return CustomConfig(
            environment=environment,
            spark=sparkConfig,
            s3=s3Config,
            mysql=mysqlConfig,
            batchSize=kwargs.get('batchSize', 10000),
            maxRetries=kwargs.get('maxRetries', 3),
            enableCache=kwargs.get('enableCache', True),
            logLevel=kwargs.get('logLevel', 'INFO')
        )
    
    @staticmethod
    def createConfigFromDict(configData: Dict[str, Any]) -> BaseConfig:
        """
        从字典创建配置
        
        Args:
            configData: 配置数据字典
            
        Returns:
            BaseConfig: 配置实例
        """
        try:
            # 解析Spark配置
            sparkData = configData.get('spark', {})
            sparkConfig = SparkConfig(
                appName=sparkData.get('appName', 'TagComputeSystem'),
                master=sparkData.get('master', 'local[*]'),
                executorMemory=sparkData.get('executorMemory', '2g'),
                driverMemory=sparkData.get('driverMemory', '1g'),
                maxResultSize=sparkData.get('maxResultSize', '1g'),
                shufflePartitions=sparkData.get('shufflePartitions', 200),
                jars=sparkData.get('jars', '')
            )
            
            # 解析S3配置
            s3Data = configData.get('s3', {})
            s3Config = S3Config(
                bucket=s3Data.get('bucket', ''),
                accessKey=s3Data.get('accessKey', ''),
                secretKey=s3Data.get('secretKey', ''),
                endpoint=s3Data.get('endpoint', ''),
                region=s3Data.get('region', 'us-east-1')
            )
            
            # 解析MySQL配置
            mysqlData = configData.get('mysql', {})
            mysqlConfig = MySQLConfig(
                host=mysqlData.get('host', 'localhost'),
                port=mysqlData.get('port', 3306),
                database=mysqlData.get('database', 'tag_system'),
                username=mysqlData.get('username', 'root'),
                password=mysqlData.get('password', '')
            )
            
            # 创建自定义配置
            return ConfigFactory.createCustomConfig(
                environment=configData.get('environment', 'custom'),
                sparkConfig=sparkConfig,
                s3Config=s3Config,
                mysqlConfig=mysqlConfig,
                batchSize=configData.get('batchSize', 10000),
                maxRetries=configData.get('maxRetries', 3),
                enableCache=configData.get('enableCache', True),
                logLevel=configData.get('logLevel', 'INFO')
            )
            
        except Exception as e:
            logger.error(f"❌ 从字典创建配置失败: {e}")
            raise ValueError(f"配置数据格式错误: {e}")
    
    @staticmethod
    def createConfigFromEnvVars(environment: str) -> BaseConfig:
        """
        从环境变量创建配置
        
        Args:
            environment: 环境名称
            
        Returns:
            BaseConfig: 配置实例
        """
        try:
            # Spark配置
            sparkConfig = SparkConfig(
                appName=os.getenv('SPARK_APP_NAME', f'TagSystem-{environment}'),
                master=os.getenv('SPARK_MASTER', 'local[*]'),
                executorMemory=os.getenv('SPARK_EXECUTOR_MEMORY', '2g'),
                driverMemory=os.getenv('SPARK_DRIVER_MEMORY', '1g'),
                shufflePartitions=int(os.getenv('SPARK_SHUFFLE_PARTITIONS', '200'))
            )
            
            # S3配置
            s3Config = S3Config(
                bucket=os.getenv('S3_BUCKET', ''),
                accessKey=os.getenv('S3_ACCESS_KEY', ''),
                secretKey=os.getenv('S3_SECRET_KEY', ''),
                endpoint=os.getenv('S3_ENDPOINT', ''),
                region=os.getenv('S3_REGION', 'us-east-1')
            )
            
            # MySQL配置
            mysqlConfig = MySQLConfig(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', '3306')),
                database=os.getenv('MYSQL_DATABASE', 'tag_system'),
                username=os.getenv('MYSQL_USERNAME', 'root'),
                password=os.getenv('MYSQL_PASSWORD', '')
            )
            
            return ConfigFactory.createCustomConfig(
                environment=environment,
                sparkConfig=sparkConfig,
                s3Config=s3Config,
                mysqlConfig=mysqlConfig,
                batchSize=int(os.getenv('BATCH_SIZE', '10000')),
                maxRetries=int(os.getenv('MAX_RETRIES', '3')),
                enableCache=os.getenv('ENABLE_CACHE', 'true').lower() == 'true',
                logLevel=os.getenv('LOG_LEVEL', 'INFO')
            )
            
        except Exception as e:
            logger.error(f"❌ 从环境变量创建配置失败: {e}")
            raise ValueError(f"环境变量配置错误: {e}")
    
    @staticmethod
    def getDefaultConfigTemplate(environment: str) -> Dict[str, Any]:
        """
        获取默认配置模板
        
        Args:
            environment: 环境名称
            
        Returns:
            Dict[str, Any]: 配置模板
        """
        templates = {
            'local': {
                'environment': 'local',
                'spark': {
                    'appName': 'TagSystem-Local',
                    'master': 'local[*]',
                    'executorMemory': '2g',
                    'driverMemory': '1g',
                    'shufflePartitions': 4
                },
                's3': {
                    'bucket': 'tag-system-local',
                    'endpoint': 'http://localhost:9000',
                    'accessKey': 'minioadmin',
                    'secretKey': 'minioadmin'
                },
                'mysql': {
                    'host': 'localhost',
                    'port': 3307,
                    'database': 'tag_system',
                    'username': 'root',
                    'password': 'password123'
                }
            },
            'glue-dev': {
                'environment': 'glue-dev',
                'spark': {
                    'appName': 'TagSystem-Dev',
                    'master': 'yarn',
                    'executorMemory': '4g',
                    'driverMemory': '2g',
                    'shufflePartitions': 200
                },
                's3': {
                    'bucket': 'tag-system-dev-data-lake',
                    'region': 'us-west-2'
                },
                'mysql': {
                    'host': 'tag-system-dev.cluster-xxx.rds.amazonaws.com',
                    'port': 3306,
                    'database': 'tag_system_dev'
                }
            },
            'glue-prod': {
                'environment': 'glue-prod',
                'spark': {
                    'appName': 'TagSystem-Prod',
                    'master': 'yarn',
                    'executorMemory': '8g',
                    'driverMemory': '4g',
                    'shufflePartitions': 400
                },
                's3': {
                    'bucket': 'tag-system-prod-data-lake',
                    'region': 'us-west-2'
                },
                'mysql': {
                    'host': 'tag-system-prod.cluster-xxx.rds.amazonaws.com',
                    'port': 3306,
                    'database': 'tag_system_prod'
                }
            }
        }
        
        return templates.get(environment, templates['local'])