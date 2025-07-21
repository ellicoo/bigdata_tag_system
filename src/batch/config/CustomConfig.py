"""
自定义配置类 - 用于ConfigFactory创建动态配置
支持运行时配置和测试配置
"""

from .BaseConfig import BaseConfig, SparkConfig, S3Config, MySQLConfig


class CustomConfig(BaseConfig):
    """
    自定义配置类
    支持动态创建配置实例，主要用于测试和运行时配置
    """
    
    def __init__(self, environment: str, spark: SparkConfig, s3: S3Config, 
                 mysql: MySQLConfig, **kwargs):
        """
        初始化自定义配置
        
        Args:
            environment: 环境名称
            spark: Spark配置
            s3: S3配置
            mysql: MySQL配置
            **kwargs: 其他配置参数
        """
        super().__init__(
            environment=environment,
            spark=spark,
            s3=s3,
            mysql=mysql,
            batchSize=kwargs.get('batchSize', 10000),
            maxRetries=kwargs.get('maxRetries', 3),
            enableCache=kwargs.get('enableCache', True),
            logLevel=kwargs.get('logLevel', 'INFO')
        )
    
    @classmethod
    def create(cls) -> 'CustomConfig':
        """
        创建默认的自定义配置实例
        
        Returns:
            CustomConfig: 自定义配置实例
        """
        # 默认配置，主要用于测试
        sparkConfig = SparkConfig(
            appName="TagSystem-Custom",
            master="local[*]",
            executorMemory="1g",
            driverMemory="1g",
            shufflePartitions=2
        )
        
        s3Config = S3Config(
            bucket="test-bucket",
            accessKey="test-key",
            secretKey="test-secret",
            endpoint="http://localhost:9000"
        )
        
        mysqlConfig = MySQLConfig(
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_password"
        )
        
        return cls(
            environment="custom",
            spark=sparkConfig,
            s3=s3Config,
            mysql=mysqlConfig
        )
    
    def __str__(self) -> str:
        return f"CustomConfig(env={self.environment})"
    
    def __repr__(self) -> str:
        return self.__str__()