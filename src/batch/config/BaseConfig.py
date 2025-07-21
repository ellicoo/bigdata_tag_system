"""
基础配置类定义 - 重构为驼峰命名风格
定义系统的核心配置结构
"""

import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod


@dataclass
class SparkConfig:
    """Spark配置"""
    appName: str = "TagComputeSystem"
    master: str = "local[*]"
    executorMemory: str = "2g"
    driverMemory: str = "1g"
    maxResultSize: str = "1g"
    shufflePartitions: int = 200
    jars: str = ""  # JAR文件路径，逗号分隔
    
    def toDict(self) -> Dict[str, Any]:
        """转换为Spark配置字典"""
        config = {
            "spark.app.name": self.appName,
            "spark.master": self.master,
            "spark.executor.memory": self.executorMemory,
            "spark.driver.memory": self.driverMemory,
            "spark.driver.maxResultSize": self.maxResultSize,
            "spark.sql.shuffle.partitions": str(self.shufflePartitions),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
        
        # 添加JAR配置
        if self.jars:
            config["spark.jars"] = self.jars
            
        return config


@dataclass
class S3Config:
    """S3配置"""
    bucket: str
    accessKey: str = ""
    secretKey: str = ""
    endpoint: str = ""
    region: str = "us-east-1"
    
    @property
    def warehousePath(self) -> str:
        """数据仓库路径"""
        return f"s3a://{self.bucket}/warehouse/"
    
    def toSparkConfig(self) -> Dict[str, str]:
        """转换为Spark S3配置"""
        config = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        
        if self.endpoint:
            config["spark.hadoop.fs.s3a.endpoint"] = self.endpoint
            config["spark.hadoop.fs.s3a.path.style.access"] = "true"
        
        if self.accessKey:
            config["spark.hadoop.fs.s3a.access.key"] = self.accessKey
        
        if self.secretKey:
            config["spark.hadoop.fs.s3a.secret.key"] = self.secretKey
            
        return config


@dataclass
class MySQLConfig:
    """MySQL配置"""
    host: str
    port: int = 3306
    database: str = "tag_system"
    username: str = "root"
    password: str = ""
    
    @property
    def jdbcUrl(self) -> str:
        """JDBC连接URL"""
        return (f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
                f"?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
                f"&useUnicode=true&connectionCollation=utf8mb4_unicode_ci")
    
    @property
    def connectionProperties(self) -> Dict[str, str]:
        """连接属性"""
        return {
            "user": self.username,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver",
            "batchsize": "10000",
            "isolationLevel": "READ_COMMITTED",
            "useUnicode": "true",
            "connectionCollation": "utf8mb4_unicode_ci"
        }


@dataclass 
class BaseConfig(ABC):
    """基础配置类"""
    environment: str
    spark: SparkConfig
    s3: S3Config
    mysql: MySQLConfig
    
    # 系统配置
    batchSize: int = 10000
    maxRetries: int = 3
    enableCache: bool = True
    logLevel: str = "INFO"
    
    @classmethod
    @abstractmethod
    def create(cls) -> 'BaseConfig':
        """创建配置实例"""
        pass
    
    def getSparkConfig(self) -> Dict[str, Any]:
        """获取完整的Spark配置"""
        config = self.spark.toDict()
        config.update(self.s3.toSparkConfig())
        return config
    
    def validate(self) -> bool:
        """验证配置完整性"""
        requiredFields = [
            self.spark.appName,
            self.s3.bucket,
            self.mysql.host,
            self.mysql.username
        ]
        return all(field for field in requiredFields)
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'environment': self.environment,
            'spark': {
                'app_name': self.spark.appName,
                'master': self.spark.master,
                'executor_memory': self.spark.executorMemory,
                'driver_memory': self.spark.driverMemory
            },
            's3': {
                'bucket': self.s3.bucket,
                'endpoint': self.s3.endpoint,
                'region': self.s3.region
            },
            'mysql': {
                'host': self.mysql.host,
                'port': self.mysql.port,
                'database': self.mysql.database,
                'username': self.mysql.username
            },
            'system': {
                'batch_size': self.batchSize,
                'max_retries': self.maxRetries,
                'enable_cache': self.enableCache,
                'log_level': self.logLevel
            }
        }
    
    def __str__(self) -> str:
        return f"BaseConfig(env={self.environment})"
    
    def __repr__(self) -> str:
        return self.__str__()