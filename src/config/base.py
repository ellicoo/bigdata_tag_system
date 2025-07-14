"""
基础配置类定义
"""

import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod


@dataclass
class SparkConfig:
    """Spark配置"""
    app_name: str = "TagComputeSystem"
    master: str = "local[*]"
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    max_result_size: str = "1g"
    shuffle_partitions: int = 200
    jars: str = ""  # JAR文件路径，逗号分隔
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为Spark配置字典"""
        config = {
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.maxResultSize": self.max_result_size,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
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
    access_key: str = ""
    secret_key: str = ""
    endpoint: str = ""
    region: str = "us-east-1"
    
    @property
    def warehouse_path(self) -> str:
        """数据仓库路径"""
        return f"s3a://{self.bucket}/warehouse/"
    
    def to_spark_config(self) -> Dict[str, str]:
        """转换为Spark S3配置"""
        config = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        
        if self.endpoint:
            config["spark.hadoop.fs.s3a.endpoint"] = self.endpoint
            config["spark.hadoop.fs.s3a.path.style.access"] = "true"
        
        if self.access_key:
            config["spark.hadoop.fs.s3a.access.key"] = self.access_key
        
        if self.secret_key:
            config["spark.hadoop.fs.s3a.secret.key"] = self.secret_key
            
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
    def jdbc_url(self) -> str:
        """JDBC连接URL"""
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&connectionCollation=utf8mb4_unicode_ci"
    
    @property
    def connection_properties(self) -> Dict[str, str]:
        """连接属性"""
        return {
            "user": self.username,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver",
            "batchsize": "10000",
            "isolationLevel": "READ_COMMITTED",
            "characterEncoding": "utf8",
            "useUnicode": "true"
        }


@dataclass 
class BaseConfig(ABC):
    """基础配置类"""
    environment: str
    spark: SparkConfig
    s3: S3Config
    mysql: MySQLConfig
    
    # 系统配置
    batch_size: int = 10000
    max_retries: int = 3
    enable_cache: bool = True
    log_level: str = "INFO"
    
    @classmethod
    @abstractmethod
    def create(cls) -> 'BaseConfig':
        """创建配置实例"""
        pass
    
    def get_spark_config(self) -> Dict[str, Any]:
        """获取完整的Spark配置"""
        config = self.spark.to_dict()
        config.update(self.s3.to_spark_config())
        return config
    
    def validate(self) -> bool:
        """验证配置完整性"""
        required_fields = [
            self.spark.app_name,
            self.s3.bucket,
            self.mysql.host,
            self.mysql.username
        ]
        return all(field for field in required_fields)