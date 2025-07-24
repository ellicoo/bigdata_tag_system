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
class MySQLConfig:
    """MySQL配置 - 基于您提供的mysql_connect_confg.py"""
    host: str = 'cex-mysql-test.c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com'
    port: int = 3358
    database: str = 'biz_statistics'
    username: str = 'root'
    password: str = 'ayjUzzH8b7gcQYRh'
    
    @property
    def jdbc_url(self) -> str:
        """JDBC连接URL"""
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&connectionCollation=utf8mb4_unicode_ci"
    
    @property
    def connection_properties(self) -> Dict[str, str]:
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
    """基础配置类 - 简化版，移除S3Config，直接读取Hive表"""
    environment: str
    spark: SparkConfig
    mysql: MySQLConfig
    
    # 系统配置
    batch_size: int = 10000
    max_retries: int = 3
    enable_cache: bool = True
    log_level: str = "INFO"
    
    # Hive表配置（基于您的读表方式）
    hive_database: str = "tag_test"  # 测试数据库
    data_date: str = "2025-01-20"    # 数据日期
    
    @classmethod
    @abstractmethod
    def create(cls) -> 'BaseConfig':
        """创建配置实例"""
        pass
    
    def get_spark_config(self) -> Dict[str, Any]:
        """获取Spark配置 - 启用Hive支持"""
        config = self.spark.to_dict()
        # 启用Hive支持（基于您的HiveToKafka.py方式）
        config.update({
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "",  # 使用嵌入式metastore
        })
        return config
    
    def validate(self) -> bool:
        """验证配置完整性"""
        required_fields = [
            self.spark.app_name,
            self.mysql.host,
            self.mysql.username,
            self.mysql.password
        ]
        return all(field for field in required_fields)
    
    def get_hive_table_names(self) -> Dict[str, str]:
        """获取Hive表名配置"""
        return {
            "user_basic_info": f"{self.hive_database}.user_basic_info",
            "user_asset_summary": f"{self.hive_database}.user_asset_summary", 
            "user_activity_summary": f"{self.hive_database}.user_activity_summary"
        }