"""
本地Docker环境配置
"""

import os
from src.config.base import BaseConfig, SparkConfig, S3Config, MySQLConfig


class LocalConfig(BaseConfig):
    """本地Docker环境配置"""
    
    @classmethod
    def create(cls) -> 'LocalConfig':
        """创建本地环境配置"""
        
        # Spark配置 - 本地模式
        spark = SparkConfig(
            app_name="TagSystem-Local",
            master="local[*]",
            executor_memory="1g",
            driver_memory="512m",
            shuffle_partitions=10,  # 本地环境减少分区数
            jars=os.path.join(os.path.dirname(__file__), "jars", "mysql-connector-j-8.0.33.jar")
        )
        
        # S3配置 - MinIO模拟
        s3 = S3Config(
            bucket=os.getenv("S3_BUCKET", "test-data-lake"),
            access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
            endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
            region="us-east-1"
        )
        
        # MySQL配置 - Docker容器
        mysql = MySQLConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3307")),  # 避免与本地MySQL冲突
            database=os.getenv("MYSQL_DATABASE", "tag_system"),
            username=os.getenv("MYSQL_USERNAME", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root123")
        )
        
        return cls(
            environment="local",
            spark=spark,
            s3=s3,
            mysql=mysql,
            batch_size=1000,  # 本地环境小批次
            max_retries=2,
            enable_cache=False,  # 本地关闭缓存便于调试
            log_level="DEBUG"
        )