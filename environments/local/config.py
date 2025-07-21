"""
本地Docker环境配置
"""

import os
from src.batch.config.BaseConfig import BaseConfig, SparkConfig, S3Config, MySQLConfig


class LocalConfig(BaseConfig):
    """本地Docker环境配置"""
    
    @classmethod
    def create(cls) -> 'LocalConfig':
        """创建本地环境配置"""
        
        # Spark配置 - 本地模式
        jars_dir = os.path.join(os.path.dirname(__file__), "jars")
        all_jars = [
            os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1034.jar")
        ]
        # 只包含存在的JAR文件
        existing_jars = [jar for jar in all_jars if os.path.exists(jar)]
        
        spark = SparkConfig(
            appName="TagSystem-Local",
            master="local[*]",
            executorMemory="2g",  # 增加内存以防止ClassLoader问题
            driverMemory="1g",    # 增加driver内存
            shufflePartitions=10,  # 本地环境减少分区数
            jars=",".join(existing_jars) if existing_jars else ""
        )
        
        # S3配置 - MinIO模拟
        s3 = S3Config(
            bucket=os.getenv("S3_BUCKET", "test-data-lake"),
            accessKey=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            secretKey=os.getenv("S3_SECRET_KEY", "minioadmin"),
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
            batchSize=1000,  # 本地环境小批次
            maxRetries=2,
            enableCache=False,  # 本地关闭缓存便于调试
            logLevel="DEBUG"
        )