"""
AWS Glue开发环境配置
"""

import os
from src.common.config.base import BaseConfig, SparkConfig, S3Config, MySQLConfig


class GluedevConfig(BaseConfig):
    """Glue开发环境配置"""
    
    @classmethod
    def create(cls) -> 'GluedevConfig':
        """创建Glue开发环境配置"""
        
        # Spark配置 - Glue环境
        spark = SparkConfig(
            app_name="TagSystem-Dev",
            master="yarn",  # Glue使用YARN
            executor_memory="4g",
            driver_memory="2g",
            shuffle_partitions=100
        )
        
        # S3配置 - 开发环境数据湖
        s3 = S3Config(
            bucket=os.getenv("DEV_S3_BUCKET", "tag-system-dev-data-lake"),
            region=os.getenv("AWS_REGION", "us-east-1")
            # Glue环境使用IAM角色，不需要显式的access_key和secret_key
        )
        
        # MySQL配置 - 开发环境RDS
        mysql = MySQLConfig(
            host=os.getenv("DEV_MYSQL_HOST", "tag-system-dev.cluster-xxx.us-east-1.rds.amazonaws.com"),
            port=int(os.getenv("DEV_MYSQL_PORT", "3306")),
            database=os.getenv("DEV_MYSQL_DATABASE", "tag_system_dev"),
            username=os.getenv("DEV_MYSQL_USERNAME", "tag_user"),
            password=os.getenv("DEV_MYSQL_PASSWORD", "")  # 通过Secrets Manager获取
        )
        
        return cls(
            environment="glue-dev",
            spark=spark,
            s3=s3,
            mysql=mysql,
            batch_size=5000,
            max_retries=3,
            enable_cache=True,
            log_level="INFO"
        )