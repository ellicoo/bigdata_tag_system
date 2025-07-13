"""
AWS Glue生产环境配置
"""

import os
from src.config.base import BaseConfig, SparkConfig, S3Config, MySQLConfig


class GlueprodConfig(BaseConfig):
    """Glue生产环境配置"""
    
    @classmethod  
    def create(cls) -> 'GlueprodConfig':
        """创建Glue生产环境配置"""
        
        # Spark配置 - 生产环境高性能
        spark = SparkConfig(
            app_name="TagSystem-Prod",
            master="yarn",
            executor_memory="8g",
            driver_memory="4g", 
            shuffle_partitions=200
        )
        
        # S3配置 - 生产环境数据湖
        s3 = S3Config(
            bucket=os.getenv("PROD_S3_BUCKET", "tag-system-prod-data-lake"),
            region=os.getenv("AWS_REGION", "us-east-1")
        )
        
        # MySQL配置 - 生产环境RDS
        mysql = MySQLConfig(
            host=os.getenv("PROD_MYSQL_HOST", "tag-system-prod.cluster-xxx.us-east-1.rds.amazonaws.com"),
            port=int(os.getenv("PROD_MYSQL_PORT", "3306")),
            database=os.getenv("PROD_MYSQL_DATABASE", "tag_system"),
            username=os.getenv("PROD_MYSQL_USERNAME", "tag_user"),
            password=os.getenv("PROD_MYSQL_PASSWORD", "")
        )
        
        return cls(
            environment="glue-prod",
            spark=spark,
            s3=s3,
            mysql=mysql,
            batch_size=20000,  # 生产环境大批次
            max_retries=5,
            enable_cache=True,
            log_level="WARN"  # 生产环境减少日志
        )