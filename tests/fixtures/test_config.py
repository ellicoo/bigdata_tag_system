"""
测试专用配置
"""

from src.config.base import BaseConfig, SparkConfig, S3Config, MySQLConfig


class TestConfig(BaseConfig):
    """测试环境配置"""
    
    def __init__(self):
        super().__init__(
            environment="test",
            spark=SparkConfig(
                app_name="TagSystem-Test",
                master="local[2]",
                executor_memory="1g",
                driver_memory="1g"
            ),
            s3=S3Config(
                bucket="test-bucket",
                access_key="testkey",
                secret_key="testsecret",
                endpoint="http://localhost:9000"
            ),
            mysql=MySQLConfig(
                host="localhost",
                port=3307,
                database="test_tag_system",
                username="test_user",
                password="test_pass"
            )
        )
    
    @classmethod
    def create(cls) -> 'TestConfig':
        """创建测试配置实例"""
        return cls()


def get_test_spark_config():
    """获取测试Spark配置"""
    return {
        "spark.app.name": "TagSystem-Test",
        "spark.master": "local[2]",
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "false",  # 测试时禁用自适应查询执行
        "spark.sql.adaptive.coalescePartitions.enabled": "false"
    }


def get_test_mysql_tables():
    """获取测试MySQL表结构"""
    return {
        "tag_category": """
            CREATE TABLE IF NOT EXISTS tag_category (
                category_id INT PRIMARY KEY AUTO_INCREMENT,
                category_name VARCHAR(100) NOT NULL,
                description TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """,
        
        "tag_definition": """
            CREATE TABLE IF NOT EXISTS tag_definition (
                tag_id INT PRIMARY KEY AUTO_INCREMENT,
                tag_name VARCHAR(200) NOT NULL,
                tag_category VARCHAR(100) NOT NULL,
                description TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (tag_category),
                INDEX idx_active (is_active)
            )
        """,
        
        "tag_rules": """
            CREATE TABLE IF NOT EXISTS tag_rules (
                rule_id INT PRIMARY KEY AUTO_INCREMENT,
                tag_id INT NOT NULL,
                rule_conditions JSON NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (tag_id) REFERENCES tag_definition(tag_id),
                INDEX idx_tag_id (tag_id),
                INDEX idx_active (is_active)
            )
        """,
        
        "user_tags": """
            CREATE TABLE IF NOT EXISTS user_tags (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                user_id VARCHAR(100) NOT NULL,
                tag_id INT NOT NULL,
                tag_name VARCHAR(200) NOT NULL,
                tag_category VARCHAR(100) NOT NULL,
                tag_detail JSON,
                computed_date DATE NOT NULL,
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_tag_id (tag_id),
                INDEX idx_computed_date (computed_date),
                UNIQUE KEY uk_user_tag_date (user_id, tag_id, computed_date)
            )
        """
    }


def get_test_sample_rules():
    """获取测试用的示例规则数据"""
    return [
        {
            "tag_id": 1,
            "tag_name": "测试高净值用户",
            "tag_category": "资产等级",
            "rule_conditions": {
                "logic": "AND",
                "conditions": [
                    {
                        "field": "total_asset_value",
                        "operator": ">=",
                        "value": 100000,
                        "type": "number"
                    }
                ]
            }
        },
        {
            "tag_id": 2,
            "tag_name": "测试VIP客户",
            "tag_category": "客户等级",
            "rule_conditions": {
                "logic": "AND",
                "conditions": [
                    {
                        "field": "user_level",
                        "operator": "in",
                        "value": ["VIP2", "VIP3"],
                        "type": "string"
                    },
                    {
                        "field": "kyc_status",
                        "operator": "=",
                        "value": "verified",
                        "type": "string"
                    }
                ]
            }
        }
    ]