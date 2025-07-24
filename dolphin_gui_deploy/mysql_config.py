"""
海豚调度器MySQL配置
基于现有mysql_connect_confg.py
"""

class DolphinMySQLConfig:
    # MySQL 连接信息 - 基于现有配置
    MYSQL_HOST = 'cex-mysql-test.c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com'
    MYSQL_PORT = 3358
    MYSQL_USERNAME = 'root'
    MYSQL_PASSWORD = 'ayjUzzH8b7gcQYRh'
    MYSQL_DATABASE = 'biz_statistics'
    
    # 标签系统表前缀
    TABLE_PREFIX = 'tag_system_'
    
    @classmethod
    def get_connection_url(cls):
        """获取MySQL连接URL"""
        return f"jdbc:mysql://{cls.MYSQL_HOST}:{cls.MYSQL_PORT}/{cls.MYSQL_DATABASE}"
    
    @classmethod
    def get_connection_properties(cls):
        """获取连接属性"""
        return {
            "user": cls.MYSQL_USERNAME,
            "password": cls.MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver",
            "charset": "utf8mb4",
            "useUnicode": "true"
        }
