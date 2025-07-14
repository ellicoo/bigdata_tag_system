"""
统一数据管理器 - 负责所有数据的读取、缓存和写入
解决重复数据库连接问题，实现资源复用
"""

import json
import logging
from typing import List, Dict, Any, Optional
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class UnifiedDataManager:
    """统一数据管理器 - 一次连接，全局复用"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        
        # 缓存所有DataFrame，直接提供给各组件使用
        self._rules_df = None
        self._tag_definitions_df = None
        self._existing_user_tags_df = None
        
        # 标记是否已初始化
        self._initialized = False
    
    def initialize(self):
        """一次性初始化所有需要的数据"""
        if self._initialized:
            logger.info("数据管理器已初始化，使用缓存数据")
            return
        
        logger.info("🔄 开始一次性加载所有基础数据...")
        
        try:
            # 1. 加载标签规则（联表查询，一次搞定）
            self._load_tag_rules()
            
            # 2. 加载标签定义（小表，持久化到内存+磁盘）
            self._load_tag_definitions()
            
            # 3. 加载现有用户标签（如果存在）
            self._load_existing_user_tags()
            
            self._initialized = True
            logger.info("✅ 统一数据管理器初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 数据管理器初始化失败: {str(e)}")
            raise
    
    def _load_tag_rules(self):
        """加载标签规则（一次性，带持久化）- 直接保持DataFrame格式"""
        logger.info("📖 加载标签规则...")
        
        query = """
        (SELECT 
            tr.rule_id,
            tr.tag_id,
            tr.rule_conditions,
            tr.is_active as rule_active,
            td.tag_name,
            td.tag_category,
            td.description as tag_description,
            td.is_active as tag_active
         FROM tag_rules tr 
         JOIN tag_definition td ON tr.tag_id = td.tag_id 
         WHERE tr.is_active = 1 AND td.is_active = 1) as active_rules
        """
        
        self._rules_df = self.spark.read.jdbc(
            url=self.mysql_config.jdbc_url,
            table=query,
            properties=self.mysql_config.connection_properties
        ).persist(StorageLevel.MEMORY_AND_DISK)  # 持久化到内存+磁盘
        
        # 触发持久化并获取统计
        rule_count = self._rules_df.count()
        logger.info(f"✅ 标签规则DataFrame加载完成，共 {rule_count} 条")
    
    def _load_tag_definitions(self):
        """加载标签定义（小表，直接持久化）"""
        logger.info("📖 加载标签定义...")
        
        self._tag_definitions_df = self.spark.read.jdbc(
            url=self.mysql_config.jdbc_url,
            table="tag_definition",
            properties=self.mysql_config.connection_properties
        ).persist(StorageLevel.MEMORY_AND_DISK)
        
        tag_def_count = self._tag_definitions_df.count()
        logger.info(f"✅ 标签定义DataFrame加载完成，共 {tag_def_count} 条")
    
    def _load_existing_user_tags(self):
        """加载现有用户标签（如果存在）- 优化超时处理"""
        try:
            logger.info("📖 加载现有用户标签...")
            
            # 优化连接属性，添加超时设置
            timeout_properties = {
                **self.mysql_config.connection_properties,
                "connectTimeout": "5000",      # 5秒连接超时
                "socketTimeout": "10000",      # 10秒socket超时
                "queryTimeout": "30"           # 30秒查询超时
            }
            
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=timeout_properties
            )
            
            # 使用limit(1)快速检查表是否有数据，避免全表扫描
            sample_check = existing_df.limit(1)
            sample_count = sample_check.count()
            
            if sample_count > 0:
                # 有数据才进行完整加载
                logger.info("检测到现有用户标签，开始完整加载...")
                
                # 转换JSON并持久化
                from pyspark.sql.functions import from_json, col
                from pyspark.sql.types import ArrayType, IntegerType
                
                self._existing_user_tags_df = existing_df.select(
                    "user_id",
                    from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                    "tag_details"
                ).persist(StorageLevel.MEMORY_AND_DISK)
                
                existing_count = self._existing_user_tags_df.count()
                logger.info(f"✅ 现有用户标签DataFrame加载完成，共 {existing_count} 条")
            else:
                logger.info("📝 user_tags表为空（首次运行）")
                self._existing_user_tags_df = None
                
        except Exception as e:
            logger.info(f"📝 现有用户标签读取失败（可能表不存在或连接超时）: {str(e)}")
            self._existing_user_tags_df = None
    
    # ==== 对外提供的数据访问接口 ====
    
    def get_active_rules_df(self) -> DataFrame:
        """获取活跃标签规则DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._rules_df
    
    def get_rules_by_category_df(self, category_name: str) -> DataFrame:
        """按分类获取标签规则DataFrame"""
        rules_df = self.get_active_rules_df()
        return rules_df.filter(rules_df.tag_category == category_name)
    
    def get_tag_definitions_df(self) -> DataFrame:
        """获取标签定义DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._tag_definitions_df
    
    def get_existing_user_tags_df(self) -> Optional[DataFrame]:
        """获取现有用户标签DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._existing_user_tags_df
    
    def get_rules_for_computation(self):
        """为标签计算提供规则数据 - 返回可迭代的规则信息"""
        rules_df = self.get_active_rules_df()
        # 只在需要时转换为Python对象（用于规则解析）
        return rules_df.collect()
    
    def write_user_tags(self, result_df: DataFrame, mode: str = "overwrite") -> bool:
        """统一的标签写入接口 - 带重试机制"""
        import time
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 开始写入用户标签，模式: {mode}，尝试: {attempt + 1}/{max_retries}")
                
                # 数据预处理
                from pyspark.sql.functions import to_json, col, when
                mysql_ready_df = result_df.select(
                    col("user_id"),
                    when(col("tag_ids").isNotNull(), to_json(col("tag_ids")))
                    .otherwise("[]").alias("tag_ids"),
                    col("tag_details"),
                    col("computed_date")
                )
                
                # 显示数据样例（仅第一次尝试时）
                if attempt == 0:
                    logger.info("写入数据样例:")
                    mysql_ready_df.show(3, truncate=False)
                
                # 单连接多批次写入参数优化
                write_properties = {
                    **self.mysql_config.connection_properties,
                    "batchsize": "5000",           # 单连接内每5000条一个批次提交
                    "numPartitions": "1",          # 强制单分区（配合coalesce确保单连接）
                    "connectTimeout": "60000",     # 60秒连接超时
                    "socketTimeout": "300000",     # 5分钟socket超时，给批量提交足够时间
                    "queryTimeout": "300",         # 5分钟查询超时
                    "autoReconnect": "true",       # 自动重连
                    "rewriteBatchedStatements": "true",  # 启用批量SQL重写优化
                    "useCompression": "true",      # 启用压缩减少网络传输
                    "cachePrepStmts": "true",      # 缓存预处理语句
                    "prepStmtCacheSize": "250",    # 预处理语句缓存大小
                    "prepStmtCacheSqlLimit": "2048" # 预处理语句SQL长度限制
                }
                
                total_count = mysql_ready_df.count()
                logger.info(f"准备写入 {total_count} 条用户标签数据")
                
                # 采用自定义批量写入策略：foreachPartition + JDBC批处理
                logger.info("采用 foreachPartition + JDBC批处理 策略")
                
                # 恢复多分区测试：使用DELETE支持并发
                target_rows_per_partition = 8000
                optimal_partitions = max(1, min(8, total_count // target_rows_per_partition))
                logger.info(f"多分区模式：{optimal_partitions}个分区，每分区约{total_count//optimal_partitions if optimal_partitions > 0 else total_count}条数据")
                
                repartitioned_df = mysql_ready_df.repartition(optimal_partitions)
                
                # 处理overwrite模式：删除当天数据
                if mode == "overwrite":
                    from datetime import date
                    today = date.today()
                    logger.info(f"overwrite模式：删除 {today} 的用户标签数据")
                    self._delete_user_tags_for_date(today)
                
                # 使用自定义批量写入函数
                self._write_with_custom_batch(repartitioned_df, "append")
                
                logger.info(f"✅ 用户标签写入成功，共 {result_df.count()} 条记录")
                return True
                
            except Exception as e:
                logger.error(f"❌ 第 {attempt + 1} 次写入失败: {str(e)}")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10  # 递增等待时间
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ 用户标签写入彻底失败，已重试 {max_retries} 次")
                    return False
        
        return False
    
    def _write_with_custom_batch(self, df: DataFrame, mode: str):
        """自定义批量写入：foreachPartition + JDBC批处理"""
        import pymysql
        from urllib.parse import urlparse, parse_qs
        
        # 提取配置参数避免闭包序列化问题
        host = self.mysql_config.host
        port = self.mysql_config.port
        username = self.mysql_config.username
        password = self.mysql_config.password
        database = self.mysql_config.database
        
        def write_partition_to_mysql(partition_data):
            """每个分区的写入逻辑"""
            import pymysql
            import json
            from datetime import date
            
            # 转换迭代器为列表
            rows = list(partition_data)
            if not rows:
                print("分区为空，跳过")
                return
                
            partition_size = len(rows)
            batch_size = 2000  # 每批处理2000条
            print(f"开始处理分区：{partition_size}条数据")
            
            # 建立MySQL连接
            connection = None
            try:
                print(f"连接MySQL: {host}:{port}")
                connection = pymysql.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    charset='utf8mb4',
                    autocommit=False,  # 手动控制事务
                    connect_timeout=30,  # 30秒连接超时
                    read_timeout=60,     # 60秒读取超时
                    write_timeout=60     # 60秒写入超时
                )
                print("MySQL连接成功")
                
                cursor = connection.cursor()
                
                # 根据模式处理
                if mode == "overwrite":
                    # 只在第一个分区执行TRUNCATE（需要协调机制）
                    pass  # 暂时跳过，避免多分区冲突
                
                # 准备批量插入SQL（简化版，避免锁冲突）
                insert_sql = """
                INSERT INTO user_tags (user_id, tag_ids, tag_details, computed_date) 
                VALUES (%s, %s, %s, %s)
                """
                
                # 分批处理数据
                for i in range(0, partition_size, batch_size):
                    batch_rows = rows[i:i + batch_size]
                    batch_data = []
                    
                    print(f"处理批次 {i//batch_size + 1}，数据行数：{len(batch_rows)}")
                    
                    try:
                        for j, row in enumerate(batch_rows):
                            # 处理Spark Row对象数据格式
                            user_id = str(row.user_id)
                            tag_ids = str(row.tag_ids) if row.tag_ids else '[]'
                            tag_details = str(row.tag_details) if row.tag_details else '{}'
                            computed_date = row.computed_date
                            
                            batch_data.append((user_id, tag_ids, tag_details, computed_date))
                            
                            if j < 2:  # 只打印前2条调试
                                print(f"数据样例 {j}: user_id={user_id}, tag_ids={tag_ids[:50]}")
                    
                        print(f"开始执行批量插入，数据量：{len(batch_data)}")
                        cursor.executemany(insert_sql, batch_data)
                        print(f"批量插入SQL执行完成")
                        
                        print(f"分区批次完成：{len(batch_data)}条数据")
                        
                    except Exception as batch_e:
                        print(f"批次处理失败：{str(batch_e)}")
                        raise
                
                # 提交事务
                connection.commit()
                print(f"分区写入完成：总计{partition_size}条数据")
                
            except Exception as e:
                if connection:
                    connection.rollback()
                print(f"分区写入失败：{str(e)}")
                raise
            finally:
                if connection:
                    connection.close()
        
        # 执行分区写入
        try:
            logger.info("开始执行 foreachPartition 自定义批量写入")
            df.foreachPartition(write_partition_to_mysql)
            logger.info("✅ foreachPartition 批量写入完成")
        except Exception as e:
            logger.error(f"❌ 自定义批量写入失败: {str(e)}")
            raise
    
    def _delete_user_tags_for_date(self, computed_date):
        """删除指定日期的用户标签 - 使用行级锁，支持并发"""
        import pymysql
        from datetime import date
        
        connection = None
        try:
            connection = pymysql.connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                user=self.mysql_config.username,
                password=self.mysql_config.password,
                database=self.mysql_config.database,
                charset='utf8mb4',
                autocommit=True
            )
            
            cursor = connection.cursor()
            # 使用DELETE代替TRUNCATE，支持并发
            delete_sql = "DELETE FROM user_tags WHERE computed_date = %s"
            cursor.execute(delete_sql, (computed_date,))
            deleted_count = cursor.rowcount
            logger.info(f"✅ 删除 {computed_date} 的用户标签数据，共 {deleted_count} 条")
            
        except Exception as e:
            logger.error(f"❌ 删除用户标签数据失败: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    def get_all_required_fields_from_rules(self) -> str:
        """从规则DataFrame中获取所有需要的字段"""
        fields_set = set(['user_id'])
        
        # 从DataFrame中获取规则条件，解析字段
        rules_list = self.get_rules_for_computation()
        for rule in rules_list:
            rule_dict = rule.asDict()
            try:
                rule_conditions = json.loads(rule_dict['rule_conditions'])
                conditions = rule_conditions['conditions']
                for condition in conditions:
                    if 'field' in condition:
                        fields_set.add(condition['field'])
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        
        return ','.join(sorted(fields_set))
    
    def cleanup(self):
        """清理所有缓存，释放资源"""
        logger.info("🧹 清理统一数据管理器缓存...")
        
        try:
            # 释放所有persist的DataFrame
            if self._rules_df is not None:
                logger.info("🧹 释放规则DataFrame persist缓存")
                self._rules_df.unpersist()
                
            if self._tag_definitions_df is not None:
                logger.info("🧹 释放标签定义DataFrame persist缓存")
                self._tag_definitions_df.unpersist()
            
            if self._existing_user_tags_df is not None:
                logger.info("🧹 释放现有用户标签DataFrame persist缓存")
                self._existing_user_tags_df.unpersist()
                
            # 清空所有DataFrame引用
            self._rules_df = None
            self._tag_definitions_df = None
            self._existing_user_tags_df = None
            self._initialized = False
            
            logger.info("✅ 数据管理器缓存清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 缓存清理异常: {str(e)}")
    
    def get_statistics(self) -> dict:
        """获取数据统计信息"""
        return {
            "total_rules": self._rules_df.count() if self._rules_df else 0,
            "total_tag_definitions": self._tag_definitions_df.count() if self._tag_definitions_df else 0,
            "existing_user_tags": self._existing_user_tags_df.count() if self._existing_user_tags_df else 0,
            "initialized": self._initialized
        }