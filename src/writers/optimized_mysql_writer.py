import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_json, col, when

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class OptimizedMySQLWriter:
    """优化的MySQL写入器 - 统一使用UPSERT策略避免全表覆盖"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
    
    def write_tag_results(self, result_df: DataFrame) -> bool:
        """
        写入标签结果 - 统一使用UPSERT策略
        
        Args:
            result_df: 标签结果DataFrame (user_id, tag_ids, tag_details, computed_date)
            
        Returns:
            写入是否成功
        """
        try:
            if result_df.count() == 0:
                logger.info("没有数据需要写入")
                return True
            
            logger.info("🚀 开始UPSERT写入标签结果...")
            
            # 转换为MySQL兼容格式
            mysql_ready_df = self._prepare_for_mysql(result_df)
            
            # 执行UPSERT写入
            success = self._upsert_to_mysql(mysql_ready_df)
            
            if success:
                # 验证写入结果
                return self._validate_write_result(result_df)
            
            return False
            
        except Exception as e:
            logger.error(f"写入标签结果失败: {str(e)}")
            return False
    
    def _prepare_for_mysql(self, result_df: DataFrame) -> DataFrame:
        """准备MySQL写入格式"""
        try:
            # 转换tag_ids为JSON字符串，移除computed_date字段
            mysql_ready_df = result_df.select(
                col("user_id"),
                when(col("tag_ids").isNotNull(), to_json(col("tag_ids")))
                .otherwise("[]").alias("tag_ids"),
                col("tag_details")
            )
            
            # 数据预览
            logger.info("MySQL写入数据样例:")
            mysql_ready_df.show(3, truncate=False)
            
            return mysql_ready_df
            
        except Exception as e:
            logger.error(f"MySQL数据准备失败: {str(e)}")
            raise
    
    def _upsert_to_mysql(self, df: DataFrame) -> bool:
        """统一UPSERT策略 - INSERT ON DUPLICATE KEY UPDATE"""
        import pymysql
        
        # 配置参数
        host = self.mysql_config.host
        port = self.mysql_config.port
        username = self.mysql_config.username
        password = self.mysql_config.password
        database = self.mysql_config.database
        
        def upsert_partition(partition_data):
            """分区UPSERT逻辑"""
            import pymysql
            
            rows = list(partition_data)
            if not rows:
                return
            
            connection = None
            try:
                connection = pymysql.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    charset='utf8mb4',
                    autocommit=False,
                    connect_timeout=30,
                    read_timeout=60,
                    write_timeout=60
                )
                
                cursor = connection.cursor()
                
                # UPSERT SQL - 只有当标签数据真正变化时才更新updated_time
                # created_time永远不变，保持第一次插入的时间
                # 修复执行顺序问题：先比较后更新，确保比较的是旧值和新值
                upsert_sql = """
                INSERT INTO user_tags (user_id, tag_ids, tag_details) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    updated_time = CASE 
                        WHEN JSON_EXTRACT(tag_ids, '$') <> JSON_EXTRACT(VALUES(tag_ids), '$')
                        THEN CURRENT_TIMESTAMP 
                        ELSE updated_time 
                    END,
                    tag_ids = VALUES(tag_ids),
                    tag_details = VALUES(tag_details)
                """
                
                # 批量处理
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    batch_data = []
                    
                    for row in batch:
                        batch_data.append((
                            str(row.user_id),
                            str(row.tag_ids) if row.tag_ids else '[]',
                            str(row.tag_details) if row.tag_details else '{}'
                        ))
                    
                    cursor.executemany(upsert_sql, batch_data)
                
                connection.commit()
                
            except Exception as e:
                if connection:
                    connection.rollback()
                raise
            finally:
                if connection:
                    connection.close()
        
        try:
            # 优化分区数量
            total_count = df.count()
            optimal_partitions = min(8, max(1, total_count // 5000))
            
            logger.info(f"📊 UPSERT写入：{total_count} 条记录，{optimal_partitions} 个分区")
            
            # 重分区并执行
            repartitioned_df = df.repartition(optimal_partitions, "user_id")
            
            # 加强错误处理的分区函数
            def upsert_partition_with_logging(partition_data):
                try:
                    rows = list(partition_data)
                    if not rows:
                        return
                    
                    print(f"🔄 分区处理 {len(rows)} 条记录...")
                    upsert_partition(rows)  # 传递rows而不是partition_data
                    print(f"✅ 分区UPSERT完成，处理了 {len(rows)} 条记录")
                except Exception as e:
                    print(f"❌ 分区UPSERT失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    raise
            
            repartitioned_df.foreachPartition(upsert_partition_with_logging)
            
            logger.info("✅ UPSERT写入完成")
            return True
            
        except Exception as e:
            logger.error(f"UPSERT写入失败: {str(e)}")
            return False
    
    def _validate_write_result(self, original_df: DataFrame) -> bool:
        """验证写入结果 - 只验证任务类打到标签的用户是否成功写入"""
        try:
            # 获取应该写入的用户
            original_users = original_df.select("user_id").distinct().collect()
            original_user_set = {row["user_id"] for row in original_users}
            expected_count = len(original_user_set)
            
            if expected_count == 0:
                logger.info("✅ 无数据写入，验证通过")
                return True
            
            # 只检查当前任务类打到标签的用户是否成功写入
            written_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            if written_df.count() == 0:
                logger.error(f"❌ 数据库中没有记录，但期望写入 {expected_count} 个用户")
                return False
            
            written_users = written_df.select("user_id").distinct().collect()
            written_user_set = {row["user_id"] for row in written_users}
            
            # 检查当前任务类打到标签的用户是否都已写入
            missing_users = original_user_set - written_user_set
            if missing_users:
                logger.error(f"❌ 任务类打到标签的用户未成功写入: {list(missing_users)[:5]}...")
                return False
            
            logger.info(f"✅ 任务类打到标签的用户写入验证通过：{expected_count} 个用户成功写入")
            return True
            
        except Exception as e:
            logger.error(f"写入验证失败: {str(e)}")
            return False
    
    def get_write_statistics(self) -> dict:
        """获取写入统计"""
        try:
            from pyspark.sql.functions import from_json, size, expr, explode
            from pyspark.sql.types import ArrayType, IntegerType
            
            stats_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            total_users = stats_df.count()
            if total_users == 0:
                return {"total_users": 0, "total_tag_assignments": 0}
            
            # 解析JSON统计
            parsed_df = stats_df.select(
                "user_id",
                from_json("tag_ids", ArrayType(IntegerType())).alias("tag_ids_array")
            )
            
            tag_counts = parsed_df.select(
                size("tag_ids_array").alias("tag_count")
            ).collect()
            
            tag_count_values = [row['tag_count'] for row in tag_counts]
            total_assignments = sum(tag_count_values)
            
            return {
                "total_users": total_users,
                "total_tag_assignments": total_assignments,
                "average_tags_per_user": round(total_assignments / total_users, 2) if total_users > 0 else 0,
                "max_tags_per_user": max(tag_count_values) if tag_count_values else 0
            }
            
        except Exception as e:
            logger.error(f"获取统计失败: {str(e)}")
            return {}