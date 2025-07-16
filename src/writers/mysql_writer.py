import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class MySQLTagWriter:
    """MySQL标签结果写入器"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
    
    def write_tag_results(self, result_df: DataFrame, mode: str = "overwrite", 
                         enable_backup: bool = False, merge_with_existing: bool = True) -> bool:
        """
        写入标签结果到MySQL
        
        Args:
            result_df: 标签结果DataFrame
            mode: 写入模式 (overwrite/append)
            enable_backup: 是否启用备份
            merge_with_existing: 是否与现有标签合并
            
        Returns:
            写入是否成功
        """
        try:
            if enable_backup and mode == "overwrite":
                # 备份现有数据
                backup_success = self._backup_current_data()
                if not backup_success:
                    logger.warning("备份失败，但继续执行写入操作")
            
            # 执行写入
            success = self._write_to_mysql(result_df, mode, merge_with_existing)
            
            if success:
                logger.info(f"✅ 标签结果写入成功，模式: {mode}, 记录数: {result_df.count()}")
                
                # 验证写入结果
                if self._validate_write_result(result_df, mode):
                    return True
                else:
                    logger.error("写入结果验证失败")
                    return False
            else:
                logger.error("标签结果写入失败")
                return False
                
        except Exception as e:
            logger.error(f"写入标签结果异常: {str(e)}")
            
            # 如果启用了备份且写入失败，尝试恢复
            if enable_backup and mode == "overwrite":
                logger.info("尝试从备份恢复数据...")
                self._restore_from_backup()
            
            return False
    
    def _write_to_mysql(self, result_df: DataFrame, mode: str, merge_with_existing: bool = True) -> bool:
        """执行MySQL写入操作 - 统一UPSERT逻辑"""
        try:
            # 根据是否需要合并决定处理逻辑
            if merge_with_existing:
                final_df = self._merge_with_existing_tags(result_df)
            else:
                final_df = result_df
            
            # 将Spark数组转换为JSON字符串，保持数组结构
            from pyspark.sql.functions import to_json, col, when
            
            mysql_ready_df = final_df.select(
                col("user_id"),
                # 确保tag_ids是JSON数组字符串格式
                when(col("tag_ids").isNotNull(), to_json(col("tag_ids")))
                .otherwise("[]").alias("tag_ids"),
                # tag_details已经是JSON字符串格式
                col("tag_details"),
                col("computed_date")
            )
            
            # 显示数据样例用于调试
            logger.info("准备写入MySQL的数据样例:")
            mysql_ready_df.show(3, truncate=False)
            
            total_count = mysql_ready_df.count()
            logger.info(f"准备UPSERT {total_count} 条用户标签数据")
            
            # 统一使用UPSERT策略
            logger.info(f"使用 UPSERT 策略写入 {total_count} 条数据")
            return self._write_with_upsert(mysql_ready_df)
            
        except Exception as e:
            logger.error(f"MySQL写入操作失败: {str(e)}")
            return False
    
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
    
    # 移除了优化JDBC写入方法，统一使用foreachPartition
    
    def _merge_with_existing_tags(self, result_df: DataFrame) -> DataFrame:
        """与现有标签合并"""
        try:
            logger.info("开始与现有标签合并...")
            
            # 读取现有标签
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.mysql_config.jdbc_url,
                    table="user_tags",
                    properties=self.mysql_config.connection_properties
                )
                
                if existing_df.count() == 0:
                    logger.info("数据库中没有现有标签，直接使用新计算的标签")
                    return result_df
                    
            except Exception as e:
                logger.info(f"读取现有标签失败（可能是首次运行）: {str(e)}")
                return result_df
            
            # 将JSON字符串转换为数组进行合并
            from pyspark.sql.functions import from_json, col, array_union, array_distinct
            from pyspark.sql.types import ArrayType, IntegerType
            
            existing_with_arrays = existing_df.select(
                col("user_id"),
                from_json(col("tag_ids"), ArrayType(IntegerType())).alias("existing_tag_ids")
            )
            
            # 左连接合并标签
            merged_df = result_df.join(
                existing_with_arrays,
                "user_id",
                "left"
            )
            
            # 合并标签数组并去重
            from pyspark.sql.functions import when, array_distinct, array_union
            
            final_merged_df = merged_df.select(
                col("user_id"),
                when(col("existing_tag_ids").isNull(), col("tag_ids"))
                .otherwise(array_distinct(array_union(col("existing_tag_ids"), col("tag_ids"))))
                .alias("tag_ids"),
                col("tag_details"),
                col("computed_date")
            )
            
            logger.info("✅ 标签合并完成")
            return final_merged_df
            
        except Exception as e:
            logger.error(f"标签合并失败: {str(e)}")
            return result_df
    
    def _write_with_upsert(self, df: DataFrame) -> bool:
        """统一的UPSERT写入 - 使用INSERT ... ON DUPLICATE KEY UPDATE"""
        import pymysql
        
        # 提取配置参数避免闭包序列化问题
        host = self.mysql_config.host
        port = self.mysql_config.port
        username = self.mysql_config.username
        password = self.mysql_config.password
        database = self.mysql_config.database
        
        def upsert_partition_to_mysql(partition_data):
            """每个分区的UPSERT逻辑"""
            import pymysql
            
            rows = list(partition_data)
            if not rows:
                return
                
            partition_size = len(rows)
            batch_size = 2000
            
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
                    write_timeout=60,
                    init_command="SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                
                cursor = connection.cursor()
                
                # UPSERT SQL: 用户存在则更新，不存在则插入
                upsert_sql = """
                INSERT INTO user_tags (user_id, tag_ids, tag_details, computed_date) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tag_ids = VALUES(tag_ids),
                    tag_details = VALUES(tag_details),
                    computed_date = VALUES(computed_date)
                """
                
                # 分批处理数据
                for i in range(0, partition_size, batch_size):
                    batch_rows = rows[i:i + batch_size]
                    batch_data = []
                    
                    for row in batch_rows:
                        user_id = str(row.user_id)
                        tag_ids = str(row.tag_ids) if row.tag_ids else '[]'
                        tag_details = str(row.tag_details) if row.tag_details else '{}'
                        computed_date = row.computed_date
                        
                        batch_data.append((user_id, tag_ids, tag_details, computed_date))
                    
                    cursor.executemany(upsert_sql, batch_data)
                
                # 提交事务
                connection.commit()
                
            except Exception as e:
                if connection:
                    connection.rollback()
                raise
            finally:
                if connection:
                    connection.close()
        
        try:
            # 合理分区避免过多连接
            optimal_partitions = min(8, max(1, df.count() // 8000))
            logger.info(f"🔍 MySQL UPSERT分区设置：{optimal_partitions} 个分区")
            repartitioned_df = df.repartition(optimal_partitions, "user_id")
            
            # 调试：检查重分区后是否有重复
            logger.info("🔍 检查重分区后是否有重复用户...")
            user_counts = repartitioned_df.groupBy("user_id").count()
            duplicates = user_counts.filter(user_counts["count"] > 1)
            duplicate_count = duplicates.count()
            if duplicate_count > 0:
                logger.error(f"❌ 发现MySQL写入前有重复用户！重复数: {duplicate_count}")
                duplicates.show(10, truncate=False)
                return False
            else:
                logger.info("✅ MySQL写入前无重复用户")
            
            repartitioned_df.foreachPartition(upsert_partition_to_mysql)
            return True
            
        except Exception as e:
            logger.error(f"UPSERT写入失败: {str(e)}")
            return False
    
    def _backup_current_data(self) -> bool:
        """备份当前数据"""
        try:
            backup_table_name = f"user_tags_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 读取当前数据
            current_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            if current_df.count() == 0:
                logger.info("当前表为空，无需备份")
                return True
            
            # 写入备份表
            current_df.write.jdbc(
                url=self.mysql_config.jdbc_url,
                table=backup_table_name,
                mode="overwrite",
                properties=self.mysql_config.connection_properties
            )
            
            logger.info(f"数据备份成功，备份表: {backup_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"数据备份失败: {str(e)}")
            return False
    
    def _restore_from_backup(self) -> bool:
        """从最新备份恢复数据"""
        try:
            # 这里简化处理，实际应该查询最新的备份表
            # 可以通过information_schema.tables查询备份表
            logger.warning("自动恢复功能需要手动实现，请检查备份表")
            return False
            
        except Exception as e:
            logger.error(f"数据恢复失败: {str(e)}")
            return False
    
    def _validate_write_result(self, original_df: DataFrame, mode: str = "overwrite") -> bool:
        """验证写入结果 - 统一以打到标签的用户数为核对标准"""
        try:
            # 获取需要写入的用户ID列表（这些是计算出标签的用户）
            original_user_ids = original_df.select("user_id").distinct().collect()
            original_user_id_set = {row["user_id"] for row in original_user_ids}
            tagged_user_count = len(original_user_id_set)
            
            logger.info(f"写入验证 - 本次打到标签的用户数: {tagged_user_count}, 模式: {mode}")
            
            if tagged_user_count == 0:
                logger.info("✅ 无用户打到标签，验证通过")
                return True
            
            # 读取写入后的数据，只检查需要写入的用户
            written_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            # 检查打到标签的用户是否都已成功写入
            written_user_ids = written_df.select("user_id").distinct().collect()
            written_user_id_set = {row["user_id"] for row in written_user_ids}
            
            # 检查是否所有打到标签的用户都已成功写入
            missing_users = original_user_id_set - written_user_id_set
            if missing_users:
                logger.error(f"❌ 写入验证失败：以下打到标签的用户未成功写入 {list(missing_users)[:5]}...")
                return False
            
            # 检查写入的用户是否都有有效的标签数据
            from pyspark.sql.functions import col, from_json, size
            from pyspark.sql.types import ArrayType, IntegerType
            
            # 只检查本次打到标签的用户
            target_users_df = written_df.filter(col("user_id").isin(list(original_user_id_set)))
            
            # 检查标签数组字段（JSON格式）
            parsed_for_validation = target_users_df.select(
                "user_id",
                from_json("tag_ids", ArrayType(IntegerType())).alias("tag_ids_array")
            )
            
            # 检查关键字段
            user_id_count = target_users_df.filter(target_users_df.user_id.isNotNull()).count()
            if user_id_count != tagged_user_count:
                logger.error("❌ 存在空的user_id")
                return False
            
            # 检查标签数组是否有效
            null_tag_ids_count = parsed_for_validation.filter(
                col("tag_ids_array").isNull() | (size("tag_ids_array") == 0)
            ).count()
            
            if null_tag_ids_count > 0:
                logger.warning(f"⚠️ 存在 {null_tag_ids_count} 个用户没有标签（可能是正常情况）")
            
            successfully_written = tagged_user_count - len(missing_users)
            logger.info(f"✅ 写入验证通过：成功写入 {successfully_written}/{tagged_user_count} 个打到标签的用户")
            
            return True
            
        except Exception as e:
            logger.error(f"写入结果验证失败: {str(e)}")
            return False
    
    def write_incremental_tags(self, new_tags_df: DataFrame) -> bool:
        """增量写入标签（更新已存在的用户，插入新用户）- 适配新数据模型"""
        try:
            # 读取现有数据
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.mysql_config.jdbc_url,
                    table="user_tags",
                    properties=self.mysql_config.connection_properties
                )
            except Exception:
                logger.info("读取现有数据失败，可能是首次运行")
                return self.write_tag_results(new_tags_df, mode="overwrite")
            
            if existing_df.count() == 0:
                logger.info("现有数据为空，直接写入新数据")
                return self.write_tag_results(new_tags_df, mode="overwrite")
            
            # 找出需要更新的用户（已存在的用户）
            update_users = new_tags_df.join(
                existing_df.select("user_id"), 
                "user_id", 
                "inner"
            )
            
            # 找出需要插入的新用户
            insert_users = new_tags_df.join(
                existing_df.select("user_id"), 
                "user_id", 
                "left_anti"
            )
            
            update_count = update_users.count()
            insert_count = insert_users.count()
            
            logger.info(f"增量写入 - 更新用户数: {update_count}, 新增用户数: {insert_count}")
            
            # 对于新数据模型，由于每个用户只有一条记录，且标签是数组形式
            # 我们需要先删除现有的用户记录，然后插入更新的记录
            
            if update_count > 0:
                # 构建删除语句（删除需要更新的用户）
                user_ids_to_update = update_users.select("user_id").distinct().collect()
                user_id_list = [f"'{row['user_id']}'" for row in user_ids_to_update]
                
                if user_id_list:
                    delete_sql = f"DELETE FROM user_tags WHERE user_id IN ({','.join(user_id_list)})"
                    
                    # 执行删除（通过临时连接）
                    connection_props = {
                        "user": self.mysql_config.connection_properties["user"],
                        "password": self.mysql_config.connection_properties["password"],
                        "driver": self.mysql_config.connection_properties["driver"]
                    }
                    
                    # 使用PyMySQL直接执行删除，避免临时表创建
                    import pymysql
                    
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
                        delete_sql = f"DELETE FROM user_tags WHERE user_id IN ({','.join(user_id_list)})"
                        cursor.execute(delete_sql)
                        deleted_count = cursor.rowcount
                        logger.info(f"删除 {deleted_count} 个用户的旧标签记录")
                    except Exception as e:
                        logger.error(f"删除旧标签记录失败: {str(e)}")
                        raise
                    finally:
                        if connection:
                            connection.close()
                    
                    logger.info(f"删除 {len(user_id_list)} 个用户的旧标签记录")
            
            # 写入所有新数据（包括更新和新增的用户）
            return self.write_tag_results(new_tags_df, mode="append")
            
        except Exception as e:
            logger.error(f"增量写入失败: {str(e)}")
            # 失败时回退到覆盖模式
            logger.info("增量写入失败，回退到覆盖模式")
            return self.write_tag_results(new_tags_df, mode="overwrite")
    
    def get_write_statistics(self) -> dict:
        """获取写入统计信息 - 适配新的数据模型（一个用户一条记录，包含标签ID数组）"""
        try:
            stats_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            total_users = stats_df.count()
            if total_users == 0:
                return {
                    "total_users": 0,
                    "total_tag_assignments": 0,
                    "average_tags_per_user": 0,
                    "max_tags_per_user": 0,
                    "min_tags_per_user": 0,
                    "unique_tags": 0
                }
            
            # 统计每个用户的标签数（通过JSON数组长度）
            from pyspark.sql.functions import from_json, size, expr, explode
            from pyspark.sql.types import ArrayType, IntegerType
            
            # 解析JSON数组
            parsed_df = stats_df.select(
                "user_id",
                from_json("tag_ids", ArrayType(IntegerType())).alias("tag_ids_array")
            )
            
            user_tag_stats = parsed_df.select(
                "user_id",
                size("tag_ids_array").alias("tag_count")
            )
            
            tag_counts = user_tag_stats.select("tag_count").collect()
            tag_count_values = [row['tag_count'] for row in tag_counts]
            
            total_assignments = sum(tag_count_values)
            
            # 统计唯一标签数（需要展开数组）
            unique_tags_df = parsed_df.select(explode("tag_ids_array").alias("tag_id"))
            unique_tags = unique_tags_df.select("tag_id").distinct().count()
            
            return {
                "total_users": total_users,
                "total_tag_assignments": total_assignments,
                "average_tags_per_user": round(total_assignments / total_users, 2) if total_users > 0 else 0,
                "max_tags_per_user": max(tag_count_values) if tag_count_values else 0,
                "min_tags_per_user": min(tag_count_values) if tag_count_values else 0,
                "unique_tags": unique_tags
            }
            
        except Exception as e:
            logger.error(f"获取写入统计失败: {str(e)}")
            return {}
    
    def cleanup_old_backups(self, keep_days: int = 7) -> bool:
        """清理旧的备份表"""
        try:
            # 这里需要实现备份表的清理逻辑
            # 查询information_schema.tables找出旧的备份表并删除
            logger.info(f"备份清理功能待实现，保留{keep_days}天内的备份")
            return True
            
        except Exception as e:
            logger.error(f"清理备份失败: {str(e)}")
            return False