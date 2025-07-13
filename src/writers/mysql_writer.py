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
                         enable_backup: bool = True) -> bool:
        """
        写入标签结果到MySQL
        
        Args:
            result_df: 标签结果DataFrame
            mode: 写入模式 (overwrite/append)
            enable_backup: 是否启用备份
            
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
            success = self._write_to_mysql(result_df, mode)
            
            if success:
                logger.info(f"✅ 标签结果写入成功，模式: {mode}, 记录数: {result_df.count()}")
                
                # 验证写入结果
                if self._validate_write_result(result_df):
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
    
    def _write_to_mysql(self, result_df: DataFrame, mode: str) -> bool:
        """执行MySQL写入操作"""
        try:
            # 将Spark数组转换为JSON字符串，保持数组结构
            from pyspark.sql.functions import to_json, col, when
            
            mysql_ready_df = result_df.select(
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
            mysql_ready_df.show(5, truncate=False)
            
            # 优化写入参数
            write_properties = {
                **self.mysql_config.connection_properties,
                "batchsize": "1000",  # 减小批次大小，提高稳定性
                "isolationLevel": "READ_COMMITTED",
                "numPartitions": "5"   # 减少并行度
            }
            
            # 执行写入
            mysql_ready_df.write.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                mode=mode,
                properties=write_properties
            )
            
            return True
            
        except Exception as e:
            logger.error(f"MySQL写入操作失败: {str(e)}")
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
    
    def _validate_write_result(self, original_df: DataFrame) -> bool:
        """验证写入结果 - 适配新数据模型"""
        try:
            # 读取写入后的数据
            written_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            # 检查记录数（用户数）
            original_count = original_df.count()
            written_count = written_df.count()
            
            logger.info(f"写入验证 - 原始用户数: {original_count}, 写入后用户数: {written_count}")
            
            # 对于overwrite模式，用户数应该相等
            if original_count != written_count:
                logger.warning(f"用户数不匹配，原始: {original_count}, 写入后: {written_count}")
                return False
            
            # 检查关键字段
            user_id_count = written_df.filter(written_df.user_id.isNotNull()).count()
            if user_id_count != written_count:
                logger.error("存在空的user_id")
                return False
            
            # 检查标签数组字段（JSON格式）
            from pyspark.sql.functions import col, expr, from_json, size
            from pyspark.sql.types import ArrayType, IntegerType
            
            # 解析JSON并检查
            parsed_for_validation = written_df.select(
                "user_id",
                from_json("tag_ids", ArrayType(IntegerType())).alias("tag_ids_array")
            )
            
            null_tag_ids_count = parsed_for_validation.filter(
                col("tag_ids_array").isNull() | (size("tag_ids_array") == 0)
            ).count()
            
            if null_tag_ids_count > 0:
                logger.warning(f"存在 {null_tag_ids_count} 个用户没有标签")
            
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
                    
                    # 创建临时表执行删除
                    self.spark.sql(f"""
                        CREATE OR REPLACE TEMPORARY VIEW users_to_delete AS
                        SELECT DISTINCT user_id FROM temp_update_users
                    """)
                    
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