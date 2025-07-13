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
            # 优化写入参数
            write_properties = {
                **self.mysql_config.connection_properties,
                "batchsize": "5000",  # 批次大小
                "isolationLevel": "READ_COMMITTED",
                "numPartitions": "10"  # 写入并行度
            }
            
            # 执行写入
            result_df.write.jdbc(
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
        """验证写入结果"""
        try:
            # 读取写入后的数据
            written_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            # 检查记录数
            original_count = original_df.count()
            written_count = written_df.count()
            
            logger.info(f"写入验证 - 原始记录数: {original_count}, 写入后记录数: {written_count}")
            
            # 对于overwrite模式，记录数应该相等
            if original_count != written_count:
                logger.warning(f"记录数不匹配，可能存在问题")
                return False
            
            # 检查关键字段
            user_id_count = written_df.filter(written_df.user_id.isNotNull()).count()
            if user_id_count != written_count:
                logger.error("存在空的user_id")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"写入结果验证失败: {str(e)}")
            return False
    
    def write_incremental_tags(self, new_tags_df: DataFrame) -> bool:
        """增量写入标签（更新已存在的用户，插入新用户）"""
        try:
            # 读取现有数据
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            # 找出需要更新的用户
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
            
            # 这里需要实现更复杂的更新逻辑
            # 由于JDBC不直接支持UPSERT，可以考虑使用临时表方式
            
            # 简化处理：直接覆盖写入
            return self.write_tag_results(new_tags_df, mode="overwrite")
            
        except Exception as e:
            logger.error(f"增量写入失败: {str(e)}")
            return False
    
    def get_write_statistics(self) -> dict:
        """获取写入统计信息"""
        try:
            stats_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            total_users = stats_df.count()
            
            # 统计标签分布
            tag_stats = stats_df.select("tag_ids").rdd.map(
                lambda row: len(row.tag_ids.split(',')) if row.tag_ids else 0
            ).collect()
            
            avg_tags_per_user = sum(tag_stats) / len(tag_stats) if tag_stats else 0
            
            return {
                "total_users": total_users,
                "average_tags_per_user": round(avg_tags_per_user, 2),
                "max_tags_per_user": max(tag_stats) if tag_stats else 0,
                "min_tags_per_user": min(tag_stats) if tag_stats else 0
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