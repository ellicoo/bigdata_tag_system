"""
数据写入器 - 整合原有的OptimizedMySQLWriter功能
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_json, current_date

from src.common.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class BatchDataWriter:
    """批处理数据写入器（原OptimizedMySQLWriter功能）"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        self.write_statistics = {
            "total_records": 0,
            "affected_users": 0,
            "write_time_seconds": 0,
            "last_write_timestamp": None
        }
    
    def write_tag_results(self, results_df: DataFrame) -> bool:
        """
        写入标签结果到MySQL
        
        Args:
            results_df: 标签结果DataFrame (user_id, tag_ids, tag_details)
            
        Returns:
            bool: 写入是否成功
        """
        try:
            import time
            start_time = time.time()
            
            if results_df is None:
                logger.warning("⚠️ 结果DataFrame为空，跳过写入")
                return True
            
            record_count = results_df.count()
            if record_count == 0:
                logger.info("📊 没有标签结果需要写入MySQL")
                return True
            
            logger.info(f"📝 开始写入标签结果到MySQL，共 {record_count} 条记录")
            
            # 1. 数据预处理和验证
            processed_df = self._preprocess_for_write(results_df)
            if processed_df is None:
                logger.error("❌ 数据预处理失败")
                return False
            
            # 2. 动态分区优化
            optimized_df = self._optimize_partitions(processed_df)
            
            # 3. 执行UPSERT写入
            success = self._execute_upsert_write(optimized_df)
            
            # 4. 验证写入结果
            if success:
                verification_success = self._verify_write_results(optimized_df)
                if verification_success:
                    # 5. 更新统计信息
                    end_time = time.time()
                    self.write_statistics.update({
                        "total_records": record_count,
                        "affected_users": optimized_df.select("user_id").distinct().count(),
                        "write_time_seconds": round(end_time - start_time, 2),
                        "last_write_timestamp": end_time
                    })
                    
                    logger.info(f"✅ 标签结果写入MySQL成功")
                    # 统计信息不在这里打印，由调用方统一处理
                    return True
                else:
                    logger.error("❌ 写入验证失败")
                    return False
            else:
                logger.error("❌ 标签结果写入MySQL失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 写入标签结果时发生异常: {str(e)}")
            return False
    
    def _preprocess_for_write(self, df: DataFrame) -> Optional[DataFrame]:
        """预处理数据，准备写入"""
        try:
            logger.info("🔄 开始数据预处理...")
            
            # 1. 确保必需字段存在
            required_columns = ["user_id", "tag_ids", "tag_details"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"❌ 缺少必需字段: {missing_columns}")
                return None
            
            # 2. 数据格式转换
            processed_df = df.select(
                col("user_id").cast("string"),
                to_json(col("tag_ids")).alias("tag_ids"),  # 转换为JSON字符串
                col("tag_details").cast("string")
            )
            
            # 3. 数据去重（基于user_id）
            deduplicated_df = processed_df.dropDuplicates(["user_id"])
            
            original_count = df.count()
            processed_count = deduplicated_df.count()
            
            if original_count != processed_count:
                logger.info(f"🔄 用户级别去重: {original_count} → {processed_count} 条记录")
            
            # 4. 数据验证
            if not self._validate_data_quality(deduplicated_df):
                logger.error("❌ 数据质量验证失败")
                return None
            
            logger.info(f"✅ 数据预处理完成，准备写入 {processed_count} 条记录")
            return deduplicated_df
            
        except Exception as e:
            logger.error(f"❌ 数据预处理失败: {str(e)}")
            return None
    
    def _validate_data_quality(self, df: DataFrame) -> bool:
        """验证数据质量"""
        try:
            # 1. 检查空值
            null_user_ids = df.filter(col("user_id").isNull()).count()
            if null_user_ids > 0:
                logger.error(f"❌ 发现 {null_user_ids} 条user_id为空的记录")
                return False
            
            # 2. 检查空标签
            empty_tag_ids = df.filter(
                (col("tag_ids").isNull()) | 
                (col("tag_ids") == "[]") |
                (col("tag_ids") == "null")
            ).count()
            
            if empty_tag_ids > 0:
                logger.warning(f"⚠️ 发现 {empty_tag_ids} 条标签为空的记录，但这可能是正常的")
            
            # 3. 检查user_id格式
            invalid_user_ids = df.filter(
                col("user_id").rlike("^\\s*$") |  # 空白字符
                col("user_id").contains("null")    # 包含null字符串
            ).count()
            
            if invalid_user_ids > 0:
                logger.error(f"❌ 发现 {invalid_user_ids} 条无效的user_id")
                return False
            
            logger.info("✅ 数据质量验证通过")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据质量验证失败: {str(e)}")
            return False
    
    def _optimize_partitions(self, df: DataFrame) -> DataFrame:
        """动态分区优化"""
        try:
            record_count = df.count()
            
            # 根据数据量动态调整分区数
            if record_count < 1000:
                target_partitions = 1
            elif record_count < 10000:
                target_partitions = 2
            elif record_count < 100000:
                target_partitions = 4
            else:
                target_partitions = 8
            
            current_partitions = df.rdd.getNumPartitions()
            
            if current_partitions != target_partitions:
                logger.info(f"🔄 分区优化: {current_partitions} → {target_partitions} 个分区")
                df = df.coalesce(target_partitions)
            else:
                logger.info(f"✅ 分区已优化: {current_partitions} 个分区")
            
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ 分区优化失败: {str(e)}，使用原始分区")
            return df
    
    def _execute_upsert_write(self, df: DataFrame) -> bool:
        """执行UPSERT写入 - 使用已验证的foreachPartition + pymysql方案"""
        import pymysql
        
        # 配置参数
        host = self.mysql_config.host
        port = self.mysql_config.port
        username = self.mysql_config.username
        password = self.mysql_config.password
        database = self.mysql_config.database
        
        def upsert_partition(partition_data):
            """分区UPSERT逻辑 - 复制自OptimizedMySQLWriter"""
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
                
                # 使用已验证的UPSERT SQL
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
            logger.info("📝 执行UPSERT写入到MySQL (使用已验证的foreachPartition方案)...")
            
            # 优化分区数量
            total_count = df.count()
            optimal_partitions = min(8, max(1, total_count // 5000))
            
            logger.info(f"📊 UPSERT写入：{total_count} 条记录，{optimal_partitions} 个分区")
            
            # 重分区并执行
            repartitioned_df = df.repartition(optimal_partitions, "user_id")
            
            # 执行分区UPSERT
            def upsert_partition_with_logging(partition_data):
                try:
                    rows = list(partition_data)
                    if not rows:
                        return
                    
                    print(f"🔄 分区处理 {len(rows)} 条记录...")
                    upsert_partition(rows)
                    print(f"✅ 分区UPSERT完成，处理了 {len(rows)} 条记录")
                except Exception as e:
                    print(f"❌ 分区UPSERT失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    raise
            
            repartitioned_df.foreachPartition(upsert_partition_with_logging)
            
            logger.info("✅ UPSERT写入执行完成")
            return True
            
        except Exception as e:
            logger.error(f"❌ UPSERT写入失败: {str(e)}")
            return False
    
    def _verify_write_results(self, written_df: DataFrame) -> bool:
        """验证写入结果"""
        try:
            logger.info("🔍 开始验证写入结果...")
            
            # 1. 获取写入的用户ID集合
            written_user_ids = set([row.user_id for row in written_df.select("user_id").collect()])
            written_count = len(written_user_ids)
            
            if written_count == 0:
                logger.warning("⚠️ 没有用户需要验证")
                return True
            
            # 2. 从MySQL查询这些用户的标签
            user_ids_str = "', '".join(written_user_ids)
            verification_query = f"""
            (SELECT user_id, tag_ids 
             FROM user_tags 
             WHERE user_id IN ('{user_ids_str}')) as verification_data
            """
            
            mysql_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table=verification_query,
                properties=self.mysql_config.connection_properties
            )
            
            mysql_user_ids = set([row.user_id for row in mysql_df.select("user_id").collect()])
            mysql_count = len(mysql_user_ids)
            
            # 3. 验证写入完整性
            if mysql_count == written_count:
                logger.info(f"✅ 写入验证成功: {mysql_count}/{written_count} 用户标签已正确写入MySQL")
                
                # 4. 抽样验证数据一致性
                self._sample_verify_data_consistency(written_df, mysql_df)
                return True
            else:
                missing_users = written_user_ids - mysql_user_ids
                logger.error(f"❌ 写入验证失败: {mysql_count}/{written_count} 用户写入成功")
                logger.error(f"❌ 缺失用户: {list(missing_users)[:10]}...")  # 只显示前10个
                return False
                
        except Exception as e:
            logger.error(f"❌ 写入验证失败: {str(e)}")
            return False
    
    def _sample_verify_data_consistency(self, written_df: DataFrame, mysql_df: DataFrame):
        """抽样验证数据一致性"""
        try:
            # 抽取3个用户进行详细验证
            sample_users = written_df.limit(3).collect()
            
            logger.info("🔍 抽样验证数据一致性（前3个用户）:")
            
            for user_row in sample_users:
                user_id = user_row.user_id
                written_tag_ids = user_row.tag_ids
                
                # 从MySQL查询对应用户的标签
                mysql_user_data = mysql_df.filter(col("user_id") == user_id).collect()
                if mysql_user_data:
                    mysql_tag_ids = mysql_user_data[0].tag_ids
                    
                    # 将JSON字符串解析为Python对象进行比较，避免格式差异
                    import json
                    try:
                        written_data = json.loads(written_tag_ids) if written_tag_ids else []
                        mysql_data = json.loads(mysql_tag_ids) if mysql_tag_ids else []
                        
                        # 排序后比较，确保顺序无关的一致性
                        written_sorted = sorted(written_data) if isinstance(written_data, list) else written_data
                        mysql_sorted = sorted(mysql_data) if isinstance(mysql_data, list) else mysql_data
                        
                        if written_sorted == mysql_sorted:
                            logger.info(f"   ✅ 用户 {user_id}: 数据一致")
                            logger.info(f"      标签ID: {written_sorted}")
                        else:
                            logger.warning(f"   ⚠️ 用户 {user_id}: 数据不一致")
                            logger.warning(f"      写入: {written_sorted}")
                            logger.warning(f"      MySQL: {mysql_sorted}")
                    except json.JSONDecodeError as e:
                        logger.warning(f"   ⚠️ 用户 {user_id}: JSON解析失败")
                        logger.warning(f"      写入原始: {written_tag_ids}")
                        logger.warning(f"      MySQL原始: {mysql_tag_ids}")
                        logger.warning(f"      错误: {str(e)}")
                else:
                    logger.warning(f"   ⚠️ 用户 {user_id}: MySQL中未找到数据")
            
        except Exception as e:
            logger.warning(f"⚠️ 抽样验证失败: {str(e)}")
    
    def get_write_statistics(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        return self.write_statistics.copy()
    
    def reset_statistics(self):
        """重置统计信息"""
        self.write_statistics = {
            "total_records": 0,
            "affected_users": 0,
            "write_time_seconds": 0,
            "last_write_timestamp": None
        }
        logger.info("📊 写入统计信息已重置")
    
    def cleanup(self):
        """清理资源"""
        try:
            # 这里可以添加清理逻辑，如关闭连接等
            logger.info("✅ 数据写入器清理完成")
        except Exception as e:
            logger.warning(f"⚠️ 数据写入器清理异常: {str(e)}")