"""
结果合并器 - 整合原有的AdvancedTagMerger和UnifiedTagMerger功能
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit, udf, array_distinct
from pyspark.sql.types import ArrayType, IntegerType, StringType

from src.common.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class BatchResultMerger:
    """批处理结果合并器（原AdvancedTagMerger和UnifiedTagMerger功能）"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
    
    def merge_with_existing_tags(self, new_tags_df: DataFrame, cached_existing_tags: DataFrame = None) -> Optional[DataFrame]:
        """
        与MySQL中现有标签合并
        
        Args:
            new_tags_df: 新计算的标签DataFrame (user_id, tag_ids, tag_details)
            cached_existing_tags: 预缓存的现有标签
            
        Returns:
            合并后的DataFrame
        """
        try:
            logger.info("开始与MySQL中现有标签合并...")
            
            # 1. 使用预缓存的现有标签数据
            if cached_existing_tags is not None:
                existing_tags = cached_existing_tags
                logger.info("使用预缓存的现有标签数据")
            else:
                # 兜底：读取现有标签并使用内存+磁盘持久化
                existing_tags = self._read_existing_user_tags()
                if existing_tags is not None:
                    from pyspark import StorageLevel
                    existing_tags = existing_tags.persist(StorageLevel.MEMORY_AND_DISK)
            
            if existing_tags is None or existing_tags.count() == 0:
                logger.info("数据库中没有现有标签，直接返回新标签")
                return new_tags_df
            
            existing_count = existing_tags.count()
            logger.info(f"现有标签数据: {existing_count} 条用户标签")
            
            # 3. 左连接合并 - 修复列名冲突问题
            merged_df = new_tags_df.alias("new").join(
                existing_tags.select("user_id", "tag_ids").alias("existing"),
                "user_id",
                "left"
            )
            
            # 4. 合并标签数组
            final_merged = merged_df.select(
                col("user_id"),
                self._merge_tag_arrays(
                    col("existing.tag_ids"), 
                    col("new.tag_ids")
                ).alias("tag_ids"),
                col("new.tag_details")
            )
            
            # 5. 详细的标签合并过程追踪日志
            logger.info("📊 标签合并完整过程追踪（展示有标签的用户前3个）:")
            
            # 获取有合并结果的用户（而不是随机前3个用户）
            sample_users = final_merged.limit(3).collect()
            sample_user_ids = [row.user_id for row in sample_users]
            
            # 注意：这里的new_tags_df实际上是经过内存合并后的结果，不是单个任务的原始结果
            # 需要展示完整的合并链路
            for user_id in sample_user_ids:
                logger.info(f"   👤 用户 {user_id} 标签合并全过程:")
                
                # 1. 多任务内存合并后的标签（这是传入的new_tags_df）
                memory_merged_tags = new_tags_df.filter(col("user_id") == user_id).collect()
                memory_merged_tag_ids = memory_merged_tags[0].tag_ids if memory_merged_tags else []
                
                # 2. MySQL现有标签
                mysql_existing_tags = existing_tags.filter(col("user_id") == user_id).collect()
                mysql_existing_tag_ids = mysql_existing_tags[0].tag_ids if mysql_existing_tags else []
                
                # 3. 最终合并后标签
                final_merged_tags = final_merged.filter(col("user_id") == user_id).collect()
                final_merged_tag_ids = final_merged_tags[0].tag_ids if final_merged_tags else []
                
                logger.info(f"      📝 多任务内存合并后标签: {memory_merged_tag_ids}")
                logger.info(f"      🗄️  MySQL现有标签: {mysql_existing_tag_ids}")
                logger.info(f"      ✅ 最终合并后标签: {final_merged_tag_ids}")
                
                # 4. 分析合并变化
                if mysql_existing_tag_ids:
                    # 有现有标签的情况
                    added_from_memory = [tag for tag in final_merged_tag_ids if tag not in mysql_existing_tag_ids]
                    if added_from_memory:
                        logger.info(f"      ➕ 从内存合并新增: {added_from_memory}")
                    else:
                        logger.info(f"      ➕ 从内存合并新增: 无 (标签重复或无变化)")
                else:
                    # 首次标签的情况
                    logger.info(f"      ➕ 首次标签合并: {final_merged_tag_ids}")
                
                # 5. 合并逻辑验证
                expected_merged = sorted(list(set(memory_merged_tag_ids + mysql_existing_tag_ids)))
                actual_merged = sorted(final_merged_tag_ids)
                if expected_merged == actual_merged:
                    logger.info(f"      ✅ 合并逻辑正确")
                else:
                    logger.info(f"      ❌ 合并逻辑异常 - 期望: {expected_merged}, 实际: {actual_merged}")
                
                logger.info(f"      ─" * 60)
            
            # 注意：不在这里清理预缓存数据，由场景调度器统一管理
            if cached_existing_tags is None and existing_tags is not None:
                # 只有非预缓存数据才需要在这里清理
                existing_tags.unpersist()
            
            merge_count = final_merged.count()
            logger.info(f"✅ 与现有标签合并完成，影响 {merge_count} 个用户")
            
            return final_merged
            
        except Exception as e:
            logger.error(f"与现有标签合并失败: {str(e)}")
            # 失败时返回原始数据
            return new_tags_df
    
    def _read_existing_user_tags(self) -> Optional[DataFrame]:
        """从MySQL读取现有用户标签并缓存到内存/磁盘"""
        try:
            logger.info("📖 从MySQL读取现有用户标签...")
            
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            if existing_df.count() == 0:
                logger.info("MySQL中没有现有标签数据")
                return None
            
            # 将JSON字符串转换为数组类型，保留时间字段用于调试
            processed_df = existing_df.select(
                "user_id",
                from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                "tag_details",
                "created_time",
                "updated_time"
            )
            
            # 持久化到内存和磁盘
            processed_df = processed_df.persist()
            
            logger.info(f"成功读取并缓存现有标签数据")
            return processed_df
            
        except Exception as e:
            logger.info(f"读取现有标签失败（可能是首次运行）: {str(e)}")
            return None
    
    def _merge_tag_arrays(self, existing_tags_col, new_tags_col):
        """合并两个标签数组并去重"""
        @udf(returnType=ArrayType(IntegerType()))
        def merge_arrays(existing_tags, new_tags):
            # 确保输入都是列表类型
            if existing_tags is None:
                existing_tags = []
            if new_tags is None:
                new_tags = []
                
            # 如果输入不是列表，转换为列表
            if not isinstance(existing_tags, list):
                existing_tags = []
            if not isinstance(new_tags, list):
                new_tags = []
            
            # 合并并去重，保持排序
            merged = list(set(existing_tags + new_tags))
            return sorted(merged)
        
        return merge_arrays(existing_tags_col, new_tags_col)
    
    def cleanup_cache(self):
        """清理缓存资源"""
        try:
            self.spark.catalog.clearCache()
            logger.info("✅ 清理标签合并缓存完成")
        except Exception as e:
            logger.warning(f"清理缓存失败: {str(e)}")


class TagMergeStrategy:
    """标签合并策略枚举"""
    
    # 不与现有标签合并，直接内存合并结果
    MEMORY_ONLY = "memory_only"
    
    # 与现有标签合并，内存合并后再与MySQL标签合并
    MEMORY_THEN_DATABASE = "memory_then_database"


class UnifiedTagMerger:
    """统一标签合并器 - 根据场景选择合并策略（保持兼容性）"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        self.advanced_merger = BatchResultMerger(spark, mysql_config)
    
    def merge_tags(self, tag_results: list, strategy: str) -> Optional[DataFrame]:
        """
        根据策略合并标签
        
        Args:
            tag_results: 标签计算结果列表
            strategy: 合并策略 (MEMORY_ONLY | MEMORY_THEN_DATABASE)
            
        Returns:
            合并后的DataFrame
        """
        try:
            if not tag_results:
                logger.warning("没有标签结果需要合并")
                return None
            
            logger.info(f"使用策略 {strategy} 合并标签")
            
            # 第一步：内存合并（所有策略都需要）
            memory_merged = self._memory_merge(tag_results)
            if memory_merged is None:
                return None
            
            # 第二步：根据策略决定是否与数据库合并
            if strategy == TagMergeStrategy.MEMORY_ONLY:
                logger.info("仅内存合并，不与数据库现有标签合并")
                return memory_merged
            
            elif strategy == TagMergeStrategy.MEMORY_THEN_DATABASE:
                logger.info("内存合并后，再与数据库现有标签合并")
                return self.advanced_merger.merge_with_existing_tags(memory_merged)
            
            else:
                logger.error(f"未知的合并策略: {strategy}")
                return memory_merged
                
        except Exception as e:
            logger.error(f"标签合并失败: {str(e)}")
            return None
    
    def _memory_merge(self, tag_results: list) -> Optional[DataFrame]:
        """内存合并：将同一用户的多个标签合并"""
        try:
            from functools import reduce
            from pyspark.sql.functions import collect_list, struct
            
            # 合并所有标签结果
            all_tags = reduce(lambda df1, df2: df1.union(df2), tag_results)
            
            if all_tags.count() == 0:
                return None
            
            # 去重
            deduplicated = all_tags.dropDuplicates(["user_id", "tag_id"])
            
            # 丰富标签信息
            enriched = self._enrich_with_tag_info(deduplicated)
            
            # 按用户聚合
            aggregated = enriched.groupBy("user_id").agg(
                collect_list("tag_id").alias("tag_ids_raw"),
                collect_list(struct("tag_id", "tag_name", "tag_category")).alias("tag_info_list")
            )
            
            # 去重并格式化
            final_result = aggregated.select(
                "user_id",
                array_distinct("tag_ids_raw").alias("tag_ids"),
                "tag_info_list"
            )
            
            return self._format_output(final_result)
            
        except Exception as e:
            logger.error(f"内存合并失败: {str(e)}")
            return None
    
    def _enrich_with_tag_info(self, tags_df: DataFrame) -> DataFrame:
        """丰富标签信息"""
        try:
            tag_definitions = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="tag_definition",
                properties=self.mysql_config.connection_properties
            ).select("tag_id", "tag_name", "tag_category")
            
            return tags_df.join(
                tag_definitions, "tag_id", "left"
            ).select(
                "user_id", "tag_id", 
                col("tag_name"), col("tag_category"), "tag_detail"
            )
            
        except Exception as e:
            logger.error(f"丰富标签信息失败: {str(e)}")
            return tags_df.select(
                "user_id", "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def _format_output(self, user_tags_df: DataFrame) -> DataFrame:
        """格式化输出"""
        import json
        
        @udf(returnType=StringType())
        def build_tag_details(tag_info_list):
            if not tag_info_list:
                return "{}"
            
            tag_details = {}
            for tag_info in tag_info_list:
                tag_id = str(tag_info['tag_id'])
                tag_details[tag_id] = {
                    'tag_name': tag_info['tag_name'],
                    'tag_category': tag_info['tag_category']
                }
            return json.dumps(tag_details, ensure_ascii=False)
        
        return user_tags_df.select(
            col("user_id"),
            col("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details")
        )
    
    def cleanup(self):
        """清理资源"""
        self.advanced_merger.cleanup_cache()