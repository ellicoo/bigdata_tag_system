import logging
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, array_union, array_distinct, when, lit
from pyspark.sql.types import ArrayType, IntegerType
from datetime import date

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class AdvancedTagMerger:
    """高级标签合并器 - 支持内存合并和数据库合并的公共组件"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
    
    def merge_with_existing_tags(self, new_tags_df: DataFrame, cached_existing_tags: DataFrame = None) -> Optional[DataFrame]:
        """
        与MySQL中现有标签合并
        
        Args:
            new_tags_df: 新计算的标签DataFrame (user_id, tag_ids, tag_details, computed_date)
            
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
            
            # 4. 合并标签数组 - 修复列引用问题
            final_merged = merged_df.select(
                col("user_id"),
                self._merge_tag_arrays(
                    col("existing.tag_ids"), 
                    col("new.tag_ids")
                ).alias("tag_ids"),
                col("new.tag_details"),
                col("new.computed_date")
            )
            
            # 5. 调试信息：显示合并前后的数据
            logger.info("合并前新标签样例:")
            new_tags_df.show(3, truncate=False)
            
            logger.info("合并前现有标签样例:")
            existing_tags.show(3, truncate=False)
            
            logger.info("合并后标签样例:")
            final_merged.show(3, truncate=False)
            
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
            
            # 将JSON字符串转换为数组类型
            processed_df = existing_df.select(
                "user_id",
                from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                "tag_details"
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
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, IntegerType
        
        @udf(returnType=ArrayType(IntegerType()))
        def merge_arrays(existing_tags, new_tags):
            if existing_tags is None:
                existing_tags = []
            if new_tags is None:
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
    """统一标签合并器 - 根据场景选择合并策略"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        self.advanced_merger = AdvancedTagMerger(spark, mysql_config)
    
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
            from pyspark.sql.functions import collect_list, array_distinct, struct
            
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
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
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
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
    
    def cleanup(self):
        """清理资源"""
        self.advanced_merger.cleanup_cache()