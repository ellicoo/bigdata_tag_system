import logging
from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, array_distinct, struct, lit
from datetime import date
import json

from .tag_computer import TagComputeEngine

logger = logging.getLogger(__name__)


class ParallelTagEngine:
    """并行标签计算引擎 - 支持多标签并行计算和内存合并"""
    
    def __init__(self, spark: SparkSession, max_workers: int = 4, mysql_config=None):
        self.spark = spark
        self.max_workers = max_workers
        self.mysql_config = mysql_config
        self.tag_engine = TagComputeEngine(spark, max_workers)
    
    def compute_tags_with_memory_merge(self, data_df: DataFrame, rules: List[Dict[str, Any]]) -> Optional[DataFrame]:
        """
        多标签并行计算 + 内存合并
        
        Args:
            data_df: 用户数据
            rules: 标签规则列表
            
        Returns:
            内存合并后的用户标签DataFrame (user_id, tag_ids, tag_details, computed_date)
        """
        try:
            logger.info(f"🚀 开始多标签并行计算，共 {len(rules)} 个标签")
            
            # 1. 并行计算所有标签
            tag_results = self.tag_engine.compute_tags_parallel(data_df, rules)
            
            if not tag_results:
                logger.warning("没有任何标签计算出结果")
                return None
            
            # 2. 内存合并：同一用户的多个标签合并为一条记录
            merged_result = self._merge_user_tags_in_memory(tag_results)
            
            logger.info(f"✅ 多标签并行计算和内存合并完成")
            return merged_result
            
        except Exception as e:
            logger.error(f"多标签并行计算失败: {str(e)}")
            return None
    
    def _merge_user_tags_in_memory(self, tag_results: List[DataFrame]) -> Optional[DataFrame]:
        """内存合并：将同一用户的多个标签合并为一条记录"""
        try:
            if not tag_results:
                return None
                
            logger.info(f"开始内存合并 {len(tag_results)} 个标签结果...")
            
            from functools import reduce
            
            # 1. 合并所有标签结果
            all_tags = reduce(lambda df1, df2: df1.union(df2), tag_results)
            
            if all_tags.count() == 0:
                logger.warning("合并后没有标签数据")
                return None
            
            # 2. 去重：移除同一用户的重复标签
            deduplicated_tags = all_tags.dropDuplicates(["user_id", "tag_id"])
            
            # 3. 丰富标签信息
            enriched_tags = self._enrich_with_tag_info(deduplicated_tags)
            
            # 4. 按用户聚合：将用户的多个标签合并为数组
            user_aggregated = enriched_tags.groupBy("user_id").agg(
                collect_list("tag_id").alias("tag_ids_raw"),
                collect_list(struct("tag_id", "tag_name", "tag_category")).alias("tag_info_list")
            )
            
            # 5. 确保标签数组去重
            user_aggregated = user_aggregated.select(
                "user_id",
                array_distinct("tag_ids_raw").alias("tag_ids"),
                "tag_info_list"
            )
            
            # 6. 格式化输出
            final_result = self._format_memory_merge_output(user_aggregated)
            
            logger.info(f"✅ 内存合并完成，影响 {final_result.count()} 个用户")
            return final_result
            
        except Exception as e:
            logger.error(f"内存合并失败: {str(e)}")
            return None
    
    def _enrich_with_tag_info(self, tags_df: DataFrame) -> DataFrame:
        """丰富标签信息"""
        try:
            # 使用传递的配置或从配置管理器获取
            if self.mysql_config:
                mysql_config = self.mysql_config
            else:
                from src.config.manager import ConfigManager
                config = ConfigManager.load_config('local')
                mysql_config = config.mysql
            
            # 读取标签定义
            tag_definitions = self.spark.read.jdbc(
                url=mysql_config.jdbc_url,
                table="tag_definition",
                properties=mysql_config.connection_properties
            ).select("tag_id", "tag_name", "tag_category")
            
            # 关联标签定义信息
            enriched_df = tags_df.join(
                tag_definitions,
                "tag_id",
                "left"
            ).select(
                "user_id",
                "tag_id", 
                col("tag_name").alias("tag_name"),
                col("tag_category").alias("tag_category"),
                "tag_detail"
            )
            
            return enriched_df
            
        except Exception as e:
            logger.error(f"丰富标签信息失败: {str(e)}")
            # 降级处理
            return tags_df.select(
                "user_id",
                "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def _format_memory_merge_output(self, user_tags_df: DataFrame) -> DataFrame:
        """格式化内存合并输出"""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
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
        
        formatted_df = user_tags_df.select(
            col("user_id"),
            col("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
        
        return formatted_df