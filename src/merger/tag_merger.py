import logging
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, array_distinct, array_union, to_json, struct, map_from_arrays, lit, when, expr, size
from pyspark.sql.types import ArrayType, IntegerType
from functools import reduce
from datetime import date

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class TagMerger:
    """标签合并器 - 处理用户标签的合并和去重（正确实现：一个用户一条记录，包含标签ID数组）"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
    
    def merge_user_tags(self, new_tag_results: List[DataFrame]) -> Optional[DataFrame]:
        """合并多个标签计算结果到统一的用户标签表结构（一个用户一条记录，包含标签ID数组）"""
        try:
            if not new_tag_results:
                logger.warning("没有标签结果需要合并")
                return None
            
            logger.info(f"开始合并 {len(new_tag_results)} 个标签计算结果...")
            
            # 1. 合并所有新计算的标签结果
            all_new_tags = reduce(lambda df1, df2: df1.union(df2), new_tag_results)
            
            if all_new_tags.count() == 0:
                logger.warning("合并后没有标签数据")
                return None
            
            # 2. 首先从规则中获取标签名称和分类信息
            enriched_tags = self._enrich_with_tag_info(all_new_tags)
            
            # 3. 按用户聚合，将每个用户的所有标签聚合成数组
            user_new_tags = enriched_tags.groupBy("user_id").agg(
                collect_list("tag_id").alias("new_tag_ids"),
                collect_list(struct("tag_id", "tag_name", "tag_category")).alias("tag_info_list")
            )
            
            # 4. 读取现有用户标签（如果存在）
            existing_tags = self._read_existing_user_tags()
            
            # 5. 合并新老标签
            merged_result = self._merge_new_and_existing_tags(user_new_tags, existing_tags)
            
            logger.info(f"✅ 标签合并完成，影响 {merged_result.count()} 个用户")
            return merged_result
            
        except Exception as e:
            logger.error(f"标签合并失败: {str(e)}")
            return None
    
    def _read_existing_user_tags(self) -> Optional[DataFrame]:
        """读取现有的用户标签数据"""
        try:
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            if existing_df.count() == 0:
                logger.info("当前没有存储的用户标签")
                return None
            
            # 将JSON字符串转换回数组，以便在Spark中进行数组操作
            from pyspark.sql.functions import from_json
            from pyspark.sql.types import ArrayType, IntegerType
            
            processed_df = existing_df.select(
                "user_id",
                from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                "tag_details"
            )
            
            logger.info(f"读取到 {existing_df.count()} 条现有用户标签记录")
            return processed_df
            
        except Exception as e:
            logger.info(f"读取现有标签失败（可能是首次运行）: {str(e)}")
            return None
    
    def _merge_new_and_existing_tags(self, new_tags_df: DataFrame, existing_tags_df: Optional[DataFrame]) -> DataFrame:
        """合并新标签和已有标签"""
        try:
            if existing_tags_df is None:
                # 首次运行，直接使用新标签
                logger.info("首次运行，直接使用新计算的标签")
                return self._format_final_output(new_tags_df)
            
            # 合并新老标签
            logger.info("合并新标签和已有标签...")
            
            # 左连接：以新标签为主，关联已有标签
            merged_df = new_tags_df.join(
                existing_tags_df, 
                "user_id", 
                "left"
            )
            
            # 合并标签ID数组（去重）
            final_df = merged_df.select(
                col("user_id"),
                # 合并标签ID：新标签 + 现有标签，然后去重
                # 合并标签ID数组并去重
                self._merge_tag_arrays(col("tag_ids"), col("new_tag_ids")).alias("merged_tag_ids"),
                col("tag_info_list")
            )
            
            return self._format_final_output_with_merged_ids(final_df)
            
        except Exception as e:
            logger.error(f"合并新老标签失败: {str(e)}")
            raise
    
    def _format_final_output(self, user_tags_df: DataFrame) -> DataFrame:
        """格式化最终输出（首次运行）"""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        import json
        
        # 使用UDF简化处理
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
            col("new_tag_ids").alias("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
        
        return formatted_df
    
    def _format_final_output_with_merged_ids(self, merged_df: DataFrame) -> DataFrame:
        """格式化最终输出（包含合并的标签ID）"""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        import json
        
        # 使用UDF简化处理
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
        
        formatted_df = merged_df.select(
            col("user_id"),
            col("merged_tag_ids").alias("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
        
        return formatted_df
    
    def _merge_tag_arrays(self, existing_tags_col, new_tags_col):
        """合并两个标签数组并去重"""
        from pyspark.sql.functions import udf, array, flatten, array_distinct
        from pyspark.sql.types import ArrayType, IntegerType
        
        @udf(returnType=ArrayType(IntegerType()))
        def merge_arrays(existing_tags, new_tags):
            if existing_tags is None:
                existing_tags = []
            if new_tags is None:
                new_tags = []
            
            # 合并并去重
            merged = list(set(existing_tags + new_tags))
            return sorted(merged)
        
        return merge_arrays(existing_tags_col, new_tags_col)
    
    def _enrich_with_tag_info(self, tags_df: DataFrame) -> DataFrame:
        """用标签定义信息丰富标签数据"""
        try:
            # 读取标签定义
            tag_definitions = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="tag_definition",
                properties=self.mysql_config.connection_properties
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
            # 降级处理：使用默认值
            return tags_df.select(
                "user_id",
                "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def validate_merge_result(self, merged_df: DataFrame) -> bool:
        """验证合并结果的有效性"""
        try:
            if merged_df.count() == 0:
                logger.error("合并结果为空")
                return False
            
            # 检查必要字段
            required_fields = ["user_id", "tag_ids", "computed_date"]
            missing_fields = [field for field in required_fields if field not in merged_df.columns]
            
            if missing_fields:
                logger.error(f"合并结果缺少必要字段: {missing_fields}")
                return False
            
            # 检查用户ID不为空
            null_user_count = merged_df.filter(col("user_id").isNull()).count()
            if null_user_count > 0:
                logger.error(f"存在 {null_user_count} 个空的用户ID")
                return False
            
            # 检查标签数组不为空
            empty_tags_count = merged_df.filter(
                col("tag_ids").isNull() | (expr("size(tag_ids)") == 0)
            ).count()
            
            if empty_tags_count > 0:
                logger.warning(f"存在 {empty_tags_count} 个用户没有标签")
            
            logger.info("✅ 合并结果验证通过")
            return True
            
        except Exception as e:
            logger.error(f"合并结果验证失败: {str(e)}")
            return False
    
    def get_merge_statistics(self, merged_df: DataFrame) -> dict:
        """获取合并统计信息（适配新的数据模型）"""
        try:
            total_users = merged_df.count()
            
            if total_users == 0:
                return {
                    "total_users": 0,
                    "total_tag_assignments": 0,
                    "avg_tags_per_user": 0,
                    "max_tags_per_user": 0,
                    "min_tags_per_user": 0
                }
            
            # 统计每个用户的标签数
            stats_df = merged_df.select(
                col("user_id"),
                size(col("tag_ids")).alias("tag_count")
            )
            
            tag_counts = stats_df.select("tag_count").collect()
            tag_count_values = [row['tag_count'] for row in tag_counts]
            
            total_assignments = sum(tag_count_values)
            
            stats = {
                "total_users": total_users,
                "total_tag_assignments": total_assignments,
                "avg_tags_per_user": round(total_assignments / total_users, 2) if total_users > 0 else 0,
                "max_tags_per_user": max(tag_count_values) if tag_count_values else 0,
                "min_tags_per_user": min(tag_count_values) if tag_count_values else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"获取合并统计失败: {str(e)}")
            return {}
    
    def optimize_merge_performance(self, df: DataFrame) -> DataFrame:
        """优化合并性能"""
        # 缓存中间结果
        df = df.cache()
        
        # 重分区优化
        if df.rdd.getNumPartitions() > 50:
            df = df.coalesce(50)
        
        return df