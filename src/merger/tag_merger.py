import json
import logging
from functools import reduce
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class TagMerger:
    """标签合并器 - 处理用户标签的合并逻辑"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig, storage_type: str = "json"):
        self.spark = spark
        self.mysql_config = mysql_config
        self.storage_type = storage_type.lower()  # json 或 text
    
    def merge_user_tags(self, new_tag_results: List[DataFrame]) -> Optional[DataFrame]:
        """
        合并用户标签
        
        Args:
            new_tag_results: 新计算的标签结果列表
            
        Returns:
            合并后的用户标签DataFrame
        """
        try:
            if not new_tag_results:
                logger.info("没有新的标签结果需要合并")
                return None
            
            # 1. 读取现有标签
            existing_tags_df = self._read_existing_tags()
            
            # 2. 聚合新标签
            aggregated_new_tags = self._aggregate_new_tags(new_tag_results)
            
            if aggregated_new_tags is None:
                logger.info("新标签聚合结果为空")
                return existing_tags_df
            
            # 3. 合并新旧标签
            merged_df = self._merge_tags_logic(existing_tags_df, aggregated_new_tags)
            
            logger.info(f"标签合并完成，最终用户数: {merged_df.count()}")
            return merged_df
            
        except Exception as e:
            logger.error(f"标签合并失败: {str(e)}")
            raise
    
    def _read_existing_tags(self) -> DataFrame:
        """读取现有的用户标签"""
        try:
            existing_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            logger.info(f"读取现有标签，用户数: {existing_df.count()}")
            return existing_df
            
        except Exception as e:
            logger.warning(f"读取现有标签失败，可能是首次运行: {str(e)}")
            # 返回空的DataFrame
            return self.spark.createDataFrame([], "user_id string, tag_ids string, tag_details string")
    
    def _aggregate_new_tags(self, tag_results: List[DataFrame]) -> Optional[DataFrame]:
        """聚合新计算的标签结果"""
        try:
            if not tag_results:
                return None
            
            # 合并所有标签结果
            all_new_tags = reduce(lambda df1, df2: df1.union(df2), tag_results)
            
            # 按用户聚合标签
            aggregated = all_new_tags.groupBy('user_id').agg(
                F.collect_list('tag_id').alias('new_tag_ids'),
                F.collect_list('tag_detail').alias('new_tag_details')
            )
            
            logger.info(f"新标签聚合完成，涉及用户数: {aggregated.count()}")
            return aggregated
            
        except Exception as e:
            logger.error(f"新标签聚合失败: {str(e)}")
            return None
    
    def _merge_tags_logic(self, existing_df: DataFrame, new_df: DataFrame) -> DataFrame:
        """执行标签合并逻辑"""
        
        if self.storage_type == "json":
            return self._merge_json_tags(existing_df, new_df)
        else:
            return self._merge_text_tags(existing_df, new_df)
    
    def _merge_json_tags(self, existing_df: DataFrame, new_df: DataFrame) -> DataFrame:
        """JSON格式的标签合并"""
        
        @F.udf(returnType=StringType())
        def merge_tag_arrays(existing_tags, new_tag_ids):
            """合并标签数组 - JSON格式"""
            try:
                # 处理现有标签
                if existing_tags:
                    try:
                        existing_list = json.loads(existing_tags)
                        if not isinstance(existing_list, list):
                            existing_list = []
                    except (json.JSONDecodeError, TypeError):
                        existing_list = []
                else:
                    existing_list = []
                
                # 合并新标签
                if new_tag_ids:
                    # 转换为set去重，再转回list
                    tag_set = set(existing_list + new_tag_ids)
                    return json.dumps(sorted(list(tag_set)))
                else:
                    return json.dumps(existing_list)
                    
            except Exception as e:
                logger.error(f"JSON标签合并失败: {str(e)}")
                return json.dumps(new_tag_ids or [])
        
        @F.udf(returnType=StringType())
        def merge_tag_details(existing_details, new_details_list):
            """合并标签详情 - JSON格式"""
            try:
                # 处理现有详情
                if existing_details:
                    try:
                        existing_dict = json.loads(existing_details)
                        if not isinstance(existing_dict, dict):
                            existing_dict = {}
                    except (json.JSONDecodeError, TypeError):
                        existing_dict = {}
                else:
                    existing_dict = {}
                
                # 合并新详情
                if new_details_list:
                    for detail_str in new_details_list:
                        try:
                            detail_dict = json.loads(detail_str)
                            existing_dict.update(detail_dict)
                        except (json.JSONDecodeError, TypeError):
                            continue
                
                return json.dumps(existing_dict, ensure_ascii=False)
                
            except Exception as e:
                logger.error(f"JSON详情合并失败: {str(e)}")
                return existing_details or "{}"
        
        # 执行合并
        merged_df = new_df.alias("new").join(
            existing_df.alias("existing"),
            F.col("new.user_id") == F.col("existing.user_id"),
            "full_outer"
        ).select(
            F.coalesce(F.col("new.user_id"), F.col("existing.user_id")).alias("user_id"),
            merge_tag_arrays(
                F.col("existing.tag_ids"),
                F.col("new.new_tag_ids")
            ).alias("tag_ids"),
            merge_tag_details(
                F.col("existing.tag_details"),
                F.col("new.new_tag_details")
            ).alias("tag_details"),
            F.current_timestamp().alias("updated_time")
        )
        
        return merged_df
    
    def _merge_text_tags(self, existing_df: DataFrame, new_df: DataFrame) -> DataFrame:
        """TEXT格式的标签合并（兼容老版本MySQL）"""
        
        @F.udf(returnType=StringType())
        def merge_tag_text(existing_tags, new_tag_ids):
            """合并标签 - TEXT格式"""
            try:
                # 处理现有标签
                if existing_tags:
                    existing_list = [t.strip() for t in existing_tags.split(',') if t.strip()]
                else:
                    existing_list = []
                
                # 合并新标签
                if new_tag_ids:
                    new_list = [str(tag_id) for tag_id in new_tag_ids]
                    # 去重并排序
                    tag_set = set(existing_list + new_list)
                    return ','.join(sorted(tag_set))
                else:
                    return ','.join(existing_list)
                    
            except Exception as e:
                logger.error(f"TEXT标签合并失败: {str(e)}")
                return ','.join(str(tag_id) for tag_id in (new_tag_ids or []))
        
        @F.udf(returnType=StringType())
        def merge_details_text(existing_details, new_details_list):
            """合并标签详情 - TEXT格式"""
            try:
                # 处理现有详情
                if existing_details:
                    try:
                        existing_dict = json.loads(existing_details)
                        if not isinstance(existing_dict, dict):
                            existing_dict = {}
                    except (json.JSONDecodeError, TypeError):
                        existing_dict = {}
                else:
                    existing_dict = {}
                
                # 合并新详情
                if new_details_list:
                    for detail_str in new_details_list:
                        try:
                            detail_dict = json.loads(detail_str)
                            existing_dict.update(detail_dict)
                        except (json.JSONDecodeError, TypeError):
                            continue
                
                return json.dumps(existing_dict, ensure_ascii=False)
                
            except Exception as e:
                logger.error(f"TEXT详情合并失败: {str(e)}")
                return existing_details or "{}"
        
        # 执行合并
        merged_df = new_df.alias("new").join(
            existing_df.alias("existing"),
            F.col("new.user_id") == F.col("existing.user_id"),
            "full_outer"
        ).select(
            F.coalesce(F.col("new.user_id"), F.col("existing.user_id")).alias("user_id"),
            merge_tag_text(
                F.col("existing.tag_ids"),
                F.col("new.new_tag_ids")
            ).alias("tag_ids"),
            merge_details_text(
                F.col("existing.tag_details"),
                F.col("new.new_tag_details")
            ).alias("tag_details"),
            F.current_timestamp().alias("updated_time")
        )
        
        return merged_df
    
    def validate_merge_result(self, merged_df: DataFrame) -> bool:
        """验证合并结果"""
        try:
            # 检查基本字段
            required_columns = ['user_id', 'tag_ids', 'tag_details']
            for col in required_columns:
                if col not in merged_df.columns:
                    logger.error(f"合并结果缺少字段: {col}")
                    return False
            
            # 检查数据量
            total_count = merged_df.count()
            if total_count == 0:
                logger.warning("合并结果为空")
                return False
            
            # 检查user_id唯一性
            distinct_count = merged_df.select('user_id').distinct().count()
            if total_count != distinct_count:
                logger.error(f"合并结果存在重复user_id，总数: {total_count}，去重后: {distinct_count}")
                return False
            
            # 检查空值
            null_user_count = merged_df.filter(F.col('user_id').isNull()).count()
            if null_user_count > 0:
                logger.error(f"合并结果存在空user_id，数量: {null_user_count}")
                return False
            
            logger.info(f"合并结果验证通过，用户数: {total_count}")
            return True
            
        except Exception as e:
            logger.error(f"合并结果验证失败: {str(e)}")
            return False