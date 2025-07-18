import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, array_distinct, struct, lit
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from datetime import date, datetime
import json

from .rule_parser import RuleConditionParser

logger = logging.getLogger(__name__)


class ParallelTagEngine:
    """并行标签计算引擎 - 支持多标签并行计算和内存合并"""
    
    def __init__(self, spark: SparkSession, max_workers: int = 4, mysql_config=None):
        self.spark = spark
        self.max_workers = max_workers
        self.mysql_config = mysql_config
        self.rule_parser = RuleConditionParser()
    
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
            tag_results = self._compute_tags_parallel(data_df, rules)
            
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
    
    def _compute_tags_parallel(self, data_df: DataFrame, rules: List[Dict[str, Any]]) -> List[DataFrame]:
        """
        并行计算多个标签 - 利用Spark原生分布式并行能力
        
        Args:
            data_df: 业务数据DataFrame
            rules: 标签规则列表
            
        Returns:
            标签结果DataFrame列表
        """
        logger.info(f"🚀 开始Spark分布式并行计算 {len(rules)} 个标签")
        
        # 智能缓存策略：只有规则数量较多时才缓存
        should_cache = len(rules) > 3
        if should_cache:
            logger.info("📦 缓存数据以提升多标签计算性能")
            cached_data = data_df.cache()
            # 触发缓存 - 使用轻量级操作
            _ = cached_data.count()
        else:
            cached_data = data_df
        
        results = []
        failed_tags = []
        
        # 直接使用Spark的分布式计算，无需Python线程池
        for rule in rules:
            try:
                logger.info(f"🔄 计算标签: {rule['tag_name']}")
                result_df = self._compute_single_tag(cached_data, rule)
                
                if result_df is not None:
                    results.append(result_df)
                    logger.info(f"✅ 标签 {rule['tag_name']} 计算完成")
                else:
                    failed_tags.append(rule['tag_name'])
                    logger.warning(f"⚠️ 标签 {rule['tag_name']} 无命中用户")
                    
            except Exception as e:
                logger.error(f"❌ 标签 {rule['tag_name']} 计算失败: {str(e)}")
                failed_tags.append(rule['tag_name'])
        
        # 清理缓存
        if should_cache:
            logger.info("🧹 清理数据缓存")
            cached_data.unpersist()
        
        logger.info(f"🎉 Spark分布式计算完成 - 成功: {len(results)}, 失败: {len(failed_tags)}")
        if failed_tags:
            logger.warning(f"失败的标签: {failed_tags}")
        
        return results
    
    def _compute_single_tag(self, data_df: DataFrame, rule: Dict[str, Any]) -> Optional[DataFrame]:
        """
        计算单个标签
        
        Args:
            data_df: 业务数据DataFrame
            rule: 标签规则字典
            
        Returns:
            包含user_id, tag_id, tag_detail的DataFrame
        """
        try:
            tag_id = rule['tag_id']
            tag_name = rule['tag_name']
            rule_conditions = rule['rule_conditions']
            
            logger.debug(f"开始计算标签: {tag_name} (ID: {tag_id})")
            
            # 解析规则条件
            condition_sql = self.rule_parser.parse_rule_conditions(rule_conditions)
            logger.debug(f"生成的SQL条件: {condition_sql}")
            
            # 获取命中条件需要的字段
            hit_fields = self.rule_parser.get_condition_fields(rule_conditions)
            
            # 执行标签计算 - 筛选符合条件的用户
            tagged_users = data_df.filter(condition_sql)
            
            if tagged_users.count() == 0:
                logger.debug(f"标签 {tag_name} 没有命中任何用户")
                return None
            
            # 选择需要的字段
            select_fields = ['user_id'] + [f for f in hit_fields if f in data_df.columns]
            result_df = tagged_users.select(*select_fields)
            
            # 添加标签ID
            result_df = result_df.withColumn('tag_id', F.lit(tag_id))
            
            # 生成标签详细信息
            result_df = self._add_tag_details(result_df, rule, hit_fields)
            
            hit_count = result_df.count()
            logger.debug(f"✅ 标签 {tag_name} 计算完成，命中用户数: {hit_count}")
            
            return result_df.select('user_id', 'tag_id', 'tag_detail')
            
        except Exception as e:
            logger.error(f"❌ 计算标签失败: {rule.get('tag_name', 'Unknown')}, 错误: {str(e)}")
            return None
    
    def _add_tag_details(self, result_df: DataFrame, rule: Dict[str, Any], hit_fields: List[str]) -> DataFrame:
        """为标签结果添加详细信息"""
        
        # 复制需要的数据避免序列化整个对象
        tag_name = rule['tag_name']
        
        @F.udf(returnType=StringType())
        def generate_tag_detail(*hit_values):
            """生成标签详细信息的UDF"""
            try:
                # 简化的命中原因生成
                reason = f"满足标签规则: {tag_name}"
                
                # 构建标签详细信息
                detail = {
                    'value': str(hit_values[0]) if hit_values and hit_values[0] is not None else "",
                    'reason': reason,
                    'source': 'AUTO',
                    'hit_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'rule_version': '1.0',
                    'tag_name': tag_name
                }
                
                return json.dumps(detail, ensure_ascii=False)
                
            except Exception as e:
                return json.dumps({'error': str(e)})
        
        # 获取用于生成详情的字段列
        detail_columns = []
        for field in hit_fields:
            if field in result_df.columns:
                detail_columns.append(F.col(field))
        
        # 如果没有可用字段，使用空值
        if not detail_columns:
            detail_columns = [F.lit(None)]
        
        # 添加标签详情列
        result_df = result_df.withColumn('tag_detail', generate_tag_detail(*detail_columns))
        
        return result_df