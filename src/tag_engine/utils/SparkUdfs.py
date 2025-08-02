#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark UDF函数集合 - 模块级函数避免序列化问题

 1. Driver端执行 (DolphinScheduler节点)
# 这些都在Driver端执行，不会有版本冲突
class TagEngine:           # Python类实例化
class TagGroup:           # Python类实例化
class HiveMeta:           # Python类实例化
class MysqlMeta:          # Python类实例化
class TagRuleParser:      # Python类实例化
# 编排逻辑都在Driver端
tagEngine = TagEngine(spark, hiveConfig, mysqlConfig)
tagEngine.computeTags()

2. Executor端执行 (YARN集群各节点)

# ❌ 这些会被序列化到Executor端，触发版本检查
@udf(returnType=ArrayType(IntegerType()))
def mergeUserTags(tagList):     # Python UDF函数
  # 这个函数会在Worker节点执行
  一旦设计成类，就会触发版本冲突

# ✅ 这些是Spark原生表达式，不涉及Python序列化
df.withColumn("tags", array_distinct(array_sort(col("tags"))))


所以：使用模块级函数而非类实例，避免Python对象序列化导致的版本不匹配
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *



def merge_with_existing_tags(new_tags_col, existing_tags_col):
    """新标签与MySQL现有标签合并
    
    使用Spark原生函数：array_union + array_distinct + array_sort
    
    Args:
        new_tags_col: Column - 新标签数组列
        existing_tags_col: Column - 现有标签数组列
        
    Returns:
        Column - 合并去重排序后的标签数组
    """
    # 处理空值情况
    new_tags = coalesce(new_tags_col, array())
    existing_tags = coalesce(existing_tags_col, array())
    
    # 使用Spark原生函数合并数组
    return array_distinct(
        array_sort(
            array_union(new_tags, existing_tags)
        )
    )


def array_to_json(array_col):
    """将数组转换为JSON字符串
    
    Args:
        array_col: Column - 数组列
        
    Returns:
        Column - JSON字符串列
    """
    return to_json(coalesce(array_col, array()))


def json_to_array(json_col):
    """将JSON字符串转换为数组
    
    Args:
        json_col: Column - JSON字符串列
        
    Returns:
        Column - 数组列
    """
    array_schema = ArrayType(IntegerType())
    return coalesce(from_json(json_col, array_schema), array())


