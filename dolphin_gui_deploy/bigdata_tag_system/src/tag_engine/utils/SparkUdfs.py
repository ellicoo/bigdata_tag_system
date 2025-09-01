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
    """新标签与MySQL现有标签合并 - 传统合并模式（只增不减）
    
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


def replace_computed_tags(computed_tags_col, existing_tags_col, computed_tag_scope_col):
    """基于本次计算范围智能替换用户标签 - 支持标签移除
    
    核心逻辑：
    1. 保留现有标签中不在本次计算范围内的标签（不受影响的标签）
    2. 用本次计算结果替换范围内的标签（支持新增、移除、保持）
    
    业务场景：
    - 现有标签: [1,2,3,4,5]
    - 本次计算范围: [2,3,6] 
    - 本次计算结果: [3,6] (用户匹配了3,6标签，不匹配2标签)
    - 最终结果: [1,4,5] + [3,6] = [1,3,4,5,6]
    
    Args:
        computed_tags_col: Column - 本次计算得到的标签数组
        existing_tags_col: Column - 用户现有的标签数组
        computed_tag_scope_col: Column - 本次计算涉及的所有标签范围数组
        
    Returns:
        Column - 智能替换后的最终标签数组
    """
    # 处理空值
    computed_tags = coalesce(computed_tags_col, array())
    existing_tags = coalesce(existing_tags_col, array())
    computed_scope = coalesce(computed_tag_scope_col, array())
    
    # 从现有标签中排除本次计算范围内的标签
    unaffected_tags = array_except(existing_tags, computed_scope)
    
    # 合并不受影响的标签 + 本次计算结果
    return array_distinct(
        array_sort(
            array_union(unaffected_tags, computed_tags)
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


def determine_timestamp(new_tags_col, existing_tags_col, existing_created_time_col, existing_updated_time_col):
    """智能确定时间戳 - 标签有变化时更新updated_time，否则保持原值
    
    核心业务逻辑：
    1. 如果新标签与现有标签不同，更新updated_time为当前时间
    2. 如果相同，保持原有的updated_time不变
    3. created_time始终保持最初创建时间，新用户使用当前时间
    
    Args:
        new_tags_col: Column - 新计算的标签数组
        existing_tags_col: Column - 现有标签数组  
        existing_created_time_col: Column - 现有创建时间
        existing_updated_time_col: Column - 现有更新时间
        
    Returns:
        Struct[created_time: Timestamp, updated_time: Timestamp] - 时间戳结构
    """
    # 处理null值
    new_tags = coalesce(new_tags_col, array())
    existing_tags = coalesce(existing_tags_col, array())
    
    # 标签是否发生变化：比较排序后的数组转换为字符串进行比较
    new_tags_sorted_str = to_json(array_sort(new_tags))
    existing_tags_sorted_str = to_json(array_sort(existing_tags))
    tags_changed = new_tags_sorted_str != existing_tags_sorted_str
    
    # 确定创建时间：新用户使用当前时间，老用户保持原值
    final_created_time = coalesce(existing_created_time_col, current_timestamp())
    
    # 确定更新时间：标签变化则更新，否则保持原值
    final_updated_time = when(
        tags_changed, 
        current_timestamp()
    ).otherwise(
        coalesce(existing_updated_time_col, current_timestamp())
    )
    
    # 返回时间戳结构
    return struct(
        final_created_time.alias("created_time"),
        final_updated_time.alias("updated_time")
    )


