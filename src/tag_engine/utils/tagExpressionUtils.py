#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签表达式构建工具
专门处理并行标签计算的表达式构建逻辑 - 使用模块级函数避免序列化问题
"""
from pyspark.sql.functions import *


def buildParallelTagExpression(tagConditions):
    """构建并行标签计算表达式 - TagGroup核心优化逻辑
    
    将多个标签条件组合成一个并行计算表达式，一次性评估所有标签
    在Driver端执行，返回Spark Column表达式，完全避免Python对象序列化
    
    Args:
        tagConditions: List[Dict] - 标签条件列表
            [{'tag_id': 1, 'condition': 'age >= 30'}, 
             {'tag_id': 2, 'condition': 'assets >= 10000'}, ...]
            
    Returns:
        Column - 并行标签数组表达式，可直接用于DataFrame.withColumn()
        
    Example:
        >>> conditions = [
        ...     {'tag_id': 1, 'condition': 'age >= 30'},
        ...     {'tag_id': 2, 'condition': 'assets >= 10000'}
        ... ]
        >>> expr = buildParallelTagExpression(conditions)
        >>> df.withColumn("tag_ids_array", expr)
    """
    if not tagConditions:
        return array()
    
    # 🚀 关键优化：构建SQL表达式字符串，使用filter高阶函数
    # 直接构建完整的SQL表达式，避免Column对象的复杂性
    
    # 构建case when表达式列表
    case_expressions = []
    for tagInfo in tagConditions:
        tag_id = tagInfo['tag_id']
        condition = tagInfo['condition']
        case_expressions.append(f"case when {condition} then {tag_id} else null end")
    
    # 构建完整的表达式：array + filter + sort + distinct
    sql_expr = f"""
    array_distinct(
        array_sort(
            filter(
                array({', '.join(case_expressions)}), 
                x -> x is not null
            )
        )
    )
    """.strip().replace('\n', ' ').replace('    ', ' ')
    
    return expr(sql_expr)