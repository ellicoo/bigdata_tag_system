#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算组
将相同表依赖的标签归为一组，实现并行高效计算
"""
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from ..parser.TagRuleParser import TagRuleParser


class TagGroup:
    """标签计算组
    
    职责：
    1. 管理一组具有相同表依赖的标签
    2. 执行该组标签的并行计算
    3. 优化表读取和JOIN操作
    4. 使用Spark原生函数进行标签合并
    """
    
    def __init__(self, tagIds: List[int], requiredTables: List[str]):
        """初始化标签计算组
        
        Args:
            tagIds: 标签ID列表
            requiredTables: 所需的表名列表
        """
        self.tagIds = tagIds
        self.requiredTables = requiredTables
        self.name = f"Group_{len(tagIds)}tags_{len(requiredTables)}tables"
        
        print(f"📦 创建标签组: {self.name}")
        print(f"   🏷️  标签: {tagIds}")
        print(f"   📊 依赖表: {requiredTables}")
    
    def computeTags(self, hiveMeta, rulesDF: DataFrame) -> DataFrame:
        """计算该组所有标签 - 共享组内表内存，并行计算后直接聚合
        
        Args:
            hiveMeta: Hive数据源管理器
            rulesDF: 标签规则DataFrame
            
        Returns:
            DataFrame: 标签计算结果，包含 user_id, tag_ids_array 字段
        """
        print(f"🚀 开始计算标签组: {self.name}")
        
        try:
            # 1. 过滤该组相关的标签规则
            groupRulesDF = rulesDF.filter(col("tag_id").isin(self.tagIds))
            print(f"   📋 该组标签规则数: {groupRulesDF.count()}")
            
            # 2. 分析字段依赖
            fieldDependencies = self._analyzeFieldDependencies(groupRulesDF)
            
            # 3. 🚀 关键优化：一次性加载并JOIN所需的Hive表（组内共享）
            joinedDF = hiveMeta.loadAndJoinTables(self.requiredTables, fieldDependencies)
            print(f"   🔗 组内共享表JOIN完成，用户数: {joinedDF.count()}")
            
            # 4. 🚀 关键优化：为该组并行计算所有标签，直接返回聚合结果
            userTagsDF = self._computeAllTagsParallelAndAggregate(joinedDF, groupRulesDF)
            
            print(f"✅ 标签组计算完成: {userTagsDF.count()} 个用户")
            return userTagsDF
            
        except Exception as e:
            print(f"❌ 标签组计算失败: {e}")
            import traceback
            traceback.print_exc()
            return self._createEmptyResult(hiveMeta.spark)
    
    def _analyzeFieldDependencies(self, rulesDF: DataFrame) -> Dict[str, List[str]]:
        """分析该组标签的字段依赖关系"""
        parser = TagRuleParser()
        fieldDependencies = parser.analyzeFieldDependencies(rulesDF)
        
        return fieldDependencies
    
    def _computeAllTagsParallelAndAggregate(self, joinedDF: DataFrame, groupRulesDF: DataFrame) -> DataFrame:
        """并行计算该组所有标签并直接聚合 - 一步到位的优化方案"""
        print(f"   🎯 并行计算并聚合 {len(self.tagIds)} 个标签...")
        
        # 收集规则到Driver进行SQL条件解析
        rules = groupRulesDF.select("tag_id", "rule_conditions").collect()
        
        # 解析所有规则为SQL条件
        parser = TagRuleParser()
        
        # 构建所有标签的并行计算表达式
        tag_conditions = []
        
        for row in rules:
            tagId = row['tag_id']
            ruleConditions = row['rule_conditions']
            
            print(f"      🏷️  解析标签 {tagId} 规则...")
            
            sqlCondition = parser.parseRuleToSql(ruleConditions, self.requiredTables)
            print(f"         🔍 标签 {tagId} SQL条件: {sqlCondition}")
            
            # 为每个标签构建条件表达式
            if sqlCondition and sqlCondition.strip() and sqlCondition != "1=0":
                # 有效规则：构建when表达式
                tag_conditions.append({
                    'tag_id': tagId,
                    'condition': sqlCondition
                })
            else:
                print(f"         ⚠️  标签 {tagId} 无有效规则条件，跳过")
        
        if not tag_conditions:
            print("         ⚠️  没有有效的标签规则")
            return self._createEmptyResult(joinedDF.sparkSession)
        
        # 🚀 关键改进：使用独立工具模块构建并行表达式
        print(f"   ⚡ 使用并行表达式工具构建 {len(tag_conditions)} 个标签条件...")
        
        # 使用独立工具模块构建并行标签表达式
        from ..utils.tagExpressionUtils import buildParallelTagExpression
        combined_tags_expr = buildParallelTagExpression(tag_conditions)
        
        # 一次性为所有用户计算其匹配的标签数组，并过滤掉空数组用户
        # 🔧 关键修复：先计算标签，再选择需要的字段，避免过早丢弃业务字段
        userTagsDF = joinedDF.withColumn("tag_ids_array", combined_tags_expr) \
                           .select("user_id", "tag_ids_array") \
                           .filter(size(col("tag_ids_array")) > 0)
        
        # 统计结果
        try:
            userCount = userTagsDF.count()
            print(f"   ✅ 并行计算并聚合完成: {userCount} 个有标签用户")
        except:
            print(f"   ✅ 并行计算并聚合完成")
        
        return userTagsDF
    
    def _createEmptyResult(self, spark) -> DataFrame:
        """创建空的计算结果"""
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("tag_ids_array", ArrayType(IntegerType()), True)
        ])
        
        return spark.createDataFrame([], schema)
    
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"TagGroup(tags={self.tagIds}, tables={self.requiredTables})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()