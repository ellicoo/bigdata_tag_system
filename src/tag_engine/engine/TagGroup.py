#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算组
将相同表依赖的标签归为一组，实现并行高效计算
"""
from typing import List, Dict, Tuple
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
    
    def computeTags(self, hiveMeta, rulesDF: DataFrame) -> Tuple[DataFrame, List[int]]:
        """计算该组所有标签 - 共享组内表内存，并行计算后直接聚合，跟踪失败标签
        
        Args:
            hiveMeta: Hive数据源管理器
            rulesDF: 标签规则DataFrame
            
        Returns:
            Tuple[DataFrame, List[int]]: (标签计算结果DataFrame, 失败的标签ID列表)
        """
        print(f"🚀 开始计算标签组: {self.name}")
        
        try:
            # 1. 过滤该组相关的标签规则
            groupRulesDF = rulesDF.filter(col("tag_id").isin(self.tagIds))
            print(f"   📋 该组标签规则数: {groupRulesDF.count()}")
            
            # 2. 分析字段依赖
            fieldDependencies = self._analyzeFieldDependencies(groupRulesDF)
            
            # 3. 构建标签ID到依赖表的映射
            tagTableMapping = self._buildTagTableMapping(groupRulesDF)
            
            # 4. 🚀 关键改进：使用容错加载方法
            joinedDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
                self.requiredTables, tagTableMapping, fieldDependencies)
            
            # 获取成功加载的表列表
            successful_tables = hiveMeta.getSuccessfulTables()

            if failed_tag_ids:
                print(f"   ⚠️  表加载失败导致 {len(failed_tag_ids)} 个标签失败: {failed_tag_ids}")
                print(f"   📋 成功加载的表: {successful_tables}")
                print(f"   ❌ {hiveMeta.getFailureSummary()}")
                
                # 更新可计算的标签列表
                available_tag_ids = [tag_id for tag_id in self.tagIds if tag_id not in failed_tag_ids]
                if not available_tag_ids:
                    print(f"   ❌ 所有标签都因表加载失败而无法计算")
                    return self._createEmptyResult(hiveMeta.spark), failed_tag_ids
                print(f"   ✅ 可计算的标签: {available_tag_ids}")
            else:
                available_tag_ids = self.tagIds
                successful_tables = self.requiredTables
                print(f"   ✅ 所有表加载成功")
            
            print(f"   🔗 组内共享表JOIN完成，用户数: {joinedDF.count()}")
            
            # 5. 🚀 关键优化：为可用标签并行计算，使用成功的表列表生成SQL
            # 只计算没有失败的标签，并且只使用成功加载的表生成SQL条件
            available_rules_df = groupRulesDF.filter(col("tag_id").isin(available_tag_ids))
            userTagsDF = self._computeAllTagsParallelAndAggregate(joinedDF, available_rules_df, successful_tables)
            
            failed_display = failed_tag_ids if failed_tag_ids else "[None]"
            successful_tag_ids = [tag_id for tag_id in self.tagIds if tag_id not in failed_tag_ids]
            successful_display = successful_tag_ids if successful_tag_ids else "[None]"
            print(f"✅ 标签组计算完成: {userTagsDF.count()} 个用户，成功标签: {successful_display}，失败标签: {failed_display}")
            return userTagsDF, failed_tag_ids
            
        except Exception as e:
            print(f"❌ 标签组计算失败: {e}")
            import traceback
            traceback.print_exc()
            # 如果整个组计算失败，所有标签都标记为失败
            return self._createEmptyResult(hiveMeta.spark), self.tagIds
    
    def _analyzeFieldDependencies(self, rulesDF: DataFrame) -> Dict[str, List[str]]:
        """分析该组标签的字段依赖关系"""
        parser = TagRuleParser()
        fieldDependencies = parser.analyzeFieldDependencies(rulesDF)
        
        return fieldDependencies
    
    def _buildTagTableMapping(self, rulesDF: DataFrame) -> Dict[int, List[str]]:
        """构建标签ID到依赖表的映射关系
        
        Args:
            rulesDF: 标签规则DataFrame
            
        Returns:
            Dict[int, List[str]]: {tag_id: [dependent_tables]}
        """
        parser = TagRuleParser()
        tagTableMapping = {}
        
        # 收集规则到Driver分析
        rules = rulesDF.select("tag_id", "rule_conditions").collect()
        
        for row in rules:
            tag_id = row['tag_id']
            rule_conditions = row['rule_conditions']
            
            # 分析该标签依赖的表
            dependencies = parser._extractTablesFromRule(rule_conditions)
            dependent_tables = list(dependencies)
            
            tagTableMapping[tag_id] = dependent_tables
            print(f"   🏷️  标签 {tag_id} 依赖表: {dependent_tables}")
        
        return tagTableMapping
    
    def _computeAllTagsParallelAndAggregate(self, joinedDF: DataFrame, groupRulesDF: DataFrame, available_tables: List[str] = None) -> DataFrame:
        """并行计算该组所有标签并直接聚合 - 一步到位的优化方案，只使用成功加载的表"""
        print(f"   🎯 并行计算并聚合 {len(self.tagIds)} 个标签...")
        
        # 使用成功加载的表列表，如果没有提供则使用原来的所有表
        tables_to_use = available_tables if available_tables is not None else self.requiredTables
        print(f"   📋 使用成功加载的表进行SQL生成: {tables_to_use}")
        
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
            
            # 🚀 关键修复：只使用成功加载的表生成SQL条件
            sqlCondition = parser.parseRuleToSql(ruleConditions, tables_to_use)
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
        
        # 一次性为所有用户计算其匹配的标签数组
        # 🔧 关键修复：保留所有用户包括无匹配标签的用户，支持标签移除逻辑
        userTagsDF = joinedDF.withColumn("tag_ids_array", combined_tags_expr) \
                           .select("user_id", "tag_ids_array")
        # 📝 注意：不再过滤空数组用户，让后续逻辑处理标签移除
        
        # 统计结果
        try:
            totalUserCount = userTagsDF.count()
            taggedUserCount = userTagsDF.filter(size(col("tag_ids_array")) > 0).count()
            print(f"   ✅ 并行计算完成: {totalUserCount} 个用户（{taggedUserCount} 个有标签，{totalUserCount - taggedUserCount} 个无标签但保留用于标签移除）")
        except:
            print(f"   ✅ 并行计算完成")
        
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