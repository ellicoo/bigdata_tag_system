#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算组
将相同表依赖的标签归为一组，实现并行高效计算
"""
from typing import List, Dict, Set
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class TagGroup:
    """标签计算组
    
    职责：
    1. 管理一组具有相同表依赖的标签
    2. 执行该组标签的并行计算
    3. 优化表读取和JOIN操作
    4. 使用UDF进行标签合并
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
    
    def computeTags(self, hiveMeta, mysqlMeta, rulesDF: DataFrame) -> DataFrame:
        """计算该组所有标签
        
        Args:
            hiveMeta: Hive数据源管理器
            mysqlMeta: MySQL数据源管理器
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
            
            # 3. 加载并JOIN所需的Hive表
            joinedDF = hiveMeta.loadAndJoinTables(self.requiredTables, fieldDependencies)
            print(f"   🔗 JOIN完成，用户数: {joinedDF.count()}")
            
            # 4. 为该组并行计算所有标签
            tagResultsDF = self._computeAllTagsParallel(joinedDF, groupRulesDF)
            
            # 5. 聚合用户标签
            userTagsDF = self._aggregateUserTags(tagResultsDF)
            
            print(f"✅ 标签组计算完成: {userTagsDF.count()} 个用户")
            return userTagsDF
            
        except Exception as e:
            print(f"❌ 标签组计算失败: {e}")
            import traceback
            traceback.print_exc()
            return self._createEmptyResult(hiveMeta.spark)
    
    def _analyzeFieldDependencies(self, rulesDF: DataFrame) -> Dict[str, List[str]]:
        """分析该组标签的字段依赖关系"""
        from ..parser.TagRuleParser import TagRuleParser
        
        parser = TagRuleParser()
        fieldDependencies = parser.analyzeFieldDependencies(rulesDF)
        
        return fieldDependencies
    
    def _computeAllTagsParallel(self, joinedDF: DataFrame, groupRulesDF: DataFrame) -> DataFrame:
        """并行计算该组所有标签"""
        print(f"   🎯 并行计算 {len(self.tagIds)} 个标签...")
        
        # 导入UDF
        from ..utils.TagUdfs import tagUdfs
        
        # 收集规则到Driver
        rules = groupRulesDF.select("tag_id", "rule_conditions").collect()
        
        tagResults = []
        
        for row in rules:
            tagId = row['tag_id']
            ruleConditions = row['rule_conditions']
            
            print(f"      🏷️  计算标签 {tagId}...")
            
            # 使用TagRuleParser解析规则为SQL条件
            from ..parser.TagRuleParser import TagRuleParser
            parser = TagRuleParser()
            sqlCondition = parser.parseRuleToSql(ruleConditions)
            
            # 应用规则筛选用户
            try:
                if sqlCondition and sqlCondition.strip() and sqlCondition != "1=0":
                    # 使用解析后的SQL条件筛选用户
                    tagDF = joinedDF.filter(expr(sqlCondition)) \
                                   .select("user_id") \
                                   .withColumn("tag_id", lit(tagId))
                else:
                    # 空规则或无效规则返回空结果
                    tagDF = joinedDF.select("user_id") \
                                   .withColumn("tag_id", lit(tagId)) \
                                   .limit(0)
                
                tagResults.append(tagDF)
                print(f"         ✅ 标签 {tagId}: {tagDF.count()} 个用户")
                
            except Exception as e:
                print(f"         ❌ 标签 {tagId} 计算失败: {e}")
                # 创建空结果
                emptyDF = joinedDF.select("user_id") \
                                 .withColumn("tag_id", lit(tagId)) \
                                 .limit(0)
                tagResults.append(emptyDF)
        
        # 合并所有标签结果
        if tagResults:
            allTagsDF = tagResults[0]
            for tagDF in tagResults[1:]:
                allTagsDF = allTagsDF.union(tagDF)
            return allTagsDF
        else:
            return self._createEmptyTagResult(joinedDF)
    
    def _aggregateUserTags(self, tagResultsDF: DataFrame) -> DataFrame:
        """聚合用户标签，去重排序"""
        print("   🔀 聚合用户标签...")
        
        # 导入UDF
        from ..utils.TagUdfs import tagUdfs
        
        # 按用户聚合标签
        userTagsDF = tagResultsDF.groupBy("user_id").agg(
            tagUdfs.mergeUserTags(collect_list("tag_id")).alias("tag_ids_array")
        )
        
        print(f"   ✅ 用户标签聚合完成: {userTagsDF.count()} 个用户")
        return userTagsDF
    
    def _createEmptyResult(self, spark) -> DataFrame:
        """创建空的计算结果"""
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("tag_ids_array", ArrayType(IntegerType()), True)
        ])
        
        return spark.createDataFrame([], schema)
    
    def _createEmptyTagResult(self, baseDF: DataFrame) -> DataFrame:
        """创建空的标签结果DataFrame"""
        return baseDF.select("user_id") \
                    .withColumn("tag_id", lit(0)) \
                    .limit(0)
    
    def getGroupInfo(self) -> Dict[str, any]:
        """获取标签组信息
        
        Returns:
            Dict: 标签组信息
        """
        return {
            "name": self.name,
            "tagIds": self.tagIds,
            "tagCount": len(self.tagIds),
            "requiredTables": self.requiredTables,
            "tableCount": len(self.requiredTables)
        }
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"TagGroup(tags={self.tagIds}, tables={self.requiredTables})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()