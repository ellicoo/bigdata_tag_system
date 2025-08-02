#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算引擎
主要负责标签计算流程的编排和执行
"""
from typing import List, Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

from ..meta.HiveMeta import HiveMeta
from ..meta.MysqlMeta import MysqlMeta
from ..parser.TagRuleParser import TagRuleParser
from .TagGroup import TagGroup
from ..utils.SparkUdfs import merge_with_existing_tags


class TagEngine:
    """标签计算引擎
    
    职责：
    1. 编排整个标签计算流程
    2. 管理Hive和MySQL数据源
    3. 协调标签分组和并行计算
    4. 执行标签合并和结果写入
    """
    
    def __init__(self, spark: SparkSession, hiveConfig: Dict = None, mysqlConfig: Dict = None):
        """初始化标签引擎
        
        Args:
            spark: Spark会话
            hiveConfig: Hive配置（可选）
            mysqlConfig: MySQL配置
        """
        self.spark = spark
        self.hiveConfig = hiveConfig or {}
        self.mysqlConfig = mysqlConfig
        
        # 初始化数据源管理器
        self.hiveMeta = HiveMeta(spark)
        self.mysqlMeta = MysqlMeta(spark, mysqlConfig)
        self.ruleParser = TagRuleParser()
        
        print("🚀 TagEngine初始化完成")
    
    def computeTags(self, mode: str = "full", tagIds: Optional[List[int]] = None) -> bool:
        """执行标签计算 - 简化的主流程编排
        
        Args:
            mode: 计算模式（full/specific）
            tagIds: 指定标签ID列表（仅在specific模式下有效）
            
        Returns:
            bool: 计算是否成功
        """
        print(f"🚀 开始标签计算，模式: {mode}")
        
        try:
            # 1. 加载标签规则
            rulesDF = self._loadTagRules(tagIds)
            if rulesDF.count() == 0:
                print("⚠️  没有找到活跃的标签规则")
                return True
            
            # 2. 智能分组（基于表依赖）
            tagGroups = self._analyzeAndGroupTags(rulesDF)
            if not tagGroups:
                print("⚠️  没有找到可计算的标签组")
                return True
            
            # 3. 流水线处理所有标签组
            success = self._processTagGroupsPipeline(tagGroups, rulesDF)
            
            if success:
                print("✅ 标签计算完成")
            
            return success
            
        except Exception as e:
            print(f"❌ 标签计算异常: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def healthCheck(self) -> bool:
        """系统健康检查 - 计算前的必要验证
        
        Returns:
            bool: 系统是否健康
        """
        print("🔍 执行系统健康检查...")
        
        try:
            checks = {
                "MySQL连接": self.mysqlMeta.testConnection(),
                "Hive访问": self._testHiveAccess(),
                "UDF功能": self._testUdfFunctions(),
                "标签规则": self._checkTagRules()
            }
            
            allOk = all(checks.values())
            
            print("📋 健康检查结果:")
            for check, result in checks.items():
                print(f"   {check}: {'✅' if result else '❌'}")
            
            return allOk
            
        except Exception as e:
            print(f"❌ 健康检查异常: {e}")
            return False
    
    # ========== 私有方法 ==========
    
    def _loadTagRules(self, tagIds: Optional[List[int]] = None) -> DataFrame:
        """加载标签规则"""
        print("📋 加载标签规则...")
        return self.mysqlMeta.loadTagRules(tagIds)
    
    def _analyzeAndGroupTags(self, rulesDF: DataFrame) -> List[TagGroup]:
        """分析依赖关系并进行智能分组"""
        print("🎯 分析标签依赖关系...")
        
        # 分析所有标签的表依赖
        dependencies = self.ruleParser.analyzeDependencies(rulesDF)
        
        # 智能分组
        tagGroups = self.ruleParser.groupTagsByTables(dependencies)
        
        return tagGroups
    
    def _processTagGroupsPipeline(self, tagGroups: List[TagGroup], rulesDF: DataFrame) -> bool:
        """简化的流水线处理：计算标签组并写入MySQL"""
        print(f"🚀 流水线处理 {len(tagGroups)} 个标签组...")
        
        successCount = 0
        
        for i, group in enumerate(tagGroups):
            print(f"\n📦 处理标签组 {i+1}/{len(tagGroups)}: {group.name}")
            
            try:
                # 过滤该组相关的标签规则
                groupRulesDF = rulesDF.filter(col("tag_id").isin(group.tagIds))
                
                # 计算该组标签
                groupResult = group.computeTags(self.hiveMeta, groupRulesDF)
                
                if groupResult.count() == 0:
                    print(f"   ⚠️  标签组 {group.name} 无匹配用户，跳过")
                    successCount += 1
                    continue
                
                # 合并并写入MySQL
                if self._mergeAndSaveGroup(groupResult, group.name):
                    print(f"   ✅ 标签组 {group.name} 处理完成")
                    successCount += 1
                
                # 清理缓存
                self.hiveMeta.clearGroupCache(group.requiredTables)
                
            except Exception as e:
                print(f"   ❌ 标签组 {group.name} 处理失败: {e}")
        
        return successCount == len(tagGroups)
    
    def _mergeAndSaveGroup(self, groupResult: DataFrame, groupName: str) -> bool:
        """合并标签并保存到MySQL"""
        try:
            # 加载现有标签
            existingTagsDF = self.mysqlMeta.loadExistingTags()
            
            # LEFT JOIN 合并
            joinedDF = groupResult.alias("new").join(
                existingTagsDF.alias("existing"),
                col("new.user_id") == col("existing.user_id"),
                "left"
            )
            
            # 使用SparkUdfs模块合并标签
            from ..utils.SparkUdfs import array_to_json
            finalDF = joinedDF.withColumn(
                "final_tag_ids",
                merge_with_existing_tags(
                    col("new.tag_ids_array"),
                    col("existing.existing_tag_ids")
                )
            ).withColumn(
                "final_tag_ids_json",
                array_to_json(col("final_tag_ids"))
            ).select(
                col("new.user_id").alias("user_id"),
                col("final_tag_ids_json")
            )
            
            # 写入MySQL
            success = self.mysqlMeta.writeTagResults(finalDF)
            
            if success:
                userCount = finalDF.count()
                print(f"   ✅ {groupName}: {userCount} 个用户")
            
            return success
            
        except Exception as e:
            print(f"   ❌ {groupName} 保存失败: {e}")
            return False
    
    
    def _testHiveAccess(self) -> bool:
        """测试Hive表访问"""
        try:
            # 尝试列出表
            tables = self.spark.sql("SHOW TABLES").collect()
            print(f"   ✅ Hive访问正常，发现 {len(tables)} 个表")
            return True
        except Exception as e:
            print(f"   ❌ Hive访问失败: {e}")
            return False
    
    def _testUdfFunctions(self) -> bool:
        """测试工具函数"""
        try:
            # 创建测试DataFrame - 测试新老标签合并
            testData = [
                ("user1", [1, 2, 3], [2, 3, 4]),
                ("user2", [5, 6], [6, 7, 8])
            ]
            testDF = self.spark.createDataFrame(testData, ["user_id", "new_tags", "existing_tags"])
            
            # 测试merge_with_existing_tags函数
            from ..utils.SparkUdfs import merge_with_existing_tags
            resultDF = testDF.withColumn(
                "merged_tags",
                merge_with_existing_tags(col("new_tags"), col("existing_tags"))
            )
            
            resultCount = resultDF.count()
            print(f"   ✅ 工具函数测试通过，处理 {resultCount} 条数据")
            
            # 测试tagExpressionUtils工具
            from ..utils.tagExpressionUtils import buildParallelTagExpression
            tag_conditions = [
                {'tag_id': 1, 'condition': 'new_tags is not null'},
                {'tag_id': 2, 'condition': 'existing_tags is not null'}
            ]
            expr = buildParallelTagExpression(tag_conditions)
            print(f"   ✅ 并行标签表达式工具测试通过")
            
            return True
            
        except Exception as e:
            print(f"   ❌ 工具函数测试失败: {e}")
            return False
    
    def _checkTagRules(self) -> bool:
        """检查标签规则"""
        try:
            rulesDF = self.mysqlMeta.loadTagRules()
            ruleCount = rulesDF.count()
            
            if ruleCount > 0:
                print(f"   ✅ 标签规则检查通过，发现 {ruleCount} 个活跃标签")
                return True
            else:
                print("   ⚠️  没有发现活跃的标签规则")
                return False
                
        except Exception as e:
            print(f"   ❌ 标签规则检查失败: {e}")
            return False
    
    
    def cleanup(self):
        """清理资源"""
        try:
            self.hiveMeta.clearCache()
            print("🧹 TagEngine资源清理完成")
        except Exception as e:
            print(f"⚠️  资源清理异常: {e}")