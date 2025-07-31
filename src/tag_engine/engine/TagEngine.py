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
                self._printStatistics()
            
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
        """测试UDF函数"""
        try:
            # 创建测试DataFrame
            testData = [("user1", [1, 2, 3]), ("user2", [2, 3, 4])]
            testDF = self.spark.createDataFrame(testData, ["user_id", "tags"])
            
            # 测试SparkUdfs模块函数
            from ..utils.SparkUdfs import merge_user_tags
            resultDF = testDF.withColumn(
                "merged_tags",
                merge_user_tags(col("tags"))
            )
            
            resultCount = resultDF.count()
            print(f"   ✅ UDF函数测试通过，处理 {resultCount} 条数据")
            return True
            
        except Exception as e:
            print(f"   ❌ UDF函数测试失败: {e}")
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
    
    def _printStatistics(self):
        """打印统计信息"""
        try:
            stats = self.mysqlMeta.getTagStatistics()
            if stats:
                print("\n📊 标签系统统计信息:")
                print(f"   活跃标签数: {stats.get('activeTagCount', 0)}")
                print(f"   有标签用户数: {stats.get('taggedUserCount', 0)}")
                print(f"   总标签数: {stats.get('totalTagCount', 0)}")
                print(f"   平均每用户标签数: {stats.get('avgTagsPerUser', 0)}")
        except:
            print("   ⚠️  无法获取统计信息")
    
    
    def cleanup(self):
        """清理资源"""
        try:
            self.hiveMeta.clearCache()
            print("🧹 TagEngine资源清理完成")
        except Exception as e:
            print(f"⚠️  资源清理异常: {e}")