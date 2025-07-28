#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算引擎
主要负责标签计算流程的编排和执行
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from ..meta.HiveMeta import HiveMeta
from ..meta.MysqlMeta import MysqlMeta
from ..parser.TagRuleParser import TagRuleParser
from .TagGroup import TagGroup
from ..utils.TagUdfs import tagUdfs


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
        """执行标签计算
        
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
            
            # 2. 分析依赖关系并智能分组
            tagGroups = self._analyzeAndGroupTags(rulesDF)
            if not tagGroups:
                print("⚠️  没有找到可计算的标签组")
                return True
            
            # 3. 并行计算所有标签组
            allResults = self._computeAllTagGroups(tagGroups, rulesDF)
            
            # 4. 合并标签结果
            finalResults = self._mergeAllTagResults(allResults)
            
            # 5. 与MySQL现有标签合并并写入
            success = self._mergeWithExistingAndSave(finalResults)
            
            if success:
                print("✅ 标签计算完成")
                self._printStatistics()
            else:
                print("❌ 标签计算失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 标签计算异常: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def healthCheck(self) -> bool:
        """健康检查
        
        Returns:
            bool: 系统是否健康
        """
        print("🔍 执行标签系统健康检查...")
        
        try:
            # 1. 测试MySQL连接
            mysqlOk = self.mysqlMeta.testConnection()
            
            # 2. 测试Hive表访问（使用简单表测试）
            hiveOk = self._testHiveAccess()
            
            # 3. 测试UDF功能
            udfOk = self._testUdfFunctions()
            
            # 4. 检查标签规则
            rulesOk = self._checkTagRules()
            
            allOk = mysqlOk and hiveOk and udfOk and rulesOk
            
            if allOk:
                print("✅ 系统健康检查通过")
            else:
                print("❌ 系统健康检查失败")
                print(f"   MySQL: {'✅' if mysqlOk else '❌'}")
                print(f"   Hive: {'✅' if hiveOk else '❌'}")
                print(f"   UDF: {'✅' if udfOk else '❌'}")
                print(f"   Rules: {'✅' if rulesOk else '❌'}")
            
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
    
    def _computeAllTagGroups(self, tagGroups: List[TagGroup], rulesDF: DataFrame) -> List[DataFrame]:
        """并行计算所有标签组"""
        print(f"🚀 并行计算 {len(tagGroups)} 个标签组...")
        
        allResults = []
        
        for i, group in enumerate(tagGroups):
            print(f"   📦 计算标签组 {i+1}/{len(tagGroups)}: {group.name}")
            
            try:
                groupResult = group.computeTags(self.hiveMeta, self.mysqlMeta, rulesDF)
                allResults.append(groupResult)
                
            except Exception as e:
                print(f"   ❌ 标签组 {group.name} 计算失败: {e}")
                # 添加空结果，避免影响其他组
                emptyResult = self._createEmptyGroupResult()
                allResults.append(emptyResult)
        
        print(f"✅ 所有标签组计算完成，成功: {len(allResults)} 个")
        return allResults
    
    def _mergeAllTagResults(self, allResults: List[DataFrame]) -> DataFrame:
        """合并所有标签组的结果"""
        print("🔀 合并所有标签组结果...")
        
        if not allResults:
            return self._createEmptyUserTagsResult()
        
        # 尝试合并，如果合并后为空再处理
        try:
            mergedDF = allResults[0]
            for resultDF in allResults[1:]:
                mergedDF = mergedDF.union(resultDF)
            
            # 只在最后检查一次
            finalCount = mergedDF.count()
            if finalCount == 0:
                print("   ⚠️  所有标签组结果都为空")
                return self._createEmptyUserTagsResult()
                
            # 按用户重新聚合所有标签
            finalDF = mergedDF.groupBy("user_id").agg(
                tagUdfs.mergeUserTags(
                    flatten(collect_list("tag_ids_array"))
                ).alias("merged_tag_ids")
            )
            
            print(f"   ✅ 标签结果合并完成: {finalDF.count()} 个用户")
            return finalDF
            
        except Exception as e:
            print(f"   ❌ 合并过程异常: {e}")
            return self._createEmptyUserTagsResult()
    
    def _mergeWithExistingAndSave(self, newTagsDF: DataFrame) -> bool:
        """与MySQL现有标签合并并保存"""
        print("💾 与现有标签合并并保存...")
        
        try:
            # 加载现有标签
            existingTagsDF = self.mysqlMeta.loadExistingTags()
            
            # LEFT JOIN 合并
            joinedDF = newTagsDF.alias("new").join(
                existingTagsDF.alias("existing"),
                col("new.user_id") == col("existing.user_id"),
                "left"
            )
            
            # 使用UDF合并标签
            finalDF = joinedDF.withColumn(
                "final_tag_ids",
                tagUdfs.mergeWithExistingTags(
                    col("new.merged_tag_ids"),
                    col("existing.existing_tag_ids")
                )
            ).withColumn(
                "final_tag_ids_json",
                tagUdfs.arrayToJson(col("final_tag_ids"))
            ).select(
                col("new.user_id").alias("user_id"),
                col("final_tag_ids_json")
            )
            
            # 写入MySQL
            success = self.mysqlMeta.writeTagResults(finalDF)
            
            if success:
                print(f"   ✅ 标签结果保存成功: {finalDF.count()} 个用户")
            
            return success
            
        except Exception as e:
            print(f"   ❌ 标签合并保存失败: {e}")
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
            
            # 测试UDF
            resultDF = testDF.withColumn(
                "merged_tags",
                tagUdfs.mergeUserTags(col("tags"))
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
    
    def _createEmptyGroupResult(self) -> DataFrame:
        """创建空的标签组结果"""
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("tag_ids_array", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _createEmptyUserTagsResult(self) -> DataFrame:
        """创建空的用户标签结果"""
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("merged_tag_ids", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def cleanup(self):
        """清理资源"""
        try:
            self.hiveMeta.clearCache()
            print("🧹 TagEngine资源清理完成")
        except Exception as e:
            print(f"⚠️  资源清理异常: {e}")
    
    def __del__(self):
        """析构函数"""
        self.cleanup()