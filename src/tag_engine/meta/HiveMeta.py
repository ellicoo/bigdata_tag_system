#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive数据源管理类
参考TFECUserPortrait的EsMeta设计模式，负责Hive表的读取、缓存和JOIN操作
"""
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *


class HiveMeta:
    """Hive数据源管理器
    
    职责：
    1. 管理Hive表的读取和缓存
    2. 智能JOIN多个表
    3. 字段选择和性能优化
    """
    
    def __init__(self, spark: SparkSession):
        """初始化Hive数据源管理器
        
        Args:
            spark: Spark会话
        """
        self.spark = spark
        self.cachedTables: Dict[str, DataFrame] = {}
        
        print("🗄️  HiveMeta初始化完成")
    
    def loadTable(self, tableName: str, selectFields: Optional[List[str]] = None) -> DataFrame:
        """纯粹加载单个Hive表，不缓存
        
        Args:
            tableName: 表名
            selectFields: 需要选择的字段列表，None表示选择所有字段
            
        Returns:
            DataFrame: 加载的表DataFrame（未缓存）
        """
        print(f"📖 加载Hive表: {tableName}")
        
        try:
            df = self.spark.table(tableName)
            
            # 字段选择优化
            if selectFields:
                # 确保user_id在选择字段中（JOIN需要）
                if "user_id" not in selectFields:
                    selectFields = ["user_id"] + selectFields
                df = df.select(*selectFields)
            
            print(f"✅ 表 {tableName} 加载完成，字段: {selectFields or 'ALL'}")
            return df
            
        except Exception as e:
            print(f"❌ 加载表 {tableName} 失败: {e}")
            # 返回空DataFrame，避免程序崩溃
            return self._createEmptyDataFrame()
    
    def loadTables(self, tableNames: List[str], fieldMapping: Optional[Dict[str, List[str]]] = None) -> Dict[str, DataFrame]:
        """批量加载多个Hive表
        
        Args:
            tableNames: 表名列表
            fieldMapping: 表名到字段列表的映射，格式: {"table1": ["field1", "field2"]}
            
        Returns:
            Dict[str, DataFrame]: 表名到DataFrame的映射
        """
        result = {}
        
        for tableName in tableNames:
            selectFields = fieldMapping.get(tableName) if fieldMapping else None
            result[tableName] = self.loadTable(tableName, selectFields)
        
        print(f"📚 批量加载完成: {len(result)} 个表")
        return result
    
    def loadAndJoinTables(self, tableNames: List[str], 
                         fieldMapping: Optional[Dict[str, List[str]]] = None,
                         joinKey: str = "user_id") -> DataFrame:
        """加载并JOIN多个表，统一控制缓存策略
        
        策略：
        - 单表情况：缓存单表
        - 多表情况：缓存JOIN结果
        
        Args:
            tableNames: 需要JOIN的表名列表
            fieldMapping: 表名到字段列表的映射
            joinKey: JOIN的键，默认为user_id
            
        Returns:
            DataFrame: JOIN后的结果DataFrame（已缓存）
        """
        if not tableNames:
            return self._createEmptyDataFrame()
        
        if len(tableNames) == 1:
            # 🚀 单表情况：缓存单表
            tableName = tableNames[0]
            selectFields = fieldMapping.get(tableName) if fieldMapping else None
            
            # 生成单表缓存key
            fieldKey = str(sorted(selectFields)) if selectFields else "ALL"
            singleTableCacheKey = f"{tableName}:{fieldKey}"
            
            # 检查是否已缓存
            if singleTableCacheKey in self.cachedTables:
                print(f"🚀 复用单表缓存: {tableName}")
                return self.cachedTables[singleTableCacheKey]
            
            # 加载并缓存单表
            resultDF = self.loadTable(tableName, selectFields)
            
            # 🔧 关键修复：使用简化的表名作为alias，避免点号导致的反引号问题
            # 例如：tag_system.user_asset_summary -> user_asset_summary
            aliasName = tableName.split('.')[-1]
            resultDF = resultDF.alias(aliasName)
            
            from pyspark import StorageLevel
            resultDF.persist(StorageLevel.MEMORY_AND_DISK)
            self.cachedTables[singleTableCacheKey] = resultDF
            
            print(f"✅ 单表加载并缓存完成: {tableName} (alias: {aliasName})")
            return resultDF
        
        # 🚀 多表情况：缓存JOIN结果
        # 为JOIN结果生成缓存key，确保相同的JOIN操作复用结果
        sortedTables = sorted(tableNames)
        fieldKey = ""
        if fieldMapping:
            fieldItems = sorted([(t, sorted(fields)) for t, fields in fieldMapping.items() if t in tableNames])
            fieldKey = str(fieldItems)
        joinCacheKey = f"JOIN:{','.join(sortedTables)}:{fieldKey}:{joinKey}"
        
        # 检查是否已缓存JOIN结果
        if joinCacheKey in self.cachedTables:
            print(f"🚀 复用JOIN缓存: {tableNames}")
            return self.cachedTables[joinCacheKey]
        
        print(f"🔗 开始JOIN表: {tableNames}")
        
        # 🚀 关键优化：多表情况直接JOIN，不缓存中间子表，只缓存最终JOIN结果
        resultDF = None
        
        for i, tableName in enumerate(tableNames):
            # 直接加载表，不缓存子表
            selectFields = fieldMapping.get(tableName) if fieldMapping else None
            currentDF = self.loadTable(tableName, selectFields)
            
            # 🔧 关键修复：使用简化的表名作为alias，避免点号导致的反引号问题
            # 例如：tag_system.user_asset_summary -> user_asset_summary
            aliasName = tableName.split('.')[-1]
            
            if resultDF is None:
                # 第一个表作为基础
                resultDF = currentDF.alias(aliasName)
                print(f"   📋 基础表: {tableName} (alias: {aliasName})")
            else:
                # LEFT JOIN后续表
                resultDF = resultDF.join(
                    currentDF.alias(aliasName),
                    joinKey,
                    "left"
                )
                print(f"   🔗 LEFT JOIN: {tableName} (alias: {aliasName})")
        
        # 🚀 关键策略：只缓存最终JOIN结果，不缓存中间子表
        from pyspark import StorageLevel
        resultDF.persist(StorageLevel.MEMORY_AND_DISK)
        self.cachedTables[joinCacheKey] = resultDF
        
        print(f"✅ JOIN完成并缓存最终结果，涉及 {len(tableNames)} 个表")
        return resultDF
    
    def clearCache(self):
        """清理所有缓存的表"""
        for tableName, df in self.cachedTables.items():
            try:
                df.unpersist()
            except:
                pass
        
        self.cachedTables.clear()
        print("🧹 HiveMeta缓存已清理")
    
    def clearGroupCache(self, groupTables: List[str]):
        """清理特定标签组的表缓存（智能缓存管理）
        
        Args:
            groupTables: 该标签组依赖的表名列表
        """
        if not groupTables:
            return
        
        clearedTables = []
        
        # 遍历所有缓存的表，检查是否属于该组
        cachesToRemove = []
        
        for cacheKey, df in self.cachedTables.items():
            # 检查缓存key是否属于该组
            shouldClear = False
            cacheDescription = cacheKey  # 用于显示的描述
            
            if cacheKey.startswith("JOIN:"):
                # JOIN缓存格式: "JOIN:table1,table2:fieldKey:joinKey"
                joinTables = cacheKey.split(":")[1].split(",")
                # 如果JOIN中包含该组的任何表，则清理
                if any(table in groupTables for table in joinTables):
                    shouldClear = True
                    cacheDescription = f"JOIN({','.join(joinTables)})"
            else:
                # 单表缓存格式: "tableName:fields" 或 "tableName:ALL"  
                tableName = cacheKey.split(":")[0]
                if tableName in groupTables:
                    shouldClear = True
                    cacheDescription = tableName
            
            if shouldClear:
                try:
                    df.unpersist()
                    cachesToRemove.append(cacheKey)
                    clearedTables.append(cacheDescription)
                except Exception as e:
                    print(f"   ⚠️  清理缓存 {cacheDescription} 失败: {e}")
        
        # 从缓存字典中移除
        for cacheKey in cachesToRemove:
            del self.cachedTables[cacheKey]
        
        if clearedTables:
            # 去重显示清理的表
            uniqueTables = list(set(clearedTables))
            print(f"   🧹 已清理 {len(uniqueTables)} 个表的缓存: {uniqueTables}")
        else:
            print(f"   ℹ️  组 {groupTables} 无需清理缓存")
    
    def _createEmptyDataFrame(self) -> DataFrame:
        """创建空的DataFrame（包含user_id字段）"""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("user_id", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)