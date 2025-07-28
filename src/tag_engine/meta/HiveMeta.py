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
        """加载单个Hive表
        
        Args:
            tableName: 表名
            selectFields: 需要选择的字段列表，None表示选择所有字段
            
        Returns:
            DataFrame: 加载的表DataFrame
        """
        cacheKey = f"{tableName}:{','.join(selectFields) if selectFields else 'ALL'}"
        
        if cacheKey not in self.cachedTables:
            print(f"📖 加载Hive表: {tableName}")
            
            try:
                df = self.spark.table(tableName)
                
                # 字段选择优化
                if selectFields:
                    # 确保user_id在选择字段中（JOIN需要）
                    if "user_id" not in selectFields:
                        selectFields = ["user_id"] + selectFields
                    df = df.select(*selectFields)
                
                # 缓存DataFrame
                df.cache()
                self.cachedTables[cacheKey] = df
                
                print(f"✅ 表 {tableName} 加载完成，字段: {selectFields or 'ALL'}")
                
            except Exception as e:
                print(f"❌ 加载表 {tableName} 失败: {e}")
                # 返回空DataFrame，避免程序崩溃
                return self._createEmptyDataFrame()
        
        return self.cachedTables[cacheKey]
    
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
        """加载并JOIN多个表
        
        Args:
            tableNames: 需要JOIN的表名列表
            fieldMapping: 表名到字段列表的映射
            joinKey: JOIN的键，默认为user_id
            
        Returns:
            DataFrame: JOIN后的结果DataFrame
        """
        if not tableNames:
            return self._createEmptyDataFrame()
        
        if len(tableNames) == 1:
            # 只有一个表，直接返回
            return self.loadTable(tableNames[0], fieldMapping.get(tableNames[0]) if fieldMapping else None)
        
        print(f"🔗 开始JOIN表: {tableNames}")
        
        # 加载所有表
        tables = self.loadTables(tableNames, fieldMapping)
        
        # 开始JOIN操作
        resultDF = None
        
        for i, tableName in enumerate(tableNames):
            currentDF = tables[tableName]
            
            if resultDF is None:
                # 第一个表作为基础
                resultDF = currentDF.alias(tableName)
                print(f"   📋 基础表: {tableName}")
            else:
                # LEFT JOIN后续表
                resultDF = resultDF.join(
                    currentDF.alias(tableName),
                    joinKey,
                    "left"
                )
                print(f"   🔗 LEFT JOIN: {tableName}")
        
        print(f"✅ JOIN完成，涉及 {len(tableNames)} 个表")
        return resultDF
    
    def getTableInfo(self, tableName: str) -> Dict[str, str]:
        """获取表信息
        
        Args:
            tableName: 表名
            
        Returns:
            Dict: 表信息字典
        """
        try:
            df = self.spark.table(tableName)
            
            return {
                "tableName": tableName,
                "columnCount": len(df.columns),
                "columns": df.columns,
                "rowCount": df.count()
            }
        except Exception as e:
            print(f"❌ 获取表 {tableName} 信息失败: {e}")
            return {}
    
    def clearCache(self):
        """清理所有缓存的表"""
        for tableName, df in self.cachedTables.items():
            try:
                df.unpersist()
            except:
                pass
        
        self.cachedTables.clear()
        print("🧹 HiveMeta缓存已清理")
    
    def getCacheStats(self) -> Dict[str, Dict]:
        """获取缓存统计信息
        
        Returns:
            Dict: 缓存统计信息
        """
        stats = {}
        
        for cacheKey, df in self.cachedTables.items():
            try:
                stats[cacheKey] = {
                    "columns": len(df.columns),
                    "isCached": df.is_cached,
                    "storageLevel": str(df.storageLevel) if hasattr(df, 'storageLevel') else "UNKNOWN"
                }
            except Exception as e:
                stats[cacheKey] = {"error": str(e)}
        
        return stats
    
    def _createEmptyDataFrame(self) -> DataFrame:
        """创建空的DataFrame（包含user_id字段）"""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("user_id", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def optimizeForTagComputation(self, df: DataFrame) -> DataFrame:
        """为标签计算优化DataFrame
        
        Args:
            df: 输入DataFrame
            
        Returns:
            DataFrame: 优化后的DataFrame
        """
        # 去重（确保每个用户只有一条记录）
        df = df.dropDuplicates(["user_id"])
        
        # 缓存（标签计算会多次使用）
        df.cache()
        
        print(f"🚀 DataFrame优化完成，用户数: {df.count()}")
        return df