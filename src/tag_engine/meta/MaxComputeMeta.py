#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MaxCompute数据源管理类
基于MaxCompute Spark引擎，读取MaxCompute表数据

与HiveMeta保持完全一致的接口和实现
表名由配置文件提供，假设DataWorks已配置好表访问
"""
from typing import List, Dict, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from datetime import datetime


class MaxComputeMeta:
    """MaxCompute数据源管理器

    职责：
    1. 管理MaxCompute表的读取和缓存
    2. 智能JOIN多个表
    3. 字段选择和性能优化

    注意：与HiveMeta保持相同的实现，只是数据源从Hive换为MaxCompute
    """

    def __init__(self, spark: SparkSession, maxComputeConfig: Dict = None):
        """初始化MaxCompute数据源管理器

        Args:
            spark: Spark会话（需已配置MaxCompute连接）
            maxComputeConfig: MaxCompute配置（可选）
        """
        self.spark = spark
        self.maxComputeConfig = maxComputeConfig or {}
        self.cachedTables: Dict[str, DataFrame] = {}
        self.failed_tables: Dict[str, str] = {}  # 记录加载失败的表

        # 设置分区日期为当天
        self.partition_date = datetime.now().strftime("%Y-%m-%d")

        print(f"🗄️  MaxComputeMeta初始化完成，将使用分区: {self.partition_date}")

    def loadAndJoinTablesWithFailureTracking(self, tableNames: List[str],
                                             tagTableMapping: Dict[int, List[str]],
                                             fieldMapping: Optional[Dict[str, List[str]]] = None,
                                             joinKey: str = "user_id") -> Tuple[DataFrame, List[int]]:
        """加载并JOIN多个MaxCompute表，跟踪失败的标签ID

        Args:
            tableNames: 需要JOIN的表名列表（格式：project.database.table 或 database.table）
            tagTableMapping: 标签ID到依赖表的映射 {tag_id: [table1, table2]}
            fieldMapping: 表名到字段列表的映射
            joinKey: JOIN的键，默认为user_id

        Returns:
            Tuple[DataFrame, List[int]]: (JOIN后的DataFrame, 失败的标签ID列表)
        """
        print(f"🔗 开始容错加载MaxCompute表: {tableNames}")

        # 清空之前的失败记录
        self.failed_tables.clear()

        successful_tables = []
        failed_tag_ids = set()

        # 逐个尝试加载表
        for tableName in tableNames:
            try:
                selectFields = fieldMapping.get(tableName) if fieldMapping else None

                # MaxCompute表访问（与HiveMeta完全一致）
                # 假设DataWorks已配置好表访问，表名由配置文件提供
                df = self.spark.table(tableName).filter(col('dt') == self.partition_date)

                print(f"   📅 应用分区过滤: dt='{self.partition_date}'")

                # 字段选择和验证
                if selectFields:
                    # 验证字段是否存在（这里会触发懒加载执行）
                    available_columns = df.columns
                    missing_fields = [field for field in selectFields if field not in available_columns]

                    if missing_fields:
                        error_msg = f"字段不存在: {missing_fields}"
                        raise Exception(error_msg)

                    # 确保user_id在选择字段中
                    if "user_id" not in selectFields:
                        selectFields = ["user_id"] + selectFields

                    # 继续链式调用：添加字段选择
                    df = df.select(*selectFields)

                successful_tables.append((tableName, df))
                print(f"✅ 表 {tableName} 加载成功")

            except Exception as e:
                error_msg = str(e)
                self.failed_tables[tableName] = error_msg
                print(f"❌ 表 {tableName} 加载失败: {error_msg}")

                # 找出依赖此表的标签ID
                for tag_id, dependent_tables in tagTableMapping.items():
                    if tableName in dependent_tables:
                        failed_tag_ids.add(tag_id)
                        print(f"   📌 标签 {tag_id} 受影响")


        # 记录成功加载的表名列表
        successful_table_names = [tableName for tableName, df in successful_tables]
        self._last_successful_tables = successful_table_names

        # 如果没有成功加载的表，返回空DataFrame
        if not successful_tables:
            print("❌ 所有表加载失败，返回空DataFrame")
            self._last_successful_tables = []
            return self._createEmptyDataFrame(), list(failed_tag_ids)

        # 如果只有一个成功的表，直接返回
        if len(successful_tables) == 1:
            tableName, resultDF = successful_tables[0]
            aliasName = tableName.split('.')[-1]
            resultDF = resultDF.alias(aliasName)
            print(f"✅ 单表加载完成: {tableName}")
            return resultDF, list(failed_tag_ids)

        # 多表JOIN
        print(f"🔗 开始JOIN {len(successful_tables)} 个成功加载的表")
        resultDF = None

        for i, (tableName, currentDF) in enumerate(successful_tables):
            aliasName = tableName.split('.')[-1]

            if resultDF is None:
                # 第一个表作为基础
                resultDF = currentDF.alias(aliasName)
                print(f"   📋 基础表: {tableName}")
            else:
                # LEFT JOIN后续表
                resultDF = resultDF.join(
                    currentDF.alias(aliasName),
                    joinKey,
                    "left"
                )
                print(f"   🔗 LEFT JOIN: {tableName}")

        print(f"✅ 容错JOIN完成，成功: {len(successful_tables)} 表，失败标签: {list(failed_tag_ids)}")
        return resultDF, list(failed_tag_ids)

    def clearCache(self):
        """清理所有缓存的表"""
        for tableName, df in self.cachedTables.items():
            try:
                df.unpersist()
            except:
                pass

        self.cachedTables.clear()
        print("🧹 MaxComputeMeta缓存已清理")

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

    def getFailureSummary(self) -> str:
        """获取失败摘要信息"""
        if not self.failed_tables:
            return "无表加载失败"

        summary_parts = []
        for table, error in self.failed_tables.items():
            summary_parts.append(f"{table}: {error}")

        return f"失败表({len(self.failed_tables)}个): {'; '.join(summary_parts)}"

    def getSuccessfulTables(self) -> List[str]:
        """获取成功加载的表名列表"""
        # 从最近一次容错加载中获取成功的表
        # 这个需要在loadAndJoinTablesWithFailureTracking中记录
        if hasattr(self, '_last_successful_tables'):
            return self._last_successful_tables
        return []

    def _createEmptyDataFrame(self) -> DataFrame:
        """创建空的DataFrame（包含user_id字段）"""
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([
            StructField("user_id", StringType(), True)
        ])

        return self.spark.createDataFrame([], schema)

    def setPartitionDate(self, partition_date: str):
        """设置分区日期（仅在需要时使用）

        Args:
            partition_date: 分区日期，格式为'YYYY-MM-DD'
        """
        self.partition_date = partition_date
        print(f"📅 分区日期已更新为: {self.partition_date}")

        # 清理缓存，因为分区日期变了
        self.clearCache()

    def getPartitionDate(self) -> str:
        """获取当前设置的分区日期"""
        return self.partition_date