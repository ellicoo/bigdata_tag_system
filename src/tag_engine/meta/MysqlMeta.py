#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据源管理类
负责标签规则、现有标签数据的读取和标签结果的写入
"""
import pymysql
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class MysqlMeta:
    """MySQL数据源管理器
    
    职责：
    1. 加载标签规则数据
    2. 加载现有用户标签数据
    3. 写入标签计算结果
    4. 管理MySQL连接和JDBC配置
    """
    
    def __init__(self, spark: SparkSession, mysqlConfig: Dict[str, str]):
        """初始化MySQL数据源管理器
        
        Args:
            spark: Spark会话
            mysqlConfig: MySQL连接配置
        """
        self.spark = spark
        self.mysqlConfig = mysqlConfig
        self.jdbcUrl = self._buildJdbcUrl()
        
        print("🗄️  MysqlMeta初始化完成")
    
    def _buildJdbcUrl(self) -> str:
        """构建JDBC连接URL"""
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        database = self.mysqlConfig['database']
        
        # 使用connectionCollation参数支持utf8mb4编码
        return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&serverTimezone=UTC"
    
    def loadTagRules(self, tagIds: Optional[List[int]] = None) -> DataFrame:
        """加载标签规则DataFrame
        
        Args:
            tagIds: 指定加载的标签ID列表，None表示加载所有活跃标签
            
        Returns:
            DataFrame: 标签规则DataFrame，包含字段：tag_id, rule_conditions, tag_name
        """
        print(f"📋 加载标签规则，指定标签: {tagIds}")
        
        # 构建查询SQL
        query = """
        (SELECT tr.tag_id, tr.rule_conditions, td.tag_name, td.description
         FROM tag_rules tr
         LEFT JOIN tag_definition td ON tr.tag_id = td.tag_id
         WHERE tr.is_active = 1
        """
        
        if tagIds:
            tagIdsStr = ','.join(map(str, tagIds))
            query += f" AND tr.tag_id IN ({tagIdsStr})"
        
        query += " ORDER BY tr.tag_id) as tag_rules"
        
        try:
            rulesDF = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", query) \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            print(f"✅ 标签规则加载完成: {rulesDF.count()} 个标签")
            return rulesDF
            
        except Exception as e:
            print(f"❌ 加载标签规则失败: {e}")
            return self._createEmptyRulesDataFrame()
    
    def loadExistingTags(self) -> DataFrame:
        """加载现有用户标签DataFrame
        
        Returns:
            DataFrame: 现有标签DataFrame，包含字段：user_id, existing_tag_ids(Array)
        """
        print("📖 加载现有用户标签数据...")
        
        query = "(SELECT user_id, tag_ids FROM user_tags WHERE tag_ids IS NOT NULL) as existing_tags"
        
        try:
            existingDF = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", query) \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # 导入UDF
            from ..utils.TagUdfs import tagUdfs
            
            # 将JSON字符串转换为数组
            existingDF = existingDF.withColumn(
                "existing_tag_ids",
                tagUdfs.jsonToArray(col("tag_ids"))
            ).select("user_id", "existing_tag_ids")
            
            print(f"✅ 现有标签数据加载完成: {existingDF.count()} 个用户")
            return existingDF
            
        except Exception as e:
            print(f"❌ 加载现有标签数据失败: {e}")
            return self._createEmptyExistingTagsDataFrame()
    
    def writeTagResults(self, resultsDF: DataFrame) -> bool:
        """写入标签计算结果到MySQL
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json
            
        Returns:
            bool: 写入是否成功
        """
        print("💾 开始写入标签结果到MySQL...")
        
        try:
            # 收集结果到Driver内存
            results = resultsDF.select("user_id", "final_tag_ids_json").collect()
            
            if not results:
                print("⚠️  没有结果需要写入")
                return True
            
            print(f"📤 准备写入 {len(results)} 条标签记录...")
            
            # 使用pymysql进行批量UPSERT
            connection = pymysql.connect(**self.mysqlConfig)
            
            try:
                with connection.cursor() as cursor:
                    upsertSql = """
                    INSERT INTO user_tags (user_id, tag_ids) 
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        updated_time = CASE 
                            WHEN JSON_EXTRACT(tag_ids, '$') <> JSON_EXTRACT(VALUES(tag_ids), '$')
                            THEN CURRENT_TIMESTAMP 
                            ELSE updated_time 
                        END,
                        tag_ids = VALUES(tag_ids)
                    """
                    
                    # 准备批量数据
                    batchData = [(row['user_id'], row['final_tag_ids_json']) for row in results]
                    
                    # 分批执行（避免单次插入过多数据）
                    batchSize = 1000
                    successCount = 0
                    
                    for i in range(0, len(batchData), batchSize):
                        batch = batchData[i:i + batchSize]
                        cursor.executemany(upsertSql, batch)
                        successCount += len(batch)
                        
                        if i + batchSize < len(batchData):
                            print(f"   📊 已写入 {successCount}/{len(batchData)} 条记录...")
                    
                    connection.commit()
                    print(f"✅ 标签结果写入成功: {successCount} 条记录")
                    return True
                    
            finally:
                connection.close()
                
        except Exception as e:
            print(f"❌ 写入标签结果失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def getTagStatistics(self) -> Dict[str, int]:
        """获取标签统计信息
        
        Returns:
            Dict: 统计信息字典
        """
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            
            try:
                with connection.cursor() as cursor:
                    # 统计活跃标签数
                    cursor.execute("SELECT COUNT(*) FROM tag_rules WHERE is_active = 1")
                    activeTagCount = cursor.fetchone()[0]
                    
                    # 统计有标签的用户数
                    cursor.execute("SELECT COUNT(*) FROM user_tags WHERE tag_ids IS NOT NULL")
                    taggedUserCount = cursor.fetchone()[0]
                    
                    # 统计总用户标签数
                    cursor.execute("SELECT SUM(JSON_LENGTH(tag_ids)) FROM user_tags WHERE tag_ids IS NOT NULL")
                    totalTagCount = cursor.fetchone()[0] or 0
                    
                    return {
                        "activeTagCount": activeTagCount,
                        "taggedUserCount": taggedUserCount,
                        "totalTagCount": totalTagCount,
                        "avgTagsPerUser": round(totalTagCount / taggedUserCount, 2) if taggedUserCount > 0 else 0
                    }
                    
            finally:
                connection.close()
                
        except Exception as e:
            print(f"❌ 获取标签统计失败: {e}")
            return {}
    
    def testConnection(self) -> bool:
        """测试MySQL连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            connection.close()
            print("✅ MySQL连接测试成功")
            return True
        except Exception as e:
            print(f"❌ MySQL连接测试失败: {e}")
            return False
    
    def _createEmptyRulesDataFrame(self) -> DataFrame:
        """创建空的标签规则DataFrame"""
        schema = StructType([
            StructField("tag_id", IntegerType(), False),
            StructField("rule_conditions", StringType(), True),
            StructField("tag_name", StringType(), True),
            StructField("description", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _createEmptyExistingTagsDataFrame(self) -> DataFrame:
        """创建空的现有标签DataFrame"""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("existing_tag_ids", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)