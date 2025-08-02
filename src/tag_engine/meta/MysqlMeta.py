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
from ..utils.SparkUdfs import json_to_array


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
            
            # 使用SparkUdfs模块转换JSON为Array
            existingDF = existingDF.withColumn(
                "existing_tag_ids",
                json_to_array(col("tag_ids"))
            ).select("user_id", "existing_tag_ids")
            
            print(f"✅ 现有标签数据加载完成: {existingDF.count()} 个用户")
            return existingDF
            
        except Exception as e:
            print(f"❌ 加载现有标签数据失败: {e}")
            return self._createEmptyExistingTagsDataFrame()
    
    def writeTagResults(self, resultsDF: DataFrame) -> bool:
        """写入标签计算结果到MySQL - 临时表+原生SQL方案
        
        使用Spark原生JDBC写入临时表，然后用纯SQL执行UPSERT
        完全避免Python版本冲突问题
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json
            
        Returns:
            bool: 写入是否成功
        """
        print("💾 开始写入标签结果到MySQL（临时表+原生SQL方案）...")
        
        try:
            # 先检查是否有数据需要写入
            totalCount = resultsDF.count()
            if totalCount == 0:
                print("⚠️  没有结果需要写入")
                return True
            
            print(f"📤 准备写入 {totalCount} 条标签记录...")
            
            # 🚀 步骤1：写入临时表（只要user_id, final_tag_ids_json）
            import time
            temp_table = f"user_tags_temp_{int(time.time())}"
            print(f"📋 创建临时表: {temp_table}")
            
            # 使用Spark原生JDBC写入，完全避免Python代码分发
            resultsDF.select("user_id", col("final_tag_ids_json").alias("tag_ids")) \
                .write \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", temp_table) \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4") \
                .mode("overwrite") \
                .save()
            
            print(f"✅ 临时表 {temp_table} 写入完成")
            
            # 🚀 步骤2：执行UPSERT
            upsert_success = self._executeSimpleUpsert(temp_table, totalCount)
            
            # 🚀 步骤3：清理临时表
            self._dropTempTable(temp_table)
            
            if upsert_success:
                print(f"✅ 标签结果写入完成: {totalCount} 条记录")
                return True
            else:
                return False
            
        except Exception as e:
            print(f"❌ 写入失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
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
    
    def _executeSimpleUpsert(self, temp_table: str, record_count: int) -> bool:
        """执行简单的UPSERT，利用现有的user_tags表结构"""
        print(f"🔄 执行UPSERT操作，从 {temp_table} 到 user_tags...")
        
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            
            with connection.cursor() as cursor:
                # 简单UPSERT，保持原有表结构和业务逻辑
                upsert_sql = f"""
                INSERT INTO user_tags (user_id, tag_ids)
                SELECT user_id, tag_ids
                FROM {temp_table}
                ON DUPLICATE KEY UPDATE
                    updated_time = CASE 
                        WHEN JSON_EXTRACT(user_tags.tag_ids, '$') <> JSON_EXTRACT(VALUES(tag_ids), '$')
                        THEN CURRENT_TIMESTAMP 
                        ELSE user_tags.updated_time 
                    END,
                    tag_ids = VALUES(tag_ids)
                """
                
                print(f"   📝 执行SQL: INSERT INTO user_tags ... FROM {temp_table}")
                cursor.execute(upsert_sql)
                affected_rows = cursor.rowcount
                connection.commit()
                
                print(f"   ✅ UPSERT完成，影响行数: {affected_rows}")
                return True
                
        except Exception as e:
            print(f"   ❌ UPSERT失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            connection.close()
    
    def _dropTempTable(self, temp_table: str):
        """清理临时表"""
        print(f"🧹 清理临时表: {temp_table}")
        
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                connection.commit()
                print(f"   ✅ 临时表 {temp_table} 已清理")
                
        except Exception as e:
            print(f"   ⚠️  清理临时表失败: {e}")
        finally:
            connection.close()