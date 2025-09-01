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

        print(f"🔗 JDBC URL: {self.jdbcUrl}")

        # print(f"🔗 JDBC URL: {self.jdbcUrl}")
        print("🔧 开始MySQL连接测试...")
        
        # 强制执行连接测试并显示结果
        try:
            connection_ok = self.testConnection()
            if connection_ok:
                print("✅ MySQL连接测试通过")
            else:
                print("❌ MySQL连接测试失败，但继续初始化")
        except Exception as e:
            print(f"❌ 连接测试异常: {e}")
            print("⚠️  将继续初始化，但可能影响后续操作")
        
        print("🗄️  MysqlMeta初始化完成")
    
    def _buildJdbcUrl(self) -> str:
        """构建JDBC连接URL"""
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        database = self.mysqlConfig['database']
        
        # 使用connectionCollation参数支持utf8mb4编码
        # return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&serverTimezone=UTC"
        
        # 添加业务方必要的连接参数：autoReconnect=true & useCursorFetch=true
        return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&autoReconnect=true&useCursorFetch=true&serverTimezone=UTC"
        
        # 完全匹配业务方配置，解决连接问题
        # return f"jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=utf8&useSSL=false&autoReconnect=true&useCursorFetch=true"


    def loadTagRules(self, tagIds: Optional[List[int]] = None) -> DataFrame:
        """加载标签规则DataFrame
        
        Args:
            tagIds: 指定加载的标签ID列表，None表示加载所有活跃标签
            
        Returns:
            DataFrame: 标签规则DataFrame，包含字段：tag_id, rule_conditions, tag_name
        """
        print(f"📋 加载标签规则，指定标签: {tagIds}")
        
        # 构建查询SQL - 更新为新的表结构
        query = """
        (SELECT trc.tag_id, trc.tag_conditions as rule_conditions, td.tag_name, td.description
         FROM tag_rules_config trc
         LEFT JOIN tag_definition td ON trc.tag_id = td.id
         WHERE td.is_active = 1
        """
        
        if tagIds:
            tagIdsStr = ','.join(map(str, tagIds))
            query += f" AND trc.tag_id IN ({tagIdsStr})"
        
        query += " ORDER BY trc.tag_id) as tag_rules_config"
        
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
            DataFrame: 现有标签DataFrame，包含字段：user_id, existing_tag_ids(Array), created_time, updated_time
        """
        print("📖 加载现有用户标签数据...")
        
        query = "(SELECT user_id, tag_id_list, created_time, updated_time FROM user_tag_relation WHERE tag_id_list IS NOT NULL) as existing_tags"
        
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
                json_to_array(col("tag_id_list"))
            ).select("user_id", "existing_tag_ids", "created_time", "updated_time")
            
            print(f"✅ 现有标签数据加载完成: {existingDF.count()} 个用户")
            return existingDF
            
        except Exception as e:
            print(f"❌ 加载现有标签数据失败: {e}")
            return self._createEmptyExistingTagsDataFrame()
    
    def writeTagResultsOverwrite(self, resultsDF: DataFrame) -> bool:
        """使用overwrite直接覆盖写入 - 高性能方案
        
        利用Spark JDBC的overwrite模式，结合智能时间戳处理，
        一步到位完成标签数据的完整覆盖更新，避免临时表的复杂性。
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json, created_time, updated_time
            
        Returns:
            bool: 写入是否成功
        """
        print("🚀 开始overwrite写入标签结果到MySQL（直接覆盖方案）...")
        
        try:
            # 检查数据完整性
            total_count = resultsDF.count()
            if total_count == 0:
                print("⚠️  没有结果需要写入")
                return True
                
            print(f"📤 准备overwrite写入 {total_count} 条完整标签记录...")
            
            # 🎯 优化JDBC连接参数
            optimized_url = f"{self.jdbcUrl}&rewriteBatchedStatements=true&useServerPrepStmts=false"
            
            # 🎯 动态分区优化：避免不必要的shuffle操作
            import builtins  # 避免与PySpark的min函数冲突
            current_partitions = resultsDF.rdd.getNumPartitions()
            optimal_partitions = builtins.min(current_partitions, 8)  # 最多8个并发连接，避免过多连接
            
            print(f"   📊 分区优化: 当前{current_partitions}个分区 → 优化为{optimal_partitions}个分区")
            
            # 🚀 直接overwrite写入：一步到位，无中间环节
            write_df = resultsDF.select(
                "user_id", 
                col("final_tag_ids_json").alias("tag_id_list"),
                "created_time",
                "updated_time"
            )
            
            # 只在需要时进行分区调整
            if current_partitions > 8:
                write_df = write_df.coalesce(optimal_partitions)
                
            # 🚀 方案：先TRUNCATE清空表，再INSERT数据（避免DROP权限问题）
            try:
                # 步骤1：使用PyMySQL清空表（TRUNCATE不需要DROP权限）
                print("   🗑️  清空现有表数据...")
                connection = pymysql.connect(**self.mysqlConfig)
                cursor = connection.cursor()
                cursor.execute("TRUNCATE TABLE user_tag_relation")
                connection.commit()
                cursor.close()
                connection.close()
                print("   ✅ 表数据清空完成")
                
                # 步骤2：使用Spark JDBC append模式写入新数据
                print("   📤 写入新数据...")
                write_df.write \
                    .format("jdbc") \
                    .option("url", optimized_url) \
                    .option("dbtable", "user_tag_relation") \
                    .option("user", self.mysqlConfig['user']) \
                    .option("password", self.mysqlConfig['password']) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("batchsize", "2000") \
                    .option("isolationLevel", "READ_COMMITTED") \
                    .mode("append") \
                    .save()
                    
            except Exception as truncate_error:
                print(f"   ⚠️  TRUNCATE+APPEND方案失败: {truncate_error}")
                print("   🔄 尝试直接append模式（可能有重复数据）...")
                # 备选方案：直接append（业务层面需要处理重复）
                write_df.write \
                    .format("jdbc") \
                    .option("url", optimized_url) \
                    .option("dbtable", "user_tag_relation") \
                    .option("user", self.mysqlConfig['user']) \
                    .option("password", self.mysqlConfig['password']) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("batchsize", "2000") \
                    .option("isolationLevel", "READ_COMMITTED") \
                    .mode("append") \
                    .save()
                
            print(f"✅ Overwrite写入完成: {total_count} 条记录")
            return True
            
        except Exception as e:
            print(f"❌ Overwrite写入失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    
    def testConnection(self) -> bool:
        """测试MySQL连接
        
        Returns:
            bool: 连接是否成功
        """
        # 先测试网络连通性
        import socket
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        
        print(f"🔍 测试网络连通性: {host}:{port}")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # 10秒超时
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result != 0:
                print(f"❌ 网络连接失败: {host}:{port} (错误码: {result})")
                print("   可能原因: 1) 安全组限制 2) 防火墙限制 3) 网络不通")
                return False
            else:
                print(f"✅ 网络连接正常: {host}:{port}")
        except Exception as e:
            print(f"❌ 网络测试异常: {e}")
            return False
        
        # 然后测试MySQL连接
        print("🔍 测试MySQL JDBC连接...")
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
            StructField("existing_tag_ids", ArrayType(IntegerType()), True),
            StructField("created_time", TimestampType(), True),
            StructField("updated_time", TimestampType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
