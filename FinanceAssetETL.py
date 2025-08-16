from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime, timedelta

# 创建Spark会话
spark = SparkSession.builder \
    .appName("FinanceAssetETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()


def get_latest_partitions():
    """获取最新的天分区和小时分区"""
    print("Getting latest available partitions from data...")

    try:
        # 先获取最大的天分区
        latest_dt_df = spark.sql("""
            SELECT MAX(dt) as max_dt 
            FROM ods_src_mongo_spot.ods_financeasset
        """)

        latest_dt = latest_dt_df.collect()[0]['max_dt']

        if latest_dt is None:
            raise Exception("No data found in table")

        print(f"Found latest date partition: {latest_dt}")

        # 再获取该天分区下的最大小时分区
        latest_ht_df = spark.sql(f"""
            SELECT MAX(ht) as max_ht 
            FROM ods_src_mongo_spot.ods_financeasset 
            WHERE dt = '{latest_dt}'
        """)

        latest_ht = latest_ht_df.collect()[0]['max_ht']

        if latest_ht is None:
            raise Exception(f"No hour partition found for date {latest_dt}")

        print(f"Found latest hour partition: {latest_ht}")
        return latest_dt, latest_ht

    except Exception as e:
        print(f"Error getting latest partitions: {e}")
        raise


def parse_balance_json(df):
    """解析balance JSON字段"""
    balance_schema = ArrayType(StructType([
        StructField("product_type", StringType(), True),
        StructField("currency_id", StringType(), True),
        StructField("balance_amount", StringType(), True)
    ]))

    def extract_balance(balance_str):
        if not balance_str:
            return []
        try:
            balance_dict = json.loads(balance_str)
            result = []
            for product_type, symbols in balance_dict.items():
                if isinstance(symbols, dict):
                    for currency_id, amount in symbols.items():
                        result.append({
                            "product_type": product_type,
                            "currency_id": currency_id,
                            "balance_amount": str(amount)
                        })
            return result
        except:
            return []

    extract_balance_udf = udf(extract_balance, balance_schema)

    df_balance = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_balance_udf(col("balance"))).alias("balance_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id").cast("string"),
        col("user_id").cast("string"),
        col("tenant_id").cast("string"),
        col("offset_value").cast("string"),
        col("balance_data.product_type").alias("product_type"),
        col("balance_data.currency_id").alias("currency_id"),
        col("balance_data.balance_amount").alias("balance_amount"),
        lit(None).alias("acc_interest"),
        lit(None).alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("balance").alias("data_source"),
        col("updated_time")
    )

    return df_balance


def parse_accinterest_json(df):
    """解析accinterest JSON字段"""
    accinterest_schema = ArrayType(StructType([
        StructField("product_type", StringType(), True),
        StructField("currency_id", StringType(), True),
        StructField("acc_interest", StringType(), True)
    ]))

    def extract_accinterest(accinterest_str):
        if not accinterest_str:
            return []
        try:
            accinterest_dict = json.loads(accinterest_str)
            result = []
            for product_type, currencies in accinterest_dict.items():
                if isinstance(currencies, dict):
                    for currency_id, interest in currencies.items():
                        result.append({
                            "product_type": product_type,
                            "currency_id": currency_id,
                            "acc_interest": str(interest)
                        })
            return result
        except:
            return []

    extract_accinterest_udf = udf(extract_accinterest, accinterest_schema)

    df_accinterest = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_accinterest_udf(col("accinterest"))).alias("accinterest_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("accinterest_data.product_type").alias("product_type"),
        col("accinterest_data.currency_id").alias("currency_id"),
        lit(None).alias("balance_amount"),
        col("accinterest_data.acc_interest").alias("acc_interest"),
        lit(None).alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("accinterest").alias("data_source"),
        col("updated_time")
    )

    return df_accinterest


def parse_lastinterest_json(df):
    """解析lastinterest JSON字段"""
    lastinterest_schema = ArrayType(StructType([
        StructField("last_interest_date", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("currency_id", StringType(), True),
        StructField("last_interest_amount", StringType(), True)
    ]))

    def extract_lastinterest(lastinterest_str):
        if not lastinterest_str:
            return []
        try:
            lastinterest_dict = json.loads(lastinterest_str)
            result = []
            for date, products in lastinterest_dict.items():
                if isinstance(products, dict):
                    for product_type, currencies in products.items():
                        if isinstance(currencies, dict):
                            for currency_id, amount in currencies.items():
                                result.append({
                                    "last_interest_date": date,
                                    "product_type": product_type,
                                    "currency_id": currency_id,
                                    "last_interest_amount": str(amount)
                                })
            return result
        except:
            return []

    extract_lastinterest_udf = udf(extract_lastinterest, lastinterest_schema)

    df_lastinterest = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_lastinterest_udf(col("lastinterest"))).alias("lastinterest_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("lastinterest_data.product_type").alias("product_type"),
        col("lastinterest_data.currency_id").alias("currency_id"),
        lit(None).alias("balance_amount"),
        lit(None).alias("acc_interest"),
        col("lastinterest_data.last_interest_date").alias("last_interest_date"),
        col("lastinterest_data.last_interest_amount").alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("lastinterest").alias("data_source"),
        col("updated_time")
    )

    return df_lastinterest


def parse_accinterestu_json(df):
    """解析accinterestu JSON字段"""
    accinterestu_schema = ArrayType(StructType([
        StructField("product_type", StringType(), True),
        StructField("acc_interest_u", StringType(), True)
    ]))

    def extract_accinterestu(accinterestu_str):
        if not accinterestu_str:
            return []
        try:
            accinterestu_dict = json.loads(accinterestu_str)
            result = []
            for product_type, interest in accinterestu_dict.items():
                result.append({
                    "product_type": product_type,
                    "acc_interest_u": str(interest)
                })
            return result
        except:
            return []

    extract_accinterestu_udf = udf(extract_accinterestu, accinterestu_schema)

    df_accinterestu = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_accinterestu_udf(col("accinterestu"))).alias("accinterestu_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("accinterestu_data.product_type").alias("product_type"),
        lit(None).alias("currency_id"),
        lit(None).alias("balance_amount"),
        lit(None).alias("acc_interest"),
        lit(None).alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        col("accinterestu_data.acc_interest_u").alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("accinterestu").alias("data_source"),
        col("updated_time")
    )

    return df_accinterestu


def parse_lastinterestu_json(df):
    """解析lastinterestu JSON字段"""
    lastinterestu_schema = ArrayType(StructType([
        StructField("last_interest_date", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("last_interest_u", StringType(), True)
    ]))

    def extract_lastinterestu(lastinterestu_str):
        if not lastinterestu_str:
            return []
        try:
            lastinterestu_dict = json.loads(lastinterestu_str)
            result = []
            for date, products in lastinterestu_dict.items():
                if isinstance(products, dict):
                    for product_type, amount in products.items():
                        result.append({
                            "last_interest_date": date,
                            "product_type": product_type,
                            "last_interest_u": str(amount)
                        })
            return result
        except:
            return []

    extract_lastinterestu_udf = udf(extract_lastinterestu, lastinterestu_schema)

    df_lastinterestu = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_lastinterestu_udf(col("lastinterestu"))).alias("lastinterestu_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("lastinterestu_data.product_type").alias("product_type"),
        lit(None).alias("currency_id"),
        lit(None).alias("balance_amount"),
        lit(None).alias("acc_interest"),
        col("lastinterestu_data.last_interest_date").alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        col("lastinterestu_data.last_interest_u").alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("lastinterestu").alias("data_source"),
        col("updated_time")
    )

    return df_lastinterestu


def parse_accinterestbtc_json(df):
    """解析accinterestbtc JSON字段"""
    accinterestbtc_schema = ArrayType(StructType([
        StructField("product_type", StringType(), True),
        StructField("acc_interest_btc", StringType(), True)
    ]))

    def extract_accinterestbtc(accinterestbtc_str):
        if not accinterestbtc_str:
            return []
        try:
            accinterestbtc_dict = json.loads(accinterestbtc_str)
            result = []
            for product_type, interest in accinterestbtc_dict.items():
                result.append({
                    "product_type": product_type,
                    "acc_interest_btc": str(interest)
                })
            return result
        except:
            return []

    extract_accinterestbtc_udf = udf(extract_accinterestbtc, accinterestbtc_schema)

    df_accinterestbtc = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_accinterestbtc_udf(col("accinterestbtc"))).alias("accinterestbtc_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("accinterestbtc_data.product_type").alias("product_type"),
        lit(None).alias("currency_id"),
        lit(None).alias("balance_amount"),
        lit(None).alias("acc_interest"),
        lit(None).alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        col("accinterestbtc_data.acc_interest_btc").alias("acc_interest_btc"),
        lit(None).alias("last_interest_btc"),
        lit("accinterestbtc").alias("data_source"),
        col("updated_time")
    )

    return df_accinterestbtc


def parse_lastinterestbtc_json(df):
    """解析lastinterestbtc JSON字段"""
    lastinterestbtc_schema = ArrayType(StructType([
        StructField("last_interest_date", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("last_interest_btc", StringType(), True)
    ]))

    def extract_lastinterestbtc(lastinterestbtc_str):
        if not lastinterestbtc_str:
            return []
        try:
            lastinterestbtc_dict = json.loads(lastinterestbtc_str)
            result = []
            for date, products in lastinterestbtc_dict.items():
                if isinstance(products, dict):
                    for product_type, amount in products.items():
                        result.append({
                            "last_interest_date": date,
                            "product_type": product_type,
                            "last_interest_btc": str(amount)
                        })
            return result
        except:
            return []

    extract_lastinterestbtc_udf = udf(extract_lastinterestbtc, lastinterestbtc_schema)

    df_lastinterestbtc = df.select(
        col("_id").alias("record_id"),
        col("accountid").alias("account_id"),
        col("userid").alias("user_id"),
        col("tenantid").alias("tenant_id"),
        col("offset").alias("offset_value"),
        explode(extract_lastinterestbtc_udf(col("lastinterestbtc"))).alias("lastinterestbtc_data"),
        col("cur_time").alias("updated_time")
    ).select(
        col("record_id"),
        col("account_id"),
        col("user_id"),
        col("tenant_id"),
        col("offset_value"),
        col("lastinterestbtc_data.product_type").alias("product_type"),
        lit(None).alias("currency_id"),
        lit(None).alias("balance_amount"),
        lit(None).alias("acc_interest"),
        col("lastinterestbtc_data.last_interest_date").alias("last_interest_date"),
        lit(None).alias("last_interest_amount"),
        lit(None).alias("acc_interest_u"),
        lit(None).alias("last_interest_u"),
        lit(None).alias("acc_interest_btc"),
        col("lastinterestbtc_data.last_interest_btc").alias("last_interest_btc"),
        lit("lastinterestbtc").alias("data_source"),
        col("updated_time")
    )

    return df_lastinterestbtc


def main():
    """主处理函数"""

    # 调试模式：获取实际存在的最新分区，而不是当天日期
    # target_dt = datetime.now().strftime('%Y-%m-%d')  # 调试时注释掉

    # 调试信息：检查当前环境
    print("=== Environment Debug Information ===")
    try:
        print("Current catalog:")
        spark.sql("SELECT current_catalog()").show()

        print("Current database:")
        spark.sql("SELECT current_database()").show()

        print("Available databases:")
        spark.sql("SHOW DATABASES").show()

        # 检查特定数据库是否存在
        databases = spark.sql("SHOW DATABASES").collect()
        db_names = [row[0] for row in databases]
        if 'ods_src_mongo_spot' in db_names:
            print("Database 'ods_src_mongo_spot' exists")
            print("Tables in ods_src_mongo_spot:")
            spark.sql("SHOW TABLES IN ods_src_mongo_spot").show()
        else:
            print("Database 'ods_src_mongo_spot' does NOT exist")
            print("Creating database ods_src_mongo_spot...")
            spark.sql("CREATE DATABASE IF NOT EXISTS ods_src_mongo_spot")

    except Exception as e:
        print(f"Debug information error: {e}")

    # 刷新表元数据
    print("Refreshing table metadata...")
    try:
        spark.sql("REFRESH TABLE ods_src_mongo_spot.ods_financeasset")
        print("Table metadata refreshed successfully")
    except Exception as e:
        print(f"Warning: Failed to refresh table metadata: {e}")
        print("Continuing without refresh...")

    # 获取最新的天分区和小时分区
    target_dt, latest_ht = get_latest_partitions()
    print(f"Processing data for partition: dt={target_dt}, ht={latest_ht}")

    # 显示可用的分区信息，便于调试
    print("Available partitions in the table:")
    spark.sql("""
        SELECT dt, ht, COUNT(*) as record_count
        FROM ods_src_mongo_spot.ods_financeasset 
        GROUP BY dt, ht 
        ORDER BY dt DESC, ht DESC 
        LIMIT 10
    """).show()

    # 读取ODS源表的最新小时分区数据
    df_ods = spark.sql(f"""
        SELECT `_id`, accountid, userid, tenantid, offset, 
               balance, accinterest, lastinterest, 
               accinterestu, lastinterestu, accinterestbtc, lastinterestbtc,
               cur_time, dt, ht
        FROM ods_src_mongo_spot.ods_financeasset 
        WHERE dt = '{target_dt}' AND ht = '{latest_ht}'
    """)

    # 检查数据量
    ods_count = df_ods.count()
    print(f"ODS source records count: {ods_count}")

    if ods_count == 0:
        print("No data found in ODS table, exiting...")
        return

    # 分别解析各个JSON字段
    df_balance = parse_balance_json(df_ods.filter(col("balance").isNotNull()))
    df_accinterest = parse_accinterest_json(df_ods.filter(col("accinterest").isNotNull()))
    df_lastinterest = parse_lastinterest_json(df_ods.filter(col("lastinterest").isNotNull()))
    df_accinterestu = parse_accinterestu_json(df_ods.filter(col("accinterestu").isNotNull()))
    df_lastinterestu = parse_lastinterestu_json(df_ods.filter(col("lastinterestu").isNotNull()))
    df_accinterestbtc = parse_accinterestbtc_json(df_ods.filter(col("accinterestbtc").isNotNull()))
    df_lastinterestbtc = parse_lastinterestbtc_json(df_ods.filter(col("lastinterestbtc").isNotNull()))

    # 合并所有数据
    df_final = df_balance.union(df_accinterest) \
        .union(df_lastinterest) \
        .union(df_accinterestu) \
        .union(df_lastinterestu) \
        .union(df_accinterestbtc) \
        .union(df_lastinterestbtc)

    # 数据质量检查
    balance_count = df_balance.count()
    accinterest_count = df_accinterest.count()
    lastinterest_count = df_lastinterest.count()
    accinterestu_count = df_accinterestu.count()
    lastinterestu_count = df_lastinterestu.count()
    accinterestbtc_count = df_accinterestbtc.count()
    lastinterestbtc_count = df_lastinterestbtc.count()
    final_count = df_final.count()

    print(f"Balance records: {balance_count}")
    print(f"AccInterest records: {accinterest_count}")
    print(f"LastInterest records: {lastinterest_count}")
    print(f"AccInterestU records: {accinterestu_count}")
    print(f"LastInterestU records: {lastinterestu_count}")
    print(f"AccInterestBTC records: {accinterestbtc_count}")
    print(f"LastInterestBTC records: {lastinterestbtc_count}")
    print(f"Total records: {final_count}")

    # 统计各数据源记录数
    df_final.groupBy("data_source").agg(
        count("*").alias("record_count"),
        countDistinct("user_id").alias("user_count"),
        countDistinct("account_id").alias("account_count")
    ).show()

    # 写入DWD表，使用ODS的最新分区值
    print(f"Writing to DWD table with partitions: dt={target_dt}, ht={latest_ht}")

    # 为写入添加分区字段
    df_final_with_partition = df_final.withColumn("dt", lit(target_dt)) \
        .withColumn("ht", lit(latest_ht))

    # 删除目标分区（如果存在）
    try:
        spark.sql(f"""
            ALTER TABLE dwd_spot.dwd_finance_asset_df 
            DROP IF EXISTS PARTITION (dt='{target_dt}', ht='{latest_ht}')
        """)
        print(f"Dropped existing partition: dt={target_dt}, ht={latest_ht}")
    except Exception as e:
        print(f"Warning: Could not drop partition (may not exist): {e}")

    # 写入数据
    df_final_with_partition.write \
        .mode("overwrite") \
        .option("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .partitionBy("dt", "ht") \
        .option("path", "s3://exchanges-flink-test/batch/data/dwd/dwd_spot/dwd_finance_asset_df") \
        .saveAsTable("dwd_spot.dwd_finance_asset_df")

    print(f"ETL处理完成! 处理分区: dt={target_dt}, ht={latest_ht}")


if __name__ == "__main__":
    main()
    spark.stop()