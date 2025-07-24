from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col  # 新增必要的函数导入

spark = SparkSession.builder \
    .appName("HiveToKafka") \
    .enableHiveSupport() \
    .getOrCreate()

# 刷新分区并读取数据
spark.sql("MSCK REPAIR TABLE dwd_spot.dwd_balance_di")
df = spark.table("dwd_spot.dwd_balance_di").where("dt = '2025-03-22'")  # 替换为实际日期

# 转换为Kafka所需格式（key-value结构）
kafka_df = df.select(
    col("_id").cast("string").alias("key"),
    to_json(struct("*")).alias("value")     # 将所有列转为JSON作为value
)

# 写入Kafka
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "b-3.cexkafkadatatest0.mnm8xv.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-1.cexkafkadatatest0.mnm8xv.c5.kafka.ap-southeast-1.amazonaws.com:9092,b-2.cexkafkadatatest0.mnm8xv.c5.kafka.ap-southeast-1.amazonaws.com:9092") \
    .option("topic", "s3_hive_to_kafka") \
    .save()

spark.stop()