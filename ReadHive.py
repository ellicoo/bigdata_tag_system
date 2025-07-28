from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("ReadHive") \
    .enableHiveSupport() \
    .getOrCreate()

# 刷新分区并读取数据
spark.sql("MSCK REPAIR TABLE dwd_spot.dwd_balance_di")
df = spark.table("dwd_spot.dwd_balance_di")

df.show()

spark.stop()