-- 标签系统测试表
-- 基于现有 crate_table_demo.sql 格式

-- 用户基本信息表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_test.user_basic_info (
    user_id string COMMENT '用户ID',
    age int COMMENT '年龄',
    user_level string COMMENT '用户等级',
    kyc_status string COMMENT 'KYC状态',
    registration_date string COMMENT '注册日期',
    risk_score double COMMENT '风险评分'
) COMMENT '用户基本信息表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_basic_info/';

-- 用户资产汇总表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_test.user_asset_summary (
    user_id string COMMENT '用户ID',
    total_asset_value double COMMENT '总资产价值',
    cash_balance double COMMENT '现金余额'
) COMMENT '用户资产汇总表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_asset_summary/';

-- 用户活动汇总表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_test.user_activity_summary (
    user_id string COMMENT '用户ID',
    trade_count_30d int COMMENT '30天交易次数',
    last_login_date string COMMENT '最后登录日期'
) COMMENT '用户活动汇总表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_activity_summary/';
