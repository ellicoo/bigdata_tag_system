-- 标签系统测试表
-- 基于现有 crate_table_demo.sql 格式，支持 json_demo.txt 中的所有字段

-- 用户基本信息表（扩展所有字段）
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_basic_info (
    user_id string COMMENT '用户ID',
    age int COMMENT '年龄',
    user_level string COMMENT '用户等级',
    registration_date string COMMENT '注册日期',
    birthday string COMMENT '生日',
    first_name string COMMENT '名字',
    last_name string COMMENT '姓氏',
    middle_name string COMMENT '中间名',
    phone_number string COMMENT '电话号码',
    email string COMMENT '邮箱',
    is_vip boolean COMMENT '是否VIP',
    is_banned boolean COMMENT '是否被封禁',
    kyc_status string COMMENT 'KYC状态',
    account_status string COMMENT '账户状态',
    primary_status string COMMENT '主要状态',
    secondary_status string COMMENT '次要状态'
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

-- 用户资产汇总表（扩展字段）
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_asset_summary (
    user_id string COMMENT '用户ID',
    total_asset_value double COMMENT '总资产价值',
    cash_balance double COMMENT '现金余额',
    debt_amount double COMMENT '债务金额'
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

-- 用户活动汇总表（扩展字段）
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_activity_summary (
    user_id string COMMENT '用户ID',
    trade_count_30d int COMMENT '30天交易次数',
    last_login_date string COMMENT '最后登录日期',
    last_trade_date string COMMENT '最后交易日期'
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

-- 用户风险档案表（新增）
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_risk_profile (
    user_id string COMMENT '用户ID',
    risk_score int COMMENT '风险评分'
) COMMENT '用户风险档案表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_risk_profile/';

-- 用户偏好表（新增）
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_preferences (
    user_id string COMMENT '用户ID',
    interested_products array<string> COMMENT '感兴趣的产品',
    owned_products array<string> COMMENT '拥有的产品',
    blacklisted_products array<string> COMMENT '黑名单产品',
    active_products array<string> COMMENT '活跃产品',
    expired_products array<string> COMMENT '过期产品',
    optional_services array<string> COMMENT '可选服务',
    required_services array<string> COMMENT '必需服务'
) COMMENT '用户偏好表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_preferences/';
