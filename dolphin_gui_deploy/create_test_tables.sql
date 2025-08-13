-- DWS层用户指标宽表测试环境
-- 基于 model/dws_user_index.sql 中的表结构，适配49个用户指标
-- 测试环境S3路径前缀：exchanges-flink-test

-- 1. 用户基础画像表 - 注册信息和身份属性
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_profile_df (
  user_id STRING COMMENT '用户ID',
  
  -- 注册基础信息
  register_time STRING COMMENT '注册时间',
  register_source_channel STRING COMMENT '注册来源渠道',
  register_method STRING COMMENT '注册方式',
  register_country STRING COMMENT '注册国家',
  
  -- 身份认证信息
  is_kyc_completed STRING COMMENT '是否完成KYC',
  kyc_country STRING COMMENT 'KYC国家',
  is_2fa_enabled STRING COMMENT '是否绑定二次验证',
  user_level STRING COMMENT '用户等级',
  is_agent STRING COMMENT '是否为代理'
  
) COMMENT '用户基础画像信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_profile_df/';


-- 2. 用户资产财务表 - 资产相关的数值指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_asset_df (
  user_id STRING COMMENT '用户ID',
  
  -- 充值提现相关
  total_deposit_amount STRING COMMENT '累计充值金额',
  total_withdraw_amount STRING COMMENT '累计提现金额', 
  net_deposit_amount STRING COMMENT '累计净入金金额',
  last_deposit_time STRING COMMENT '最近一次充值时间',
  last_withdraw_time STRING COMMENT '最近一次提现时间',
  withdraw_count_30d STRING COMMENT '提现次数（近30日）',
  deposit_fail_count STRING COMMENT '充值失败次数',
  
  -- 当前资产状况 - 按账户类型细分
  spot_position_value STRING COMMENT '现货账户持仓金额',
  contract_position_value STRING COMMENT '合约账户持仓金额',
  finance_position_value STRING COMMENT '理财账户持仓金额',
  onchain_position_value STRING COMMENT '链上账户持仓金额',
  current_total_position_value STRING COMMENT '总持仓市值（当前）',
  
  -- 可用余额 - 按账户类型细分
  spot_available_balance STRING COMMENT '现货账户可用余额',
  contract_available_balance STRING COMMENT '合约账户可用余额',
  onchain_available_balance STRING COMMENT '链上账户可用余额',
  available_balance STRING COMMENT '总可用余额',
  
  -- 锁仓金额 - 按账户类型细分
  spot_locked_amount STRING COMMENT '现货锁仓金额',
  contract_locked_amount STRING COMMENT '合约锁仓金额',
  finance_locked_amount STRING COMMENT '理财锁仓金额',
  onchain_locked_amount STRING COMMENT '链上锁仓金额',
  locked_amount STRING COMMENT '总锁仓金额'
  
) COMMENT '用户资产财务表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_asset_df/';


-- 3. 用户交易行为表 - 交易相关指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_trading_df (
  user_id STRING COMMENT '用户ID',
  
  -- 交易金额统计 - 按业务类型细分
  spot_trading_volume STRING COMMENT '现货总交易金额',
  contract_trading_volume STRING COMMENT '合约总交易金额',
  
  -- 最近30日交易额 - 按业务类型细分
  spot_recent_30d_volume STRING COMMENT '现货最近30日交易额',
  contract_recent_30d_volume STRING COMMENT '合约最近30日交易额',
  
  -- 交易次数统计 - 按业务类型细分
  spot_trade_count STRING COMMENT '现货交易次数',
  contract_trade_count STRING COMMENT '合约交易次数',
  finance_trade_count STRING COMMENT '理财交易次数',
  onchain_trade_count STRING COMMENT '链上交易次数',
  
  -- 交易时间相关
  first_trade_time STRING COMMENT '首次交易时间',
  last_trade_time STRING COMMENT '最后交易时间',
  
  -- 交易行为特征
  has_contract_trading STRING COMMENT '是否有合约交易',
  contract_trading_style STRING COMMENT '合约交易风格',
  has_finance_management STRING COMMENT '是否开通过理财',
  has_pending_orders STRING COMMENT '是否有挂单行为'
  
) COMMENT '用户交易行为表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_trading_df/';


-- 4. 用户活跃行为表 - 登录活跃相关指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_activity_df (
  user_id STRING COMMENT '用户ID',
  
  -- 时间维度指标
  days_since_register STRING COMMENT '注册至今天数',
  days_since_last_login STRING COMMENT '最近登录距今天数',
  last_login_time STRING COMMENT '最近登录时间',
  last_activity_time STRING COMMENT '最后活动时间',
  
  -- 活跃统计指标
  login_count_7d STRING COMMENT '登录次数（近7日）',
  
  -- 设备和地理信息
  login_ip_address STRING COMMENT '登录IP地址',
  country_region_code STRING COMMENT '国家地区编码',
  email_suffix STRING COMMENT '邮箱后缀',
  operating_system STRING COMMENT '操作系统'
  
) COMMENT '用户活跃行为表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_activity_df/';


-- 5. 用户风险风控表 - 风险相关指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_risk_df (
  user_id STRING COMMENT '用户ID',
  
  -- 风险标识
  is_blacklist_user STRING COMMENT '是否为黑名单用户',
  is_high_risk_ip STRING COMMENT '是否为高风险IP',
  
  -- 渠道风险
  channel_source STRING COMMENT '渠道来源'
  
) COMMENT '用户风险风控表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_risk_df/';


-- 6. 用户营销激励表 - 运营活动相关指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_marketing_df (
  user_id STRING COMMENT '用户ID',
  
  -- 激励统计
  red_packet_count STRING COMMENT '红包领取次数',
  
  -- 邀请返佣
  successful_invites_count STRING COMMENT '好友邀请成功数',
  commission_rate STRING COMMENT '返佣比例',
  total_commission_amount STRING COMMENT '返佣金额（累计）'
  
) COMMENT '用户营销激励表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_marketing_df/';


-- 7. 用户行为偏好表 - 列表类型指标（使用array类型）
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user.dws_user_behavior_df (
  user_id STRING COMMENT '用户ID',
  
  -- 列表类型指标 - 使用array<string>类型更合理
  current_holding_coins ARRAY<STRING> COMMENT '当前持仓币种列表',
  traded_coins_list ARRAY<STRING> COMMENT '交易过的币种列表',  
  device_fingerprint_list ARRAY<STRING> COMMENT '登录设备指纹列表',
  participated_activity_ids ARRAY<STRING> COMMENT '参与过的活动ID列表',
  reward_claim_history ARRAY<STRING> COMMENT '奖励领取历史（类型）',
  used_coupon_types ARRAY<STRING> COMMENT '使用过的卡券类型'
  
) COMMENT '用户行为偏好表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/dws/dws_user/dws_user_behavior_df/';
