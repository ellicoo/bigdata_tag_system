-- 完整的标签规则插入示例，基于新DWS层表结构，覆盖所有49个指标和所有操作符
INSERT INTO tag_rules_config (tag_id, tag_conditions) VALUES
-- 数值类型指标规则 - 覆盖所有17个数值指标 (=, !=, >, <, >=, <=, in_range, not_in_range, is_null, is_not_null)
(1, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "total_deposit_amount", "operator": "=", "value": "100000", "type": "number"}]}}]}'),
(2, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "total_withdraw_amount", "operator": "!=", "value": "0", "type": "number"}]}}]}'),
(3, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "contract_recent_30d_volume", "operator": ">", "value": "10000", "type": "number"}]}}]}'),
(4, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "login_count_7d", "operator": "<", "value": "5", "type": "number"}]}}]}'),
(5, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "available_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),
(6, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "spot_commission_rate", "operator": "<=", "value": "30", "type": "number"}]}}]}'),
(7, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_register", "operator": "in_range", "value": ["30", "365"], "type": "number"}]}}]}'),
(8, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "withdraw_count_30d", "operator": "not_in_range", "value": ["0", "3"], "type": "number"}]}}]}'),
(9, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "locked_amount", "operator": "is_null", "value": "", "type": "number"}]}}]}'),
(10, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": "is_not_null", "value": "", "type": "number"}]}}]}'),
(11, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "net_deposit_amount", "operator": ">", "value": "0", "type": "number"}]}}]}'),
(12, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "spot_trading_volume", "operator": ">=", "value": "100000", "type": "number"}]}}]}'),
(13, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "deposit_fail_count", "operator": "<=", "value": "3", "type": "number"}]}}]}'),
(14, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_last_login", "operator": "in_range", "value": ["1", "7"], "type": "number"}]}}]}'),
(15, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "red_packet_count", "operator": "not_in_range", "value": ["0", "2"], "type": "number"}]}}]}'),
(16, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "successful_invites_count", "operator": "is_null", "value": "", "type": "number"}]}}]}'),
(17, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "total_commission_amount", "operator": "is_not_null", "value": "", "type": "number"}]}}]}'),

-- 字符串类型指标规则 - 覆盖所有5个字符串指标 (=, !=, contains, not_contains, starts_with, ends_with, is_null, is_not_null)
(18, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "login_ip_address", "operator": "=", "value": "192.168.1.1", "type": "string"}]}}]}'),
(19, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "country_region_code", "operator": "!=", "value": "+1", "type": "string"}]}}]}'),
(20, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "email_suffix", "operator": "contains", "value": "gmail", "type": "string"}]}}]}'),
(21, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "register_source_channel", "operator": "not_contains", "value": "spam", "type": "string"}]}}]}'),
(22, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "country_region_code", "operator": "starts_with", "value": "+86", "type": "string"}]}}]}'),
(23, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "email_suffix", "operator": "ends_with", "value": ".com", "type": "string"}]}}]}'),
(24, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "operating_system", "operator": "is_null", "value": "", "type": "string"}]}}]}'),
(25, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "login_ip_address", "operator": "is_not_null", "value": "", "type": "string"}]}}]}'),

-- 日期类型指标规则 - 覆盖所有7个日期指标 (=, !=, >, <, >=, <=, date_in_range, date_not_in_range, is_null, is_not_null)
(26, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "register_time", "operator": "=", "value": "2025-01-01", "type": "date"}]}}]}'),
(27, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "last_login_time", "operator": "!=", "value": "2025-01-01", "type": "date"}]}}]}'),
(28, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "last_activity_time", "operator": ">", "value": "2025-01-01", "type": "date"}]}}]}'),
(29, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "last_deposit_time", "operator": "<", "value": "2024-12-31", "type": "date"}]}}]}'),
(30, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "last_withdraw_time", "operator": ">=", "value": "2024-01-01", "type": "date"}]}}]}'),
(31, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "first_trade_time", "operator": "<=", "value": "2025-12-31", "type": "date"}]}}]}'),
(32, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "last_trade_time", "operator": "date_in_range", "value": ["2024-01-01", "2024-12-31"], "type": "date"}]}}]}'),
(33, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "last_login_time", "operator": "date_not_in_range", "value": ["2023-01-01", "2023-12-31"], "type": "date"}]}}]}'),
(34, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "first_trade_time", "operator": "is_null", "value": "", "type": "date"}]}}]}'),
(35, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "register_time", "operator": "is_not_null", "value": "", "type": "date"}]}}]}'),

-- 布尔类型指标规则 - 覆盖所有8个布尔指标 (is_true/=, is_false/!=)
(36, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_kyc_completed", "operator": "is_true", "value": "true", "type": "boolean"}]}}]}'),
(37, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_2fa_enabled", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),
(38, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "=", "value": "false", "type": "boolean"}]}}]}'),
(39, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_high_risk_ip", "operator": "!=", "value": "true", "type": "boolean"}]}}]}'),
(40, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "has_contract_trading", "operator": "is_true", "value": "true", "type": "boolean"}]}}]}'),
(41, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "has_finance_management", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),
(42, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "has_pending_orders", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),
(43, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_agent", "operator": "!=", "value": "true", "type": "boolean"}]}}]}'),

-- 枚举类型指标规则 - 覆盖所有6个枚举指标 (=, !=, belongs_to, not_belongs_to, is_null, is_not_null)
(44, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "user_level", "operator": "=", "value": "VIP3", "type": "enum"}]}}]}'),
(45, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "channel_source", "operator": "!=", "value": "spam", "type": "enum"}]}}]}'),
(46, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "register_method", "operator": "belongs_to", "value": ["email", "phone", "google"], "type": "enum"}]}}]}'),
(47, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "contract_trading_style", "operator": "not_belongs_to", "value": ["high_risk", "speculative"], "type": "enum"}]}}]}'),
(48, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "register_country", "operator": "is_null", "value": "", "type": "enum"}]}}]}'),
(49, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "kyc_country", "operator": "is_not_null", "value": "", "type": "enum"}]}}]}'),

-- 列表类型指标规则 - 覆盖所有6个列表指标 (contains_any, contains_all, not_contains, intersects, no_intersection, is_null, is_not_null)
(50, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["BTC", "ETH"], "type": "list"}]}}]}'),
(51, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "traded_coins_list", "operator": "contains_all", "value": ["USDT", "BTC"], "type": "list"}]}}]}'),
(52, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "device_fingerprint_list", "operator": "not_contains", "value": ["malicious_device"], "type": "list"}]}}]}'),
(53, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "participated_activity_ids", "operator": "intersects", "value": ["ACT001", "ACT002"], "type": "list"}]}}]}'),
(54, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "reward_claim_history", "operator": "no_intersection", "value": ["spam_reward", "fake_reward"], "type": "list"}]}}]}'),
(55, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "used_coupon_types", "operator": "is_null", "value": "", "type": "list"}]}}]}'),
(56, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "is_not_null", "value": "", "type": "list"}]}}]}'),

-- 复杂混合逻辑示例 - condition间的AND/OR/NOT组合
-- 高价值认证用户：(高资产 OR 高交易量) AND 已认证 AND 非封禁
(57, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "100000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trading_volume", "operator": ">=", "value": "500000", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_kyc_completed", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "!=", "value": "true", "type": "boolean"}]}}]}'),

-- 活跃或新用户：近期活跃 OR 新注册用户
(58, '{"logic": "OR", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_last_login", "operator": "<=", "value": "7", "type": "number"}, {"table": "dws_user.dws_user_activity_df", "field": "login_count_7d", "operator": ">=", "value": "3", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_register", "operator": "<=", "value": "30", "type": "number"}]}}]}'),

-- 非问题用户：NOT(高风险 OR 封禁)
(59, '{"logic": "NOT", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_high_risk_ip", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- fields间的混合逻辑示例 - AND/OR/NOT在同一condition内
-- 主流邮箱用户：Gmail OR Yahoo邮箱
(60, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "email_suffix", "operator": "contains", "value": "gmail", "type": "string"}, {"table": "dws_user.dws_user_activity_df", "field": "email_suffix", "operator": "contains", "value": "yahoo", "type": "string"}]}}]}'),

-- 中高级VIP用户：VIP2 OR VIP3 OR VIP4 OR VIP5
(61, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "user_level", "operator": "=", "value": "VIP2", "type": "enum"}, {"table": "dws_user.dws_user_profile_df", "field": "user_level", "operator": "=", "value": "VIP3", "type": "enum"}, {"table": "dws_user.dws_user_profile_df", "field": "user_level", "operator": "=", "value": "VIP4", "type": "enum"}, {"table": "dws_user.dws_user_profile_df", "field": "user_level", "operator": "=", "value": "VIP5", "type": "enum"}]}}]}'),

-- 中国用户且有资产：+86手机号 AND 有总资产
(62, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "country_region_code", "operator": "=", "value": "+86", "type": "string"}, {"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">", "value": "0", "type": "number"}]}}]}'),

-- 非低价值用户：NOT(低资产 AND 无交易)
(63, '{"logic": "AND", "conditions": [{"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": "<=", "value": "1000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trading_volume", "operator": "=", "value": "0", "type": "number"}]}}]}'),

-- 多层嵌套复杂逻辑示例
-- 优质活跃用户：(高资产 OR 高交易) AND (近期登录 OR 频繁登录) AND NOT(高风险 OR 封禁)
(64, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "50000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "spot_recent_30d_volume", "operator": ">=", "value": "100000", "type": "number"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_last_login", "operator": "<=", "value": "3", "type": "number"}, {"table": "dws_user.dws_user_activity_df", "field": "login_count_7d", "operator": ">=", "value": "5", "type": "number"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_high_risk_ip", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- 完整信息的投资者：(KYC已认证 AND 2FA已开启) AND (有交易经验 OR 有资产) AND NOT(代理用户)
(65, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_kyc_completed", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_profile_df", "field": "is_2fa_enabled", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "spot_trading_volume", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">", "value": "0", "type": "number"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_agent", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- 多币种持有者：持有BTC AND (持有ETH OR USDT) AND NOT持有风险币种
(66, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["BTC"], "type": "list"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["ETH"], "type": "list"}, {"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["USDT"], "type": "list"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "intersects", "value": ["RISK_COIN", "SCAM_COIN"], "type": "list"}]}}]}'),

-- 活动参与但理性用户：参与活动 AND (有邀请成功 OR 有返佣) AND NOT(过度营销用户)
(67, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "participated_activity_ids", "operator": "is_not_null", "value": "", "type": "list"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "successful_invites_count", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_marketing_df", "field": "total_commission_amount", "operator": ">", "value": "0", "type": "number"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_marketing_df", "field": "red_packet_count", "operator": ">", "value": "50", "type": "number"}]}}]}'),

-- 多平台跨设备用户：使用多种设备 AND (iOS OR Android) AND NOT(只用Web)
(68, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "device_fingerprint_list", "operator": "contains_all", "value": ["Chrome_Win10", "Safari_macOS"], "type": "list"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "operating_system", "operator": "=", "value": "iOS", "type": "string"}, {"table": "dws_user.dws_user_activity_df", "field": "operating_system", "operator": "=", "value": "Android", "type": "string"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "device_fingerprint_list", "operator": "contains_all", "value": ["Web_Only"], "type": "list"}]}}]}'),

-- 理财偏好但谨慎用户：开通理财 AND (有挂单习惯 OR 非高杠杆) AND NOT(投机风格)
(69, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "has_finance_management", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "has_pending_orders", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trading_style", "operator": "!=", "value": "高杠杆", "type": "enum"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "contract_trading_style", "operator": "belongs_to", "value": ["投机", "高风险"], "type": "enum"}]}}]}'),

-- 国际化高净值用户：非中国地区 AND 高资产 AND (多币种 OR 合约交易) AND NOT(新手用户)
(70, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "country_region_code", "operator": "!=", "value": "+86", "type": "string"}, {"table": "dws_user.dws_user_profile_df", "field": "register_country", "operator": "not_belongs_to", "value": ["CN"], "type": "enum"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "200000", "type": "number"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["BTC", "ETH", "USDT"], "type": "list"}, {"table": "dws_user.dws_user_trading_df", "field": "has_contract_trading", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_register", "operator": "<=", "value": "30", "type": "number"}]}}]}'),

-- === 新增DWS颗粒化字段标签规则 (71-90) ===
-- 基于新的DWS表结构颗粒化字段的标签规则

-- 现货资产优势用户：现货持仓 > 合约持仓
(71, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": ">", "value": "contract_position_value", "type": "number"}]}}]}'),

-- 合约交易专家：合约交易量 >= 现货交易量 AND 有合约交易
(72, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "contract_trading_volume", "operator": ">=", "value": "50000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "has_contract_trading", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- 理财爱好者：理财持仓 > 10000 AND 理财交易次数 > 5
(73, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "finance_position_value", "operator": ">", "value": "10000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "finance_trade_count", "operator": ">", "value": "5", "type": "number"}]}}]}'),

-- 链上活跃用户：链上持仓 > 0 AND 链上交易次数 > 3
(74, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "onchain_position_value", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "onchain_trade_count", "operator": ">", "value": "3", "type": "number"}]}}]}'),

-- 现货流动性提供者：现货可用余额 > 50000 AND 现货交易次数 > 20
(75, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_available_balance", "operator": ">", "value": "50000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "spot_trade_count", "operator": ">", "value": "20", "type": "number"}]}}]}'),

-- 合约资金管理者：合约可用余额 > 20000 AND 合约锁仓 < 5000 (风险控制)
(76, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "contract_available_balance", "operator": ">", "value": "20000", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "contract_locked_amount", "operator": "<", "value": "5000", "type": "number"}]}}]}'),

-- 理财保守型用户：理财锁仓 > 理财可用余额 (长期持有)
(77, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "finance_locked_amount", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "finance_locked_amount", "operator": ">", "value": "finance_available_balance", "type": "number"}]}}]}'),

-- 多元化投资者：现货+合约+理财 都有持仓
(78, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "contract_position_value", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "finance_position_value", "operator": ">", "value": "0", "type": "number"}]}}]}'),

-- 现货专一用户：只有现货持仓，其他持仓为0
(79, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "contract_position_value", "operator": "=", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "finance_position_value", "operator": "=", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "onchain_position_value", "operator": "=", "value": "0", "type": "number"}]}}]}'),

-- 高频现货交易者：现货近30日交易额 > 现货总交易额的50%
(80, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "spot_recent_30d_volume", "operator": ">", "value": "10000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "spot_trade_count", "operator": ">", "value": "15", "type": "number"}]}}]}'),

-- 合约近期活跃用户：合约近30日交易额 > 20000 AND 合约交易次数 > 10
(81, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "contract_recent_30d_volume", "operator": ">", "value": "20000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trade_count", "operator": ">", "value": "10", "type": "number"}]}}]}'),

-- 全业务活跃用户：现货+合约+理财+链上 都有交易
(82, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "spot_trade_count", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trade_count", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "finance_trade_count", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "onchain_trade_count", "operator": ">", "value": "0", "type": "number"}]}}]}'),

-- 资产均衡用户：现货、合约、理财持仓都在相似水平(差异 < 50%)
(83, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": "in_range", "value": ["5000", "50000"], "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "contract_position_value", "operator": "in_range", "value": ["5000", "50000"], "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "finance_position_value", "operator": "in_range", "value": ["5000", "50000"], "type": "number"}]}}]}'),

-- 链上投资专家：链上持仓占总持仓比例 > 30%
(84, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "onchain_position_value", "operator": ">", "value": "10000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "onchain_trade_count", "operator": ">", "value": "5", "type": "number"}]}}]}'),

-- 现金流管理专家：总可用余额 > 总锁仓金额
(85, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "available_balance", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "available_balance", "operator": ">", "value": "locked_amount", "type": "number"}]}}]}'),

-- 理财产品重度用户：理财交易次数 > 现货+合约交易次数
(86, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "finance_trade_count", "operator": ">", "value": "10", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "has_finance_management", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- 跨链DeFi用户：链上交易 > 0 AND 持有主流币种
(87, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "onchain_trade_count", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_behavior_df", "field": "current_holding_coins", "operator": "contains_any", "value": ["BTC", "ETH"], "type": "list"}]}}]}'),

-- 资产安全意识用户：各类锁仓总额 > 可用余额总额 (风险控制意识强)
(88, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "locked_amount", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "locked_amount", "operator": ">", "value": "available_balance", "type": "number"}]}}]}'),

-- 新兴业务探索者：链上交易占总交易的比例较高
(89, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "onchain_trade_count", "operator": ">", "value": "3", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "onchain_position_value", "operator": ">", "value": "5000", "type": "number"}]}}]}'),

-- 综合高价值用户：各类持仓都 > 20000 AND 各类交易都活跃
(90, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "spot_position_value", "operator": ">", "value": "20000", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "contract_position_value", "operator": ">", "value": "20000", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "finance_position_value", "operator": ">", "value": "20000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "spot_trade_count", "operator": ">", "value": "5", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trade_count", "operator": ">", "value": "5", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "finance_trade_count", "operator": ">", "value": "3", "type": "number"}]}}]}')

;
