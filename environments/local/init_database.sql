-- 大数据标签系统 - 本地环境数据库初始化脚本
-- 使用方法: mysql -h 127.0.0.1 -P 3307 -u root -proot123 < init_database.sql

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS tag_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tag_system;

-- 1. 标签分类表
--CREATE TABLE IF NOT EXISTS tag_category (
--    category_id INT PRIMARY KEY AUTO_INCREMENT,
--    category_name VARCHAR(100) NOT NULL COMMENT '分类名称',
--    description TEXT COMMENT '分类描述',
--    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
--    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--    INDEX idx_active (is_active)
--) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签分类表';

-- 更改为
CREATE TABLE IF NOT EXISTS `tag_category` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `category_name` varchar(200) NOT NULL COMMENT '分类名称',
  `is_delete` tinyint(1) NOT NULL COMMENT '是否删除',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT='标签分类表';


-- 2. 标签定义表
--CREATE TABLE IF NOT EXISTS tag_definition (
--    tag_id INT PRIMARY KEY AUTO_INCREMENT,
--    tag_name VARCHAR(200) NOT NULL COMMENT '标签名称',
--    tag_category VARCHAR(100) NOT NULL COMMENT '标签分类',
--    description TEXT COMMENT '标签描述',
--    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
--    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--    INDEX idx_category (tag_category),
--    INDEX idx_active (is_active)
--) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签定义表';

-- 标签定义更改为：
CREATE TABLE `tag_definition` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `tag_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '标签名称',
  `tag_identify` varchar(200) CHARACTER SET utf8mb4  NOT NULL COMMENT '标签唯一标识',
  `category_id` varchar(100) CHARACTER SET utf8mb4  NOT NULL COMMENT '标签大类',
  `tag_type` tinyint(1) NOT NULL COMMENT '标签类型，1-自动标签，2-人工标签',
  `is_active` tinyint NOT NULL DEFAULT '1' COMMENT '是否激活, 1-激活，0-未激活',
  `trigger_type` tinyint(1) NOT NULL COMMENT '触发方式（仅自动标签），1-实时，2-定时',
  `trigger_cycle` tinyint DEFAULT NULL COMMENT '触发周期配置(1-小时2-天3-周)',
  `description` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '标签描述',
  `rules_description` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '命中规则说明',
  `created_user` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '创建人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id` DESC) USING BTREE,
  UNIQUE KEY `uniq_tag_identify` (`tag_identify`),
  KEY `idx_category` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='标签定义表';

-- 3. 标签规则表
--CREATE TABLE IF NOT EXISTS tag_rules (
--    rule_id INT PRIMARY KEY AUTO_INCREMENT,
--    tag_id INT NOT NULL COMMENT '标签ID',
--    rule_conditions JSON NOT NULL COMMENT '规则条件（JSON格式）',
--    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
--    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--    FOREIGN KEY (tag_id) REFERENCES tag_definition(tag_id) ON DELETE CASCADE,
--    INDEX idx_tag_id (tag_id),
--    INDEX idx_active (is_active)
--) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签规则表';

--标签规则更改为
CREATE TABLE `tag_rules_config` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `tag_id` int NOT NULL COMMENT '标签ID',
  `tag_conditions` json NOT NULL COMMENT '规则条件',
  `is_active` tinyint(1) DEFAULT '1' COMMENT '是否激活',
  `created_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_tag_id` (`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='标签规则';

-- 4. 用户标签结果表（修正版：一个用户一条记录，包含所有标签ID数组）
-- created_time: 第一次插入时设置，永远不变
-- updated_time: 只有通过UPSERT逻辑显式更新时才变化（不使用ON UPDATE CURRENT_TIMESTAMP）
--CREATE TABLE user_tags (
--    id BIGINT PRIMARY KEY AUTO_INCREMENT,
--    user_id VARCHAR(100) NOT NULL COMMENT '用户ID',
--    tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组',
--    tag_details JSON COMMENT '标签详细信息（key-value形式）',
--    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间（永远不变）',
--    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间（由UPSERT逻辑控制）',
--    INDEX idx_user_id (user_id),
--    INDEX idx_created_time (created_time),
--    INDEX idx_updated_time (updated_time),
--    UNIQUE KEY uk_user_id (user_id)
--) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '用户标签结果表（一个用户一条记录，包含标签ID数组）';

--标签结果表更新为：
CREATE TABLE `user_tag_relation` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `user_id` bigint  NOT NULL COMMENT '用户ID',
  `tag_id_list` json NOT NULL COMMENT '用户的所有标签ID数组',
  `created_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户标签记录';



-- 插入初始测试数据
-- 标签分类（新表结构）
INSERT IGNORE INTO tag_category (id, category_name, is_delete) VALUES 
(1, '用户价值', 0),
(2, '行为特征', 0),
(3, '风险等级', 0),
(4, '生命周期', 0),
(5, '偏好特征', 0),
(6, '人口特征', 0),
(7, '资产结构', 0),
(8, '产品偏好', 0),
(9, '活跃度', 0),
(10, '综合特征', 0);

-- 标签定义（新表结构，50条标签）
INSERT IGNORE INTO tag_definition (id, tag_name, tag_identify, category_id, tag_type, is_active, trigger_type, trigger_cycle, description, rules_description, created_user) VALUES 
-- 数值类型操作符测试标签 (1-10)
(1, '资产等值10万用户', 'ASSET_EQUAL_100K', '1', 1, 1, 2, 2, '总资产价值等于10万的用户', '总资产价值 = 100000', 'system'),
(2, '有资产用户', 'HAS_ASSET', '1', 1, 1, 2, 2, '总资产价值不为0的用户', '总资产价值 != 0', 'system'),
(3, '活跃交易用户', 'ACTIVE_TRADER', '2', 1, 1, 2, 2, '30天交易次数超过10次的用户', '30天交易次数 > 10', 'system'),
(4, '低频交易用户', 'LOW_FREQ_TRADER', '2', 1, 1, 2, 2, '30天交易次数少于5次的用户', '30天交易次数 < 5', 'system'),
(5, '高现金用户', 'HIGH_CASH', '7', 1, 1, 2, 2, '现金余额超过5万的用户', '现金余额 >= 50000', 'system'),
(6, '低风险用户', 'LOW_RISK', '3', 1, 1, 2, 2, '风险评分小于等于30的用户', '风险评分 <= 30', 'system'),
(7, '成年用户', 'ADULT', '6', 1, 1, 2, 2, '年龄在18-65岁之间的用户', '年龄 18-65岁', 'system'),
(8, '非未成年用户', 'NOT_MINOR', '6', 1, 1, 2, 2, '年龄不在0-17岁范围的用户', '年龄 不在 0-17岁', 'system'),
(9, '无债务用户', 'DEBT_FREE', '7', 1, 1, 2, 2, '债务金额为空的用户', '债务金额 为空', 'system'),
(10, '有总资产用户', 'HAS_TOTAL_ASSET', '1', 1, 1, 2, 2, '总资产价值不为空的用户', '总资产价值 不为空', 'system'),

-- 字符串类型操作符测试标签 (11-18)
(11, 'VIP3用户', 'VIP3_USER', '1', 1, 1, 2, 2, '用户等级为VIP3的用户', '用户等级 = VIP3', 'system'),
(12, '非VIP1用户', 'NOT_VIP1', '1', 1, 1, 2, 2, '用户等级不是VIP1的用户', '用户等级 != VIP1', 'system'),
(13, '138号段用户', 'PHONE_138', '6', 1, 1, 2, 2, '手机号包含138的用户', '手机号 包含 138', 'system'),
(14, '非临时邮箱用户', 'NOT_TEMP_EMAIL', '6', 1, 1, 2, 2, '邮箱不包含temp的用户', '邮箱 不包含 temp', 'system'),
(15, '中国手机用户', 'CHINA_PHONE', '6', 1, 1, 2, 2, '手机号以+86开头的用户', '手机号 以+86开头', 'system'),
(16, 'Gmail邮箱用户', 'GMAIL_USER', '6', 1, 1, 2, 2, '邮箱以gmail.com结尾的用户', '邮箱 以gmail.com结尾', 'system'),
(17, '无中间名用户', 'NO_MIDDLE_NAME', '6', 1, 1, 2, 2, '中间名为空的用户', '中间名 为空', 'system'),
(18, '有名字用户', 'HAS_FIRST_NAME', '6', 1, 1, 2, 2, '名字不为空的用户', '名字 不为空', 'system'),

-- 日期类型操作符测试标签 (19-26)
(19, '元旦注册用户', 'NEW_YEAR_REG', '4', 1, 1, 2, 2, '2025年1月1日注册的用户', '注册日期 = 2025-01-01', 'system'),
(20, '非元旦登录用户', 'NOT_NEW_YEAR_LOGIN', '9', 1, 1, 2, 2, '最后登录不是2025年1月1日的用户', '最后登录 != 2025-01-01', 'system'),
(21, '近期登录用户', 'RECENT_LOGIN', '9', 1, 1, 2, 2, '最后登录晚于2025年1月1日的用户', '最后登录 > 2025-01-01', 'system'),
(22, '2024年前注册用户', 'PRE_2024_REG', '4', 1, 1, 2, 2, '2024年12月31日前注册的用户', '注册日期 < 2024-12-31', 'system'),
(23, '2024年注册用户', 'Y2024_REG', '4', 1, 1, 2, 2, '2024年内注册的用户', '注册日期 在2024年', 'system'),
(24, '非2023年登录用户', 'NOT_2023_LOGIN', '9', 1, 1, 2, 2, '最后登录不在2023年的用户', '最后登录 不在2023年', 'system'),
(25, '未交易用户', 'NO_TRADE', '2', 1, 1, 2, 2, '最后交易日期为空的用户', '最后交易日期 为空', 'system'),
(26, '有生日用户', 'HAS_BIRTHDAY', '6', 1, 1, 2, 2, '生日不为空的用户', '生日 不为空', 'system'),

-- 布尔类型操作符测试标签 (27-28)
(27, 'VIP认证用户', 'VIP_VERIFIED', '1', 1, 1, 2, 2, 'VIP标识为true的用户', 'VIP标识 = true', 'system'),
(28, '非封禁用户', 'NOT_BANNED', '4', 1, 1, 2, 2, '封禁标识为false的用户', '封禁标识 = false', 'system'),

-- 枚举类型操作符测试标签 (29-34)
(29, 'KYC已验证用户', 'KYC_VERIFIED', '4', 1, 1, 2, 2, 'KYC状态为已验证的用户', 'KYC状态 = verified', 'system'),
(30, '非暂停账户用户', 'NOT_SUSPENDED', '4', 1, 1, 2, 2, '账户状态不是暂停的用户', '账户状态 != suspended', 'system'),
(31, 'VIP等级用户', 'VIP_LEVEL', '1', 1, 1, 2, 2, '用户等级属于VIP1/VIP2/VIP3的用户', '用户等级 属于 VIP1/VIP2/VIP3', 'system'),
(32, '正常状态用户', 'NORMAL_STATUS', '4', 1, 1, 2, 2, '账户状态不是暂停或封禁的用户', '账户状态 正常', 'system'),
(33, '无次要状态用户', 'NO_SECONDARY_STATUS', '4', 1, 1, 2, 2, '次要状态为空的用户', '次要状态 为空', 'system'),
(34, '有主要状态用户', 'HAS_PRIMARY_STATUS', '4', 1, 1, 2, 2, '主要状态不为空的用户', '主要状态 不为空', 'system'),

-- 列表类型操作符测试标签 (35-41)
(35, '股债偏好用户', 'STOCK_BOND_PREF', '8', 1, 1, 2, 2, '感兴趣产品包含股票或债券的用户', '感兴趣产品 包含 股票或债券', 'system'),
(36, '储蓄全产品用户', 'SAVING_ALL_PROD', '8', 1, 1, 2, 2, '拥有储蓄和支票全部产品的用户', '拥有 储蓄+支票 全部产品', 'system'),
(37, '非外汇用户', 'NOT_FOREX', '8', 1, 1, 2, 2, '黑名单产品不包含外汇的用户', '黑名单产品 不包含 外汇', 'system'),
(38, '高端产品用户', 'PREMIUM_PROD', '8', 1, 1, 2, 2, '活跃产品与高端/黄金有交集的用户', '活跃产品 包含 高端/黄金', 'system'),
(39, '非白金用户', 'NOT_PLATINUM', '8', 1, 1, 2, 2, '过期产品与高端/白金无交集的用户', '过期产品 不包含 高端/白金', 'system'),
(40, '无可选服务用户', 'NO_OPTIONAL_SVC', '5', 1, 1, 2, 2, '可选服务为空的用户', '可选服务 为空', 'system'),
(41, '有必需服务用户', 'HAS_REQUIRED_SVC', '5', 1, 1, 2, 2, '必需服务不为空的用户', '必需服务 不为空', 'system'),

-- NOT逻辑测试标签 (42-43)
(42, '非VIP1用户NOT', 'NOT_VIP1_LOGIC', '1', 1, 1, 2, 2, '不是VIP1用户（NOT逻辑）', 'NOT(用户等级=VIP1)', 'system'),
(43, '非低价值用户', 'NOT_LOW_VALUE', '1', 1, 1, 2, 2, '不是低资产且零交易的用户', 'NOT(低资产 AND 零交易)', 'system'),

-- 复杂多条件组合标签 (44-50)
(44, '高价值认证用户', 'HIGH_VALUE_VERIFIED', '10', 1, 1, 2, 2, '高等级或高资产且已认证非封禁用户', '(高等级 OR 高资产) AND 已认证 AND 非封禁', 'system'),
(45, '活跃或富有用户', 'ACTIVE_OR_RICH', '10', 1, 1, 2, 2, '近期活跃交易用户或高现金用户', '近期活跃交易 OR 高现金', 'system'),
(46, '正常活跃用户', 'NORMAL_ACTIVE', '10', 1, 1, 2, 2, '非暂停封禁且近期登录的用户', 'NOT(暂停或封禁) AND 近期登录', 'system'),
(47, '主流邮箱中国用户', 'MAINSTREAM_EMAIL_CN', '10', 1, 1, 2, 2, 'Gmail/Yahoo邮箱且中国手机号用户', '(Gmail OR Yahoo邮箱) AND 中国手机号', 'system'),
(48, '中年高价值用户', 'MIDDLE_AGE_HIGH_VALUE', '10', 1, 1, 2, 2, '25-45岁非低资产的高等级或高端产品用户', '25-45岁 AND 非低资产 AND (高等级 OR 高端产品)', 'system'),
(49, '非高风险新手用户', 'NOT_HIGH_RISK_NEWBIE', '10', 1, 1, 2, 2, '不同时持有高风险产品且非VIP的用户', 'NOT(高风险产品 AND 非VIP)', 'system'),
(50, '完整信息用户', 'COMPLETE_INFO', '10', 1, 1, 2, 2, '姓名生日完整且可选服务为空的用户', '姓名完整 AND 生日完整 AND 可选服务为空', 'system');


-- 完整的标签规则插入示例，包含各种操作符和逻辑类型
INSERT INTO tag_rules_config (tag_id, tag_conditions) VALUES
-- 基础数值类型操作符示例
(1, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": "=", "value": "100000", "type": "number"}]}}]}'),
(2, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": "!=", "value": "0", "type": "number"}]}}]}'),
(3, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "10", "type": "number"}]}}]}'),
(4, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "trade_count_30d", "operator": "<", "value": "5", "type": "number"}]}}]}'),
(5, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "cash_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),
(6, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_risk_profile", "field": "risk_score", "operator": "<=", "value": "30", "type": "number"}]}}]}'),
(7, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "age", "operator": "in_range", "value": ["18", "65"], "type": "number"}]}}]}'),
(8, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "age", "operator": "not_in_range", "value": ["0", "17"], "type": "number"}]}}]}'),
(9, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "debt_amount", "operator": "is_null", "value": "", "type": "number"}]}}]}'),
(10, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": "is_not_null", "value": "", "type": "number"}]}}]}'),

-- 字符串类型操作符示例
(11, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "=", "value": "VIP3", "type": "string"}]}}]}'),
(12, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "!=", "value": "VIP1", "type": "string"}]}}]}'),
(13, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "phone_number", "operator": "contains", "value": "138", "type": "string"}]}}]}'),
(14, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "email", "operator": "not_contains", "value": "temp", "type": "string"}]}}]}'),
(15, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "phone_number", "operator": "starts_with", "value": "+86", "type": "string"}]}}]}'),
(16, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "email", "operator": "ends_with", "value": "gmail.com", "type": "string"}]}}]}'),
(17, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "middle_name", "operator": "is_null", "value": "", "type": "string"}]}}]}'),
(18, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "first_name", "operator": "is_not_null", "value": "", "type": "string"}]}}]}'),

-- 日期类型操作符示例
(19, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "registration_date", "operator": "=", "value": "2025-01-01", "type": "date"}]}}]}'),
(20, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "last_login_date", "operator": "!=", "value": "2025-01-01", "type": "date"}]}}]}'),
(21, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "last_login_date", "operator": ">", "value": "2025-01-01", "type": "date"}]}}]}'),
(22, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "registration_date", "operator": "<", "value": "2024-12-31", "type": "date"}]}}]}'),
(23, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "registration_date", "operator": "date_in_range", "value": ["2024-01-01", "2024-12-31"], "type": "date"}]}}]}'),
(24, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "last_login_date", "operator": "date_not_in_range", "value": ["2023-01-01", "2023-12-31"], "type": "date"}]}}]}'),
(25, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_activity_summary", "field": "last_trade_date", "operator": "is_null", "value": "", "type": "date"}]}}]}'),
(26, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "birthday", "operator": "is_not_null", "value": "", "type": "date"}]}}]}'),

-- 布尔类型操作符示例
(27, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "is_vip", "operator": "is_true", "value": "true", "type": "boolean"}]}}]}'),
(28, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "is_banned", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

-- 枚举类型操作符示例
(29, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "enum"}]}}]}'),
(30, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "account_status", "operator": "!=", "value": "suspended", "type": "enum"}]}}]}'),
(31, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP1", "VIP2", "VIP3"], "type": "enum"}]}}]}'),
(32, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "account_status", "operator": "not_belongs_to", "value": ["suspended", "banned"], "type": "enum"}]}}]}'),
(33, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "secondary_status", "operator": "is_null", "value": "", "type": "enum"}]}}]}'),
(34, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "primary_status", "operator": "is_not_null", "value": "", "type": "enum"}]}}]}'),

-- 列表类型操作符示例
(35, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "interested_products", "operator": "contains_any", "value": ["stocks", "bonds"], "type": "list"}]}}]}'),
(36, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "owned_products", "operator": "contains_all", "value": ["savings", "checking"], "type": "list"}]}}]}'),
(37, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "blacklisted_products", "operator": "not_contains", "value": ["forex"], "type": "list"}]}}]}'),
(38, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "active_products", "operator": "intersects", "value": ["premium", "gold"], "type": "list"}]}}]}'),
(39, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "expired_products", "operator": "no_intersection", "value": ["premium", "platinum"], "type": "list"}]}}]}'),
(40, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "optional_services", "operator": "is_null", "value": "", "type": "list"}]}}]}'),
(41, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_preferences", "field": "required_services", "operator": "is_not_null", "value": "", "type": "list"}]}}]}'),

-- NOT逻辑示例
(42, '{"logic": "NOT", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "=", "value": "VIP1", "type": "string"}]}}]}'),
(43, '{"logic": "NOT", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": "<", "value": "1000", "type": "number"}, {"table": "tag_system.user_activity_summary", "field": "trade_count_30d", "operator": "=", "value": "0", "type": "number"}]}}]}'),

-- 复杂的多条件组合示例
(44, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP2", "VIP3"], "type": "enum"}, {"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "100000", "type": "number"}]}}, {"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "enum"}, {"table": "tag_system.user_basic_info", "field": "is_banned", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

(45, '{"logic": "OR", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_activity_summary", "field": "last_login_date", "operator": "date_in_range", "value": ["2025-01-01", "2025-07-26"], "type": "date"}, {"table": "tag_system.user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "5", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_system.user_asset_summary", "field": "cash_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),

(46, '{"logic": "NOT", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_system.user_basic_info", "field": "account_status", "operator": "belongs_to", "value": ["suspended", "banned"], "type": "enum"}, {"table": "tag_system.user_activity_summary", "field": "last_login_date", "operator": "date_not_in_range", "value": ["2024-01-01", "2025-07-26"], "type": "date"}]}}]}'),

-- 字符串模糊匹配组合
(47, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_system.user_basic_info", "field": "email", "operator": "ends_with", "value": "gmail.com", "type": "string"}, {"table": "tag_system.user_basic_info", "field": "email", "operator": "ends_with", "value": "yahoo.com", "type": "string"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_system.user_basic_info", "field": "phone_number", "operator": "starts_with", "value": "+86", "type": "string"}]}}]}'),

-- 数值范围和枚举组合
(48, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_basic_info", "field": "age", "operator": "in_range", "value": ["25", "45"], "type": "number"}, {"table": "tag_system.user_asset_summary", "field": "total_asset_value", "operator": "not_in_range", "value": ["0", "1000"], "type": "number"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "tag_system.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP3", "VIP4", "VIP5"], "type": "enum"}, {"table": "tag_system.user_preferences", "field": "owned_products", "operator": "contains_any", "value": ["premium", "platinum"], "type": "list"}]}}]}'),

-- 列表操作和布尔组合
(49, '{"logic": "NOT", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_preferences", "field": "interested_products", "operator": "contains_all", "value": ["high_risk", "speculative"], "type": "list"}, {"table": "tag_system.user_basic_info", "field": "is_vip", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

-- 空值检查组合
(50, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_system.user_basic_info", "field": "first_name", "operator": "is_not_null", "value": "", "type": "string"}, {"table": "tag_system.user_basic_info", "field": "last_name", "operator": "is_not_null", "value": "", "type": "string"}, {"table": "tag_system.user_basic_info", "field": "birthday", "operator": "is_not_null", "value": "", "type": "date"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "tag_system.user_basic_info", "field": "middle_name", "operator": "is_null", "value": "", "type": "string"}, {"table": "tag_system.user_preferences", "field": "optional_services", "operator": "is_null", "value": "", "type": "list"}]}}]}');



-- 显示初始化结果
SELECT '数据库初始化完成！' as status;
SELECT '标签分类数量:' as info, COUNT(*) as count FROM tag_category WHERE is_delete = 0;
SELECT '标签定义数量:' as info, COUNT(*) as count FROM tag_definition WHERE is_active = 1;  
SELECT '标签规则数量:' as info, COUNT(*) as count FROM tag_rules_config WHERE is_active = 1;