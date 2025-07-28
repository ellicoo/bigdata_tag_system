-- 大数据标签系统 - 本地环境数据库初始化脚本
-- 使用方法: mysql -h 127.0.0.1 -P 3307 -u root -proot123 < init_database.sql

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS tag_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tag_system;

-- 1. 标签分类表
CREATE TABLE IF NOT EXISTS tag_category (
    category_id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(100) NOT NULL COMMENT '分类名称',
    description TEXT COMMENT '分类描述',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_active (is_active)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签分类表';

-- 2. 标签定义表
CREATE TABLE IF NOT EXISTS tag_definition (
    tag_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(200) NOT NULL COMMENT '标签名称',
    tag_category VARCHAR(100) NOT NULL COMMENT '标签分类',
    description TEXT COMMENT '标签描述',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_category (tag_category),
    INDEX idx_active (is_active)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签定义表';

-- 3. 标签规则表
CREATE TABLE IF NOT EXISTS tag_rules (
    rule_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_id INT NOT NULL COMMENT '标签ID',
    rule_conditions JSON NOT NULL COMMENT '规则条件（JSON格式）',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (tag_id) REFERENCES tag_definition(tag_id) ON DELETE CASCADE,
    INDEX idx_tag_id (tag_id),
    INDEX idx_active (is_active)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '标签规则表';

-- 4. 用户标签结果表（修正版：一个用户一条记录，包含所有标签ID数组）
-- created_time: 第一次插入时设置，永远不变
-- updated_time: 只有通过UPSERT逻辑显式更新时才变化（不使用ON UPDATE CURRENT_TIMESTAMP）
DROP TABLE IF EXISTS user_tags;
CREATE TABLE user_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL COMMENT '用户ID',
    tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组',
    tag_details JSON COMMENT '标签详细信息（key-value形式）',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间（永远不变）',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间（由UPSERT逻辑控制）',
    INDEX idx_user_id (user_id),
    INDEX idx_created_time (created_time),
    INDEX idx_updated_time (updated_time),
    UNIQUE KEY uk_user_id (user_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '用户标签结果表（一个用户一条记录，包含标签ID数组）';

-- 插入初始测试数据
-- 标签分类
INSERT IGNORE INTO tag_category (category_id, category_name, description) VALUES 
(1, '用户价值', '基于用户资产和行为的价值分类'),
(2, '行为特征', '基于用户行为模式的特征标签'),
(3, '风险等级', '基于用户风险评估的等级标签'),
(4, '生命周期', '基于用户注册和活跃状态的生命周期标签'),
(5, '偏好特征', '基于用户交易偏好的特征标签');

-- 标签定义（确保中文字符正确）
INSERT IGNORE INTO tag_definition (tag_id, tag_name, tag_category, description) VALUES 
(1, '高净值用户', '用户价值', '总资产价值超过15万的用户'),
(2, '活跃交易者', '行为特征', '近30天交易次数超过10次的用户'),
(3, '低风险用户', '风险等级', '风险评估为低风险的用户'),
(4, '新注册用户', '生命周期', '注册时间在30天内的新用户'),
(5, 'VIP客户', '用户价值', 'VIP等级为VIP2或VIP3且KYC已验证的用户'),
(6, '现金充足用户', '资产结构', '现金余额超过5万的用户'),
(7, '年轻用户', '人口特征', '年龄在18-30岁之间的用户'),
(8, '最近活跃用户', '行为特征', '最近7天内有登录的用户');

-- 插入新结构的标签规则
-- 插入新结构的标签规则
--INSERT INTO tag_rules (tag_id, rule_conditions) VALUES

---- 标签1: 高资产用户（单表单条件）
--(1, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "100000", "type": "number"}]}}]}'),
--
---- 标签2: 活跃交易用户（单表单条件）
--(2, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "10", "type": "number"}]}}]}'),
--
---- 标签3: 低风险用户（单表单条件）
--(3, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_risk_profile", "field": "risk_score", "operator": "<=", "value": "30", "type": "number"}]}}]}'),
--
---- 标签4: 新注册用户（单表单条件，时间相关）
--(4, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "registration_date", "operator": "recent_days", "value": "30", "type": "date"}]}}]}'),
--
---- 标签5: VIP认证用户（单表多条件AND）
--(5, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "in", "value": ["VIP2", "VIP3"], "type": "string"}, {"table": "tag_test.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "string"}]}}]}'),
--
---- 标签6: 高现金余额用户（单表单条件）
--(6, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "cash_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),
--
---- 标签7: 年轻用户（单表单条件，范围查询）
--(7, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "age", "operator": "in_range", "value": ["18", "30"], "type": "number"}]}}]}'),
--
---- 标签8: 近期活跃用户（单表单条件，时间相关）
--(8, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "recent_days", "value": "7", "type": "date"}]}}]}'),
--
---- 标签9: 高价值客户（多表多条件AND）
--(9, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "500000", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": ">=", "value": "20", "type": "number"}]}}]}'),
--
---- 标签10: 潜在流失用户（多表多条件OR）
--(10, '{"logic": "OR", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "days_ago", "value": "30", "type": "date"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": "<=", "value": "2", "type": "number"}]}}]}'),
--
---- 标签11: 优质新用户（多表复杂条件）
--(11, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "registration_date", "operator": "recent_days", "value": "60", "type": "date"}]}}, {"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "string"}, {"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "10000", "type": "number"}]}}]}'),
--
---- 标签12: 超级VIP用户（复杂多条件组合）
--(12, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "=", "value": "VIP5", "type": "string"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "1000000", "type": "number"}, {"table": "tag_test.user_activity_summary", "field": "trade_volume_30d", "operator": ">=", "value": "500000", "type": "number"}]}}, {"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "recent_days", "value": "7", "type": "date"}, {"table": "tag_test.user_risk_profile", "field": "risk_score", "operator": "<=", "value": "20", "type": "number"}]}}]}');


-- 完整的标签规则插入示例，包含各种操作符和逻辑类型
INSERT INTO tag_rules (tag_id, rule_conditions) VALUES
-- 基础数值类型操作符示例
(1, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": "=", "value": "100000", "type": "number"}]}}]}'),
(2, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": "!=", "value": "0", "type": "number"}]}}]}'),
(3, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "10", "type": "number"}]}}]}'),
(4, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": "<", "value": "5", "type": "number"}]}}]}'),
(5, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "cash_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),
(6, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_risk_profile", "field": "risk_score", "operator": "<=", "value": "30", "type": "number"}]}}]}'),
(7, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "age", "operator": "in_range", "value": ["18", "65"], "type": "number"}]}}]}'),
(8, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "age", "operator": "not_in_range", "value": ["0", "17"], "type": "number"}]}}]}'),
(9, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "debt_amount", "operator": "is_null", "value": "", "type": "number"}]}}]}'),
(10, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": "is_not_null", "value": "", "type": "number"}]}}]}'),

-- 字符串类型操作符示例
(11, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "=", "value": "VIP3", "type": "string"}]}}]}'),
(12, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "!=", "value": "VIP1", "type": "string"}]}}]}'),
(13, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "phone_number", "operator": "contains", "value": "138", "type": "string"}]}}]}'),
(14, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "email", "operator": "not_contains", "value": "temp", "type": "string"}]}}]}'),
(15, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "phone_number", "operator": "starts_with", "value": "+86", "type": "string"}]}}]}'),
(16, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "email", "operator": "ends_with", "value": "gmail.com", "type": "string"}]}}]}'),
(17, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "middle_name", "operator": "is_null", "value": "", "type": "string"}]}}]}'),
(18, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "first_name", "operator": "is_not_null", "value": "", "type": "string"}]}}]}'),

-- 日期类型操作符示例
(19, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "registration_date", "operator": "=", "value": "2025-01-01", "type": "date"}]}}]}'),
(20, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "!=", "value": "2025-01-01", "type": "date"}]}}]}'),
(21, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": ">", "value": "2025-01-01", "type": "date"}]}}]}'),
(22, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "registration_date", "operator": "<", "value": "2024-12-31", "type": "date"}]}}]}'),
(23, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "registration_date", "operator": "date_in_range", "value": ["2024-01-01", "2024-12-31"], "type": "date"}]}}]}'),
(24, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "date_not_in_range", "value": ["2023-01-01", "2023-12-31"], "type": "date"}]}}]}'),
(25, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_trade_date", "operator": "is_null", "value": "", "type": "date"}]}}]}'),
(26, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "birthday", "operator": "is_not_null", "value": "", "type": "date"}]}}]}'),

-- 布尔类型操作符示例
(27, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "is_vip", "operator": "is_true", "value": "true", "type": "boolean"}]}}]}'),
(28, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "is_banned", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

-- 枚举类型操作符示例
(29, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "enum"}]}}]}'),
(30, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "account_status", "operator": "!=", "value": "suspended", "type": "enum"}]}}]}'),
(31, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP1", "VIP2", "VIP3"], "type": "enum"}]}}]}'),
(32, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "account_status", "operator": "not_belongs_to", "value": ["suspended", "banned"], "type": "enum"}]}}]}'),
(33, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "secondary_status", "operator": "is_null", "value": "", "type": "enum"}]}}]}'),
(34, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "primary_status", "operator": "is_not_null", "value": "", "type": "enum"}]}}]}'),

-- 列表类型操作符示例
(35, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "interested_products", "operator": "contains_any", "value": ["stocks", "bonds"], "type": "list"}]}}]}'),
(36, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "owned_products", "operator": "contains_all", "value": ["savings", "checking"], "type": "list"}]}}]}'),
(37, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "blacklisted_products", "operator": "not_contains", "value": ["forex"], "type": "list"}]}}]}'),
(38, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "active_products", "operator": "intersects", "value": ["premium", "gold"], "type": "list"}]}}]}'),
(39, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "expired_products", "operator": "no_intersection", "value": ["premium", "platinum"], "type": "list"}]}}]}'),
(40, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "optional_services", "operator": "is_null", "value": "", "type": "list"}]}}]}'),
(41, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_preferences", "field": "required_services", "operator": "is_not_null", "value": "", "type": "list"}]}}]}'),

-- NOT逻辑示例
(42, '{"logic": "NOT", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "=", "value": "VIP1", "type": "string"}]}}]}'),
(43, '{"logic": "NOT", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": "<", "value": "1000", "type": "number"}, {"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": "=", "value": "0", "type": "number"}]}}]}'),

-- 复杂的多条件组合示例
(44, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP2", "VIP3"], "type": "enum"}, {"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "100000", "type": "number"}]}}, {"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified", "type": "enum"}, {"table": "tag_test.user_basic_info", "field": "is_banned", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

(45, '{"logic": "OR", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "date_in_range", "value": ["2025-01-01", "2025-07-26"], "type": "date"}, {"table": "tag_test.user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "5", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_test.user_asset_summary", "field": "cash_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}'),

(46, '{"logic": "NOT", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_basic_info", "field": "account_status", "operator": "belongs_to", "value": ["suspended", "banned"], "type": "enum"}, {"table": "tag_test.user_activity_summary", "field": "last_login_date", "operator": "date_not_in_range", "value": ["2024-01-01", "2025-07-26"], "type": "date"}]}}]}'),

-- 字符串模糊匹配组合
(47, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_basic_info", "field": "email", "operator": "ends_with", "value": "gmail.com", "type": "string"}, {"table": "tag_test.user_basic_info", "field": "email", "operator": "ends_with", "value": "yahoo.com", "type": "string"}]}}, {"condition": {"logic": "None", "fields": [{"table": "tag_test.user_basic_info", "field": "phone_number", "operator": "starts_with", "value": "+86", "type": "string"}]}}]}'),

-- 数值范围和枚举组合
(48, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_basic_info", "field": "age", "operator": "in_range", "value": ["25", "45"], "type": "number"}, {"table": "tag_test.user_asset_summary", "field": "total_asset_value", "operator": "not_in_range", "value": ["0", "1000"], "type": "number"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP3", "VIP4", "VIP5"], "type": "enum"}, {"table": "tag_test.user_preferences", "field": "owned_products", "operator": "contains_any", "value": ["premium", "platinum"], "type": "list"}]}}]}'),

-- 列表操作和布尔组合
(49, '{"logic": "NOT", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_preferences", "field": "interested_products", "operator": "contains_all", "value": ["high_risk", "speculative"], "type": "list"}, {"table": "tag_test.user_basic_info", "field": "is_vip", "operator": "is_false", "value": "false", "type": "boolean"}]}}]}'),

-- 空值检查组合
(50, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "tag_test.user_basic_info", "field": "first_name", "operator": "is_not_null", "value": "", "type": "string"}, {"table": "tag_test.user_basic_info", "field": "last_name", "operator": "is_not_null", "value": "", "type": "string"}, {"table": "tag_test.user_basic_info", "field": "birthday", "operator": "is_not_null", "value": "", "type": "date"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "tag_test.user_basic_info", "field": "middle_name", "operator": "is_null", "value": "", "type": "string"}, {"table": "tag_test.user_preferences", "field": "optional_services", "operator": "is_null", "value": "", "type": "list"}]}}]}');



-- 显示初始化结果
SELECT '数据库初始化完成！' as status;
SELECT '标签分类数量:' as info, COUNT(*) as count FROM tag_category WHERE is_active = 1;
SELECT '标签定义数量:' as info, COUNT(*) as count FROM tag_definition WHERE is_active = 1;  
SELECT '标签规则数量:' as info, COUNT(*) as count FROM tag_rules WHERE is_active = 1;