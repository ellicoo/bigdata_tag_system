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
CREATE TABLE IF NOT EXISTS user_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL COMMENT '用户ID',
    tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组',
    tag_details JSON COMMENT '标签详细信息（key-value形式）',
    computed_date DATE NOT NULL COMMENT '计算日期',
    INDEX idx_user_id (user_id),
    INDEX idx_computed_date (computed_date),
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

-- 标签规则（JSON格式）
INSERT IGNORE INTO tag_rules (tag_id, rule_conditions) VALUES 
(1, '{"logic": "AND", "conditions": [{"field": "total_asset_value", "operator": ">=", "value": 100000, "type": "number"}]}'),
(2, '{"logic": "AND", "conditions": [{"field": "trade_count_30d", "operator": ">", "value": 10, "type": "number"}]}'),
(3, '{"logic": "AND", "conditions": [{"field": "risk_score", "operator": "<=", "value": 30, "type": "number"}]}'),
(4, '{"logic": "AND", "conditions": [{"field": "registration_date", "operator": "recent_days", "value": 30, "type": "date"}]}'),
(5, '{"logic": "AND", "conditions": [{"field": "user_level", "operator": "in", "value": ["VIP2", "VIP3"], "type": "string"}, {"field": "kyc_status", "operator": "=", "value": "verified", "type": "string"}]}'),
(6, '{"logic": "AND", "conditions": [{"field": "cash_balance", "operator": ">=", "value": 50000, "type": "number"}]}'),
(7, '{"logic": "AND", "conditions": [{"field": "age", "operator": "in_range", "value": [18, 30], "type": "number"}]}'),
(8, '{"logic": "AND", "conditions": [{"field": "last_login_date", "operator": "recent_days", "value": 7, "type": "date"}]}');

-- 显示初始化结果
SELECT '数据库初始化完成！' as status;
SELECT '标签分类数量:' as info, COUNT(*) as count FROM tag_category WHERE is_active = 1;
SELECT '标签定义数量:' as info, COUNT(*) as count FROM tag_definition WHERE is_active = 1;  
SELECT '标签规则数量:' as info, COUNT(*) as count FROM tag_rules WHERE is_active = 1;