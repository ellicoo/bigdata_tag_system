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
  `user_id` varchar(128) NOT NULL COMMENT '用户ID',
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
(50, '完整信息用户', 'COMPLETE_INFO', '10', 1, 1, 2, 2, '姓名生日完整且可选服务为空的用户', '姓名完整 AND 生日完整 AND 可选服务为空', 'system'),

-- === 新增DWS颗粒化字段标签定义 (71-90) ===
-- 基于新的DWS表结构颗粒化字段的标签
(71, '现货资产优势用户', 'SPOT_ASSET_ADVANTAGE', '7', 1, 1, 2, 2, '现货持仓超过合约持仓的用户', '现货持仓 > 合约持仓', 'system'),
(72, '合约交易专家', 'CONTRACT_TRADING_EXPERT', '2', 1, 1, 2, 2, '合约交易量大且有合约交易经验的用户', '合约交易量 >= 50000 AND 有合约交易', 'system'),
(73, '理财爱好者', 'FINANCE_ENTHUSIAST', '8', 1, 1, 2, 2, '理财持仓和交易次数都较高的用户', '理财持仓 > 10000 AND 理财交易次数 > 5', 'system'),
(74, '链上活跃用户', 'ONCHAIN_ACTIVE', '2', 1, 1, 2, 2, '在链上有持仓和交易活动的用户', '链上持仓 > 0 AND 链上交易次数 > 3', 'system'),
(75, '现货流动性提供者', 'SPOT_LIQUIDITY_PROVIDER', '2', 1, 1, 2, 2, '现货可用余额高且交易频繁的用户', '现货可用余额 > 50000 AND 现货交易次数 > 20', 'system'),
(76, '合约资金管理者', 'CONTRACT_FUND_MANAGER', '7', 1, 1, 2, 2, '合约资金充足且风险控制良好的用户', '合约可用余额 > 20000 AND 合约锁仓 < 5000', 'system'),
(77, '理财保守型用户', 'FINANCE_CONSERVATIVE', '5', 1, 1, 2, 2, '理财锁仓超过可用余额的长期持有用户', '理财锁仓 > 0 AND 理财锁仓 > 理财可用余额', 'system'),
(78, '多元化投资者', 'DIVERSIFIED_INVESTOR', '1', 1, 1, 2, 2, '现货、合约、理财都有持仓的多元化投资用户', '现货+合约+理财 都有持仓', 'system'),
(79, '现货专一用户', 'SPOT_ONLY_USER', '5', 1, 1, 2, 2, '只持有现货资产，其他持仓为零的专一用户', '只有现货持仓，其他持仓为0', 'system'),
(80, '高频现货交易者', 'HIGH_FREQ_SPOT_TRADER', '2', 1, 1, 2, 2, '现货近30日交易活跃且交易频繁的用户', '现货近30日交易额 > 10000 AND 现货交易次数 > 15', 'system'),
(81, '合约近期活跃用户', 'CONTRACT_RECENT_ACTIVE', '2', 1, 1, 2, 2, '合约近30日交易活跃的用户', '合约近30日交易额 > 20000 AND 合约交易次数 > 10', 'system'),
(82, '全业务活跃用户', 'ALL_BUSINESS_ACTIVE', '2', 1, 1, 2, 2, '现货、合约、理财、链上都有交易的全能用户', '现货+合约+理财+链上 都有交易', 'system'),
(83, '资产均衡用户', 'BALANCED_ASSET_USER', '7', 1, 1, 2, 2, '现货、合约、理财持仓水平相近的均衡用户', '现货、合约、理财持仓都在5000-50000范围', 'system'),
(84, '链上投资专家', 'ONCHAIN_INVESTMENT_EXPERT', '8', 1, 1, 2, 2, '链上持仓占比较高的DeFi投资专家', '链上持仓 > 10000 AND 链上交易次数 > 5', 'system'),
(85, '现金流管理专家', 'CASH_FLOW_MANAGER', '7', 1, 1, 2, 2, '可用余额超过锁仓金额的现金流管理用户', '总可用余额 > 总锁仓金额', 'system'),
(86, '理财产品重度用户', 'FINANCE_HEAVY_USER', '8', 1, 1, 2, 2, '理财交易次数很高的理财产品重度使用用户', '理财交易次数 > 10 AND 已开通理财', 'system'),
(87, '跨链DeFi用户', 'CROSS_CHAIN_DEFI_USER', '8', 1, 1, 2, 2, '有链上交易且持有主流币种的DeFi用户', '链上交易 > 0 AND 持有BTC或ETH', 'system'),
(88, '资产安全意识用户', 'SECURITY_CONSCIOUS_USER', '3', 1, 1, 2, 2, '锁仓金额超过可用余额的风险控制意识强用户', '各类锁仓总额 > 可用余额总额', 'system'),
(89, '新兴业务探索者', 'EMERGING_BUSINESS_EXPLORER', '8', 1, 1, 2, 2, '积极参与链上等新兴业务的探索型用户', '链上交易次数 > 3 AND 链上持仓 > 5000', 'system'),
(90, '综合高价值用户', 'COMPREHENSIVE_HIGH_VALUE', '1', 1, 1, 2, 2, '各类持仓和交易都达到高标准的综合高价值用户', '各类持仓都 > 20000 AND 各类交易都活跃', 'system');


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
(12, '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "total_trading_volume", "operator": ">=", "value": "100000", "type": "number"}]}}]}'),
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
(57, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "100000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "total_trading_volume", "operator": ">=", "value": "500000", "type": "number"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_kyc_completed", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "!=", "value": "true", "type": "boolean"}]}}]}'),

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
(63, '{"logic": "AND", "conditions": [{"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": "<=", "value": "1000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "total_trading_volume", "operator": "=", "value": "0", "type": "number"}]}}]}'),

-- 多层嵌套复杂逻辑示例
-- 优质活跃用户：(高资产 OR 高交易) AND (近期登录 OR 频繁登录) AND NOT(高风险 OR 封禁)
(64, '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "50000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "spot_recent_30d_volume", "operator": ">=", "value": "100000", "type": "number"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_activity_df", "field": "days_since_last_login", "operator": "<=", "value": "3", "type": "number"}, {"table": "dws_user.dws_user_activity_df", "field": "login_count_7d", "operator": ">=", "value": "5", "type": "number"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_risk_df", "field": "is_high_risk_ip", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_risk_df", "field": "is_blacklist_user", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

-- 完整信息的投资者：(KYC已认证 AND 2FA已开启) AND (有交易经验 OR 有资产) AND NOT(代理用户)
(65, '{"logic": "AND", "conditions": [{"condition": {"logic": "AND", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_kyc_completed", "operator": "=", "value": "true", "type": "boolean"}, {"table": "dws_user.dws_user_profile_df", "field": "is_2fa_enabled", "operator": "=", "value": "true", "type": "boolean"}]}}, {"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "total_trading_volume", "operator": ">", "value": "0", "type": "number"}, {"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">", "value": "0", "type": "number"}]}}, {"condition": {"logic": "NOT", "fields": [{"table": "dws_user.dws_user_profile_df", "field": "is_agent", "operator": "=", "value": "true", "type": "boolean"}]}}]}'),

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



-- 显示初始化结果
SELECT '数据库初始化完成！' as status;
SELECT '标签分类数量:' as info, COUNT(*) as count FROM tag_category WHERE is_delete = 0;
SELECT '标签定义数量:' as info, COUNT(*) as count FROM tag_definition WHERE is_active = 1;  
SELECT '标签规则数量:' as info, COUNT(*) as count FROM tag_rules_config WHERE is_active = 1;