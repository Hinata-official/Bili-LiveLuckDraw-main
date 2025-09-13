-- 创建用户画像数据库
CREATE DATABASE IF NOT EXISTS user_profile;

-- 使用该数据库
USE user_profile;

-- 创建用户画像表
CREATE TABLE IF NOT EXISTS user_profile (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    uid BIGINT NOT NULL UNIQUE COMMENT '用户UID，关联B站用户',
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    gender VARCHAR(10) DEFAULT 'unknown' COMMENT '性别',
    age INT DEFAULT 0 COMMENT '年龄',
    avatar VARCHAR(255) DEFAULT '' COMMENT '头像URL',
    tags VARCHAR(255) DEFAULT '' COMMENT '用户标签，用逗号分隔',
    level INT DEFAULT 0 COMMENT '用户等级',
    is_vip BOOLEAN DEFAULT FALSE COMMENT '是否VIP',
    watch_time INT DEFAULT 0 COMMENT '观看时长（分钟）',
    gift_amount DECIMAL(10,2) DEFAULT 0.00 COMMENT '送礼金额',
    last_active_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '最后活跃时间',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户画像表';

-- 创建索引以提高查询性能
CREATE INDEX idx_uid ON user_profile (uid);
CREATE INDEX idx_username ON user_profile (username);
CREATE INDEX idx_last_active_time ON user_profile (last_active_time);

-- 插入一些示例数据
INSERT INTO user_profile (uid, username, gender, age, avatar, tags, level, is_vip, watch_time, gift_amount)
VALUES 
    (10001, '张三', 'male', 25, 'https://example.com/avatar1.jpg', '游戏,动漫,科技', 5, 1, 1200, 200.50),
    (10002, '李四', 'female', 22, 'https://example.com/avatar2.jpg', '美妆,时尚,旅行', 3, 0, 800, 50.20),
    (10003, '王五', 'male', 30, 'https://example.com/avatar3.jpg', '音乐,体育,电影', 7, 1, 2500, 500.80),
    (10004, '赵六', 'female', 18, 'https://example.com/avatar4.jpg', '学习,美食,宠物', 2, 0, 500, 20.30),
    (10005, '钱七', 'male', 28, 'https://example.com/avatar5.jpg', '编程,技术,职场', 6, 1, 1800, 300.60);

-- 查询用户画像表数据
SELECT * FROM user_profile LIMIT 10;