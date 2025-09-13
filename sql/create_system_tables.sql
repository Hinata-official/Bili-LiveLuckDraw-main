-- 创建系统所需的其他表结构

-- 使用用户画像数据库
USE user_profile;

-- 1. 弹幕历史记录表：存储历史弹幕数据，便于分析和回顾
CREATE TABLE IF NOT EXISTS danmu_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    room_id INT NOT NULL COMMENT '直播间ID',
    uid BIGINT NOT NULL COMMENT '发送用户UID',
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    content TEXT NOT NULL COMMENT '弹幕内容',
    timestamp DATETIME NOT NULL COMMENT '发送时间',
    is_lucky BOOLEAN DEFAULT FALSE COMMENT '是否包含互动关键词',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='弹幕历史记录表';

-- 创建弹幕历史记录表索引
CREATE INDEX idx_room_id_timestamp ON danmu_history (room_id, timestamp DESC);
CREATE INDEX idx_uid ON danmu_history (uid);
CREATE INDEX idx_is_lucky ON danmu_history (is_lucky);

-- 2. Vtuber应答模板表：存储各种类型的应答模板，用于生成个性化回复
CREATE TABLE IF NOT EXISTS vtuber_response_template (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    template_type VARCHAR(50) NOT NULL COMMENT '模板类型（如：greeting, thanks, question等）',
    content TEXT NOT NULL COMMENT '模板内容，可包含变量如{username}、{gift_name}等',
    priority INT DEFAULT 1 COMMENT '优先级，数字越小优先级越高',
    is_enabled BOOLEAN DEFAULT TRUE COMMENT '是否启用',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Vtuber应答模板表';

-- 创建应答模板表索引
CREATE INDEX idx_template_type ON vtuber_response_template (template_type);
CREATE INDEX idx_is_enabled ON vtuber_response_template (is_enabled);

-- 3. 直播间信息表：存储直播间的基本信息
CREATE TABLE IF NOT EXISTS live_room_info (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    room_id INT NOT NULL UNIQUE COMMENT '直播间ID',
    room_name VARCHAR(100) NOT NULL COMMENT '直播间名称',
    anchor_name VARCHAR(50) NOT NULL COMMENT '主播名称',
    anchor_uid BIGINT NOT NULL COMMENT '主播UID',
    category VARCHAR(50) DEFAULT '' COMMENT '直播分类',
    status TINYINT DEFAULT 0 COMMENT '直播状态（0：未开播，1：直播中）',
    view_count INT DEFAULT 0 COMMENT '观看人数',
    last_update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '最后更新时间',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='直播间信息表';

-- 创建直播间信息表索引
CREATE INDEX idx_room_id ON live_room_info (room_id);
CREATE INDEX idx_anchor_uid ON live_room_info (anchor_uid);
CREATE INDEX idx_status ON live_room_info (status);

-- 4. 互动关键词表：存储抽奖和互动的关键词
CREATE TABLE IF NOT EXISTS lucky_words (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    room_id INT NOT NULL COMMENT '直播间ID，0表示全局关键词',
    word VARCHAR(50) NOT NULL COMMENT '关键词',
    weight INT DEFAULT 1 COMMENT '权重，用于抽奖概率',
    start_time DATETIME DEFAULT NULL COMMENT '生效开始时间',
    end_time DATETIME DEFAULT NULL COMMENT '生效结束时间',
    is_enabled BOOLEAN DEFAULT TRUE COMMENT '是否启用',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_room_word (room_id, word) -- 确保同一房间关键词不重复
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='互动关键词表';

-- 创建互动关键词表索引
CREATE INDEX idx_room_id ON lucky_words (room_id);
CREATE INDEX idx_is_enabled ON lucky_words (is_enabled);

-- 5. Vtuber应答历史表：存储已生成的个性化应答记录
CREATE TABLE IF NOT EXISTS vtuber_response_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    room_id INT NOT NULL COMMENT '直播间ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    user_name VARCHAR(50) NOT NULL COMMENT '用户名',
    danmu_content TEXT COMMENT '触发的弹幕内容',
    response_content TEXT NOT NULL COMMENT '生成的应答内容',
    response_type VARCHAR(50) DEFAULT '' COMMENT '应答类型',
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Vtuber应答历史表';

-- 创建应答历史表索引
CREATE INDEX idx_room_id_timestamp ON vtuber_response_history (room_id, timestamp DESC);
CREATE INDEX idx_user_id ON vtuber_response_history (user_id);

-- 插入一些示例数据

-- 插入应答模板示例数据
INSERT INTO vtuber_response_template (template_type, content, priority)
VALUES 
    ('greeting', '你好呀，{username}！欢迎来到直播间~', 1),
    ('thanks', '谢谢{username}的支持，mua~', 1),
    ('question', '{username}问的这个问题很有趣呢，让我想想...', 1),
    ('welcome_back', '欢迎回来，{username}！好久不见啦~', 2),
    ('gift_thanks', '哇！谢谢{username}送的{gift_name}，超级喜欢！', 1);

-- 插入互动关键词示例数据
INSERT INTO lucky_words (room_id, word, weight)
VALUES 
    (0, '抽奖', 10),
    (0, '恭喜', 5),
    (0, '主播好漂亮', 3),
    (0, '666', 2);

-- 查询所有表结构
SHOW TABLES;

-- 显示表结构详情
DESCRIBE user_profile;
DESCRIBE danmu_history;
DESCRIBE vtuber_response_template;
DESCRIBE live_room_info;
DESCRIBE lucky_words;
DESCRIBE vtuber_response_history;