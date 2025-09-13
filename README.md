<img src="/public/icon.png" alt="logo" width="200"/>

# Bili-LiveLuckDraw

[![GitHub license](https://img.shields.io/github/license/grtsinry43/Bili-LiveLuckDraw)](https://opensource.org/license/MIT)
[![GitHub release](https://img.shields.io/github/v/release/grtsinry43/Bili-LiveLuckDraw)](https://github.com/grtsinry43/Bili-LiveLuckDraw/releases)
[![Last commit](https://img.shields.io/github/last-commit/grtsinry43/Bili-LiveLuckDraw)](https://github.com/grtsinry43/Bili-LiveLuckDraw/commits/main)

---

## 项目简介

Bili-LiveLuckDraw是一个完整的B站直播弹幕处理系统，集成了实时弹幕采集、个性化应答生成、用户画像分析和抽奖功能。该系统使用Electron作为前端框架，结合Kafka、Flink和MySQL构建了一个高效的实时数据处理流水线。

## 系统架构

```
┌─────────────────┐     ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│ B站直播间弹幕   │ ──> │ Electron采集器 │ ──> │ Kafka 消息队列│ ──> │ Flink 处理器  │
└─────────────────┘     └───────────────┘     └───────────────┘     └───────┬───────┘
                                                                             │
                                                                             ▼
┌─────────────────┐     ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│ 前端应用展示    │ <── │ Electron UI   │ <── │ Kafka 应答队列│ <── │ MySQL 用户画像│
└─────────────────┘     └───────────────┘     └───────────────┘     └───────────────┘
```

## 技术栈

- **前端框架**：React + TypeScript
- **桌面应用**：Electron
- **消息队列**：Kafka
- **流处理**：Apache Flink
- **数据库**：MySQL
- **构建工具**：Vite
- **打包工具**：electron-builder

## 功能特性

1. **实时弹幕采集**：通过WebSocket连接B站直播弹幕服务器，实时获取弹幕数据
2. **个性化应答**：基于用户画像和弹幕内容，生成个性化的Vtuber风格应答
3. **抽奖功能**：支持设置互动关键词，自动统计符合条件的参与者
4. **用户画像分析**：结合MySQL存储的用户画像数据，进行精准的用户分析
5. **数据可视化**：实时展示弹幕列表、参与者信息和Vtuber应答

## 快速开始

### 环境要求

- Node.js 16+ 
- npm 7+ 
- Java 8+ (Flink运行环境)
- MySQL 5.7+ 
- Kafka 2.x+

### 安装步骤

1. **克隆仓库**
```bash
git clone https://github.com/grtsinry43/Bili-LiveLuckDraw.git
cd Bili-LiveLuckDraw
```

2. **安装依赖**
```bash
pnpm install
```

3. **配置环境**

   修改`config/system-config.json`文件，根据您的环境配置Kafka、MySQL等参数。

4. **初始化数据库**

   导入`sql`目录下的SQL文件，创建必要的数据库表：
   ```bash
   mysql -u root -p < sql/create_user_profile.sql
   mysql -u root -p < sql/create_system_tables.sql
   ```

5. **启动Kafka和ZooKeeper**

   确保您的Kafka和ZooKeeper服务已经启动，并创建以下主题：
   ```bash
   kafka-topics.sh --create --topic bili-danmu --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic vtuber-response --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic danmu-processing-errors --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

6. **启动Flink作业**

   编译并运行Flink作业：
   ```bash
   cd flink
   mvn clean package
   flink run -c com.bilibili.DanmuProcessorJob target/bili-danmu-processor-1.0.jar
   ```

7. **启动应用**

   在项目根目录下执行：
   ```bash
   pnpm run dev
   ```

## 使用方法

在 Releases 中下载最新版本的安装包，安装并运行即可。

使用时在右上角登录B站账号，然后输入直播间号，点击开始获取即可。当结束获取后，点击开始抽奖即可。

### 详细使用步骤

1. **登录B站**

   点击"前往登录"按钮，在弹出的浏览器窗口中登录您的B站账号。

2. **设置直播间ID**

   点击"去设置"按钮，输入您想要监控的直播间ID。

3. **设置互动关键词**

   在输入框中输入互动关键词，例如"抽奖"。

4. **开始监控**

   点击"开始获取"按钮，系统将开始采集弹幕数据并自动统计符合条件的参与者。

5. **查看结果**

   - **实时弹幕列表**：显示当前采集到的所有弹幕
   - **参与抽奖的观众**：显示符合互动关键词条件的观众
   - **Vtuber个性化应答**：显示系统生成的个性化应答消息

## 屏幕截图

![截图效果](https://github.com/user-attachments/assets/ad5d7713-c495-4fc7-9260-58a6959e216a)

## 开发

```bash
pnpm install
```

```bash
pnpm run dev
```

## 打包

```bash
pnpm run build
```

## 配置说明

系统主要配置文件为`config/system-config.json`，包含以下主要配置项：

- **kafka**: Kafka相关配置，包括brokers地址、topics名称等
- **mysql**: MySQL数据库连接配置
- **flink**: Flink作业配置，包括检查点、重启策略等
- **bilibili**: B站API和WebSocket相关配置
- **application**: 应用程序基本配置

## License

MIT License
