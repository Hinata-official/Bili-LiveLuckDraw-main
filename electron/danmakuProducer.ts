import { app } from 'electron';
import { Kafka } from 'kafkajs';
// 修复Kafka重复导入问题：确认仅导入一次Kafka客户端

// 1. 初始化 Kafka 客户端（宿主机访问使用localhost）
const kafka = new Kafka({
    clientId: 'bili-danmu-producer', // 客户端标识（自定义）
    brokers: ['localhost:9092'] // 宿主机访问Kafka服务
});

// 2. 创建生产者实例
const producer = kafka.producer();
let isKafkaConnected = false; // 连接状态标记

// 3. 连接 Kafka 生产者
export async function initKafkaProducer() {
    if (isKafkaConnected) return; // 避免重复连接
    try {
        await producer.connect();
        isKafkaConnected = true;
        console.log('[Kafka] 生产者连接成功！');
    } catch (error) {
        console.error('[Kafka] 生产者连接失败：', error);
        throw error; // 抛出错误，方便开发阶段排查
    }
}

// 4. 发送弹幕到 Kafka 主题（bili-danmu）
export async function sendDanmuToKafka(danmu: {
    uid: number;
    username: string;
    content: string;
    roomId: number;
}) {
    if (!isKafkaConnected) {
        console.error('[Kafka] 生产者未连接，无法发送弹幕');
        return;
    }

    try {
        await producer.send({
            topic: 'bili-danmu', // 目标主题（需提前创建）
            messages: [
                {
                    key: danmu.uid.toString(), // 消息 key（用用户 ID，便于分区）
                    value: JSON.stringify({ // 消息内容（序列化）
                        ...danmu,
                        timestamp: Date.now() // 新增时间戳
                    })
                }
            ]
        });
        console.log(`[Kafka] 发送弹幕成功：用户 ${danmu.username} - ${danmu.content}`);
    } catch (error) {
        console.error('[Kafka] 发送弹幕失败：', error);
    }
}

// 5. 应用退出时断开 Kafka 连接（避免资源泄漏）
app.on('before-quit', async () => {
    if (isKafkaConnected) {
        await producer.disconnect();
        console.log('[Kafka] 生产者已断开连接');
    }
});