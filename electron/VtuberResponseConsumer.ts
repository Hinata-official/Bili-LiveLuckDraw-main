import { Kafka, Consumer, ConsumerRunConfig } from 'kafkajs';
import { app, webContents } from 'electron';

/**
 * Vtuber应答消息消费者
 * 负责从Kafka的vtuber-response主题消费应答消息，并推送给前端显示
 */
export class VtuberResponseConsumer {
    private kafka: Kafka;
    private consumer: Consumer;
    private isConnected: boolean = false;
    
    // Kafka配置（宿主机访问使用localhost）
    private readonly brokers: string[] = ['localhost:9092'];
    private readonly clientId: string = 'bili-vtuber-consumer';
    private readonly groupId: string = 'vtuber-response-group';
    private readonly topic: string = 'vtuber-response';
    
    constructor() {
        // 初始化Kafka客户端
        this.kafka = new Kafka({
            clientId: this.clientId,
            brokers: this.brokers
        });
        
        // 创建消费者实例
        this.consumer = this.kafka.consumer({
            groupId: this.groupId,
            allowAutoTopicCreation: true
        });
        
        // 应用退出时清理资源
        app.on('before-quit', async () => {
            if (this.isConnected) {
                await this.disconnect();
            }
        });
    }
    
    /**
     * 初始化并连接Kafka消费者
     */
    public async initialize(): Promise<void> {
        if (this.isConnected) {
            console.log('[VtuberResponseConsumer] 消费者已经连接，无需重复初始化');
            return;
        }
        
        try {
            // 连接到Kafka
            await this.consumer.connect();
            
            // 订阅主题
            await this.consumer.subscribe({
                topic: this.topic,
                fromBeginning: false // 从最新消息开始消费
            });
            
            this.isConnected = true;
            console.log('[VtuberResponseConsumer] 消费者连接成功，已订阅主题:', this.topic);
        } catch (error) {
            console.error('[VtuberResponseConsumer] 初始化失败:', error);
            throw error;
        }
    }
    
    /**
     * 开始消费应答消息
     */
    public async startProcessing(): Promise<void> {
        if (!this.isConnected) {
            throw new Error('[VtuberResponseConsumer] 消费者未初始化，请先调用initialize方法');
        }
        
        try {
            // 配置消费行为
            const runConfig: ConsumerRunConfig = {
                eachMessage: async ({ message }) => {
                    try {
                        // 解析消息内容
                        const messageValue = message.value?.toString('utf-8');
                        if (!messageValue) {
                            console.warn('[VtuberResponseConsumer] 收到空消息，跳过处理');
                            return;
                        }
                        
                        // 解析JSON格式的应答数据
                        const responseData = JSON.parse(messageValue);
                        
                        // 验证必要字段
                        if (!responseData.userId || !responseData.response) {
                            console.warn('[VtuberResponseConsumer] 消息格式不正确，缺少必要字段:', messageValue);
                            return;
                        }
                        
                        console.log('[VtuberResponseConsumer] 收到应答消息:', responseData);
                        
                        // 将消息推送给所有打开的窗口
                        this.broadcastToAllWindows('vtuber-response', responseData);
                        
                    } catch (error) {
                        console.error('[VtuberResponseConsumer] 处理消息失败:', error, '消息内容:', message.value?.toString('utf-8'));
                    }
                }
            };
            
            // 启动消费
            await this.consumer.run(runConfig);
            console.log('[VtuberResponseConsumer] 开始消费应答消息');
        } catch (error) {
            console.error('[VtuberResponseConsumer] 启动处理失败:', error);
            throw error;
        }
    }
    
    /**
     * 停止消费并断开连接
     */
    public async disconnect(): Promise<void> {
        if (!this.isConnected) {
            return;
        }
        
        try {
            await this.consumer.disconnect();
            this.isConnected = false;
            console.log('[VtuberResponseConsumer] 消费者已断开连接');
        } catch (error) {
            console.error('[VtuberResponseConsumer] 断开连接失败:', error);
        }
    }
    
    /**
     * 向所有打开的窗口广播消息
     */
    private broadcastToAllWindows(channel: string, data: any): void {
        try {
            // 获取所有打开的窗口
            const windows = webContents.getAllWebContents();
            
            // 向每个窗口发送消息
            windows.forEach((window) => {
                if (window && !window.isDestroyed()) {
                    window.send(channel, data);
                }
            });
        } catch (error) {
            console.error('[VtuberResponseConsumer] 广播消息失败:', error);
        }
    }
}

// 导出单例实例
const vtuberResponseConsumer = new VtuberResponseConsumer();
export default vtuberResponseConsumer;

// 导出辅助函数，方便调用
/**
 * 初始化Vtuber应答消费者
 */
export async function initializeVtuberResponseConsumer(): Promise<void> {
    try {
        await vtuberResponseConsumer.initialize();
        await vtuberResponseConsumer.startProcessing();
        console.log('[VtuberResponseConsumer] 初始化成功');
    } catch (error) {
        console.error('[VtuberResponseConsumer] 初始化失败:', error);
        // 可以选择在这里重试初始化逻辑
        setTimeout(() => {
            console.log('[VtuberResponseConsumer] 尝试重新初始化...');
            initializeVtuberResponseConsumer().catch(e => {
                console.error('[VtuberResponseConsumer] 重新初始化失败:', e);
            });
        }, 5000); // 5秒后重试
    }
}