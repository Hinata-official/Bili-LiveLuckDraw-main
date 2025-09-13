import fetch from 'node-fetch';
import WebSocket from 'ws';
import { Buffer } from 'buffer';
import DanmuExtractor from './DanmuExtractor';
import { getBuvid3, getCookies, getUid, sendMsgToRenderer } from "./main.ts";
import fetchCookie from 'fetch-cookie';
import { ipcMain } from "electron";
import { signRequest } from "./signTool.ts";
import { sendDanmuToKafka } from './danmakuProducer.ts';

// -------------------------- 1. 补充必要的类型接口（TypeScript 核心） --------------------------
/** 发送到 Kafka 的弹幕数据结构 */
interface DanmuToKafkaParams {
    uid: number;
    username: string;
    content: string;
    roomId: number;
}
/** B站 room_init 接口响应类型 */
interface RoomInitResponse {
    data: {
        room_id: number;
    };
    code: number; // 补充接口状态码，TypeScript 类型更完整
}
export const fetchWithCookies = fetchCookie(fetch);
/** B站 getDanmuInfo 接口响应类型 */
interface DanmuInfoResponse {
    data: {
        host_list: Array<{ host: string; wss_port: number }>;
        token: string;
    };
    code: number;
}

/** 弹幕消息体基础类型（简化，根据实际格式调整） */
interface DanmuMsgContent {
    cmd: string;
    info: Array<never>; // 实际场景可拆分为更具体的类型，如 [number, string, [number, string]]
    roomid?: number; // 可选，避免类型报错
}

// -------------------------- 2. 全局状态管理（单开场景，确保唯一连接/监听） --------------------------
let globalWs: WebSocket | null = null; // 唯一 WebSocket 实例
let globalMsgHandler: ((content: DanmuMsgContent) => void) | null = null; // 唯一消息回调（明确类型）

const danmuExtractor = new DanmuExtractor(); // 弹幕提取器单例

// -------------------------- 3. 工具函数（带完整类型定义） --------------------------
/**
 * 获取B站真实房间号（从短ID/自定义ID转换）
 * @param shortId 前端输入的直播间短ID/自定义ID
 * @returns 真实房间号（number）
 */
export async function getRoomId(shortId: string): Promise<number> {
    try {
        const response = await fetch(`https://api.live.bilibili.com/room/v1/Room/room_init?id=${shortId}`);
        const data = await response.json() as RoomInitResponse;

        if (data.code !== 0 || !data.data?.room_id) {
            throw new Error(`获取真实房间号失败，接口响应：${JSON.stringify(data)}`);

        }
        console.log("获取真实房间号：", data.data.room_id);
        return data.data.room_id;
    } catch (error) {
        console.error("getRoomId 错误：", error);
        throw error; // 抛出错误，让调用方处理
    }
}

/**
 * 获取弹幕服务器信息（地址+token）
 * @param roomId 真实房间号
 * @returns 弹幕服务器配置
 */
export async function getDanmuInfo(roomId: number): Promise<DanmuInfoResponse['data']> {
    try {
        const params = { id: roomId, web_location: 444.8, type: 0 };
        const args = await signRequest(params);
        // 使用类型断言避免Cookie类型冲突
        const cookies = getCookies().map((cookie: any) => `${cookie.name}=${cookie.value}`).join('; ');

        const response = await fetchWithCookies(
            `https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?${args}`,
            {
                headers: { Cookie: cookies },
                method: 'GET' // 显式指定请求方法，TypeScript 更规范
            }
        );

        const data = await response.json() as DanmuInfoResponse;
        if (data.code !== 0 || !data.data) {
            throw new Error(`获取弹幕服务器信息失败，接口响应：${JSON.stringify(data)}`);
        }
        return data.data;
    } catch (error) {
        console.error("getDanmuInfo 错误：", error);
        throw error;
    }
}

/**
 * 生成B站WebSocket鉴权包
 * @param roomId 真实房间号
 * @param token 弹幕服务器token
 * @param _uid 当前用户UID
 * @param buvid 设备标识
 * @returns 鉴权包Buffer
 */
function generateCertificate(
    roomId: number,
    token: string,
    _uid: number,
    buvid: string
): Buffer {
    const headerLength = 16;
    const protocol = 1;
    const type = 7;
    const sequence = 2;
    const body = JSON.stringify({
        uid: _uid,
        roomid: roomId,
        protover: 2,
        buvid: buvid,
        platform: 'web',
        type: 2,
        key: token,
    });

    const bodyBuffer = Buffer.from(body, 'utf-8'); // 显式指定编码，TypeScript 更安全
    const headerBuffer = Buffer.alloc(headerLength);
    headerBuffer.writeUInt32BE(headerLength + bodyBuffer.length, 0);
    headerBuffer.writeUInt16BE(headerLength, 4);
    headerBuffer.writeUInt16BE(protocol, 6);
    headerBuffer.writeUInt32BE(type, 8);
    headerBuffer.writeUInt32BE(sequence, 12);

    return Buffer.concat([headerBuffer, bodyBuffer]);
}

/** 生成WebSocket心跳包 */
function generateHeartbeat(): Buffer {
    const headerLength = 16;
    const protocol = 1;
    const type = 2;
    const sequence = 2;
    const body = '[Object object]';

    const bodyBuffer = Buffer.from(body, 'utf-8');
    const headerBuffer = Buffer.alloc(headerLength);
    headerBuffer.writeUInt32BE(headerLength + bodyBuffer.length, 0);
    headerBuffer.writeUInt16BE(headerLength, 4);
    headerBuffer.writeUInt16BE(protocol, 6);
    headerBuffer.writeUInt32BE(type, 8);
    headerBuffer.writeUInt32BE(sequence, 12);

    return Buffer.concat([headerBuffer, bodyBuffer]);
}

/**
 * 发送鉴权包到WebSocket服务器
 * @param ws WebSocket实例
 * @param roomId 真实房间号
 * @param token 弹幕服务器token
 * @param uid 当前用户UID
 * @param buvid 设备标识
 */
function sendCertificate(
    ws: WebSocket,
    roomId: number,
    token: string,
    uid: number,
    buvid: string
): void {
    const certificate = generateCertificate(roomId, token, uid, buvid);
    console.log("发送认证包：", certificate.toString('hex').match(/.{1,32}/g)?.join('\n'));
    ws.send(certificate);
}

/**
 * 处理WebSocket接收的消息（转发给弹幕提取器）
 * @param _ws WebSocket实例
 * @param data 接收的消息数据
 */
function handleWebSocketMessages(_ws: WebSocket, data: WebSocket.Data): void {
    // TypeScript 类型守卫：确保data是Buffer
    if (data instanceof Buffer) {
        danmuExtractor.getDmMsg(data);
    }
}

// -------------------------- 4. 抽奖逻辑（带类型定义） --------------------------
let participants: number[] = []; // 单开场景，全局抽奖参与者列表
let keyword: string = ''; // 单开场景，全局抽奖关键词

/**
 * 关键词匹配（判断是否加入抽奖）
 * @param content 弹幕内容
 * @param userName 用户名
 * @param uid 用户ID
 */
function checkForKeywords(
    content: string,
    userName: string,
    uid: number
): void {
    if (!keyword) return;

    if (content.includes(keyword) && !participants.includes(uid)) {
        participants.push(uid);
        sendMsgToRenderer('add_user', { name: userName, uid: uid });
        console.log(`匹配到关键词：${keyword}，${userName}（UID: ${uid}）进入抽奖`);
    }
}

// -------------------------- 5. IPC事件监听（单开场景，全局绑定） --------------------------
/** 监听前端设置抽奖关键词 */
ipcMain.on('lucky-word', (_event, word: string) => {
    keyword = word;
    console.log('设置抽奖关键词：', word);
});

/** 监听前端重置抽奖 */
ipcMain.on('reset', () => {
    participants = [];
    console.log('重置抽奖：清空参与者列表');
});

// -------------------------- 6. 核心：WebSocket连接管理（单开场景，确保唯一） --------------------------
/**
 * 启动WebSocket连接（单开场景，自动关闭旧连接）
 * @param shortId 前端输入的直播间短ID/自定义ID
 */
export async function startWebSocket(shortId: string): Promise<void> {
    console.log('开始初始化WebSocket连接（前端输入ID：', shortId, '）');

    // 第一步：关闭旧连接+清理旧监听（核心修复：避免重复）
    if (globalMsgHandler) {
        console.log('清理旧弹幕消息监听');
        danmuExtractor.off('MsgData', globalMsgHandler);
        globalMsgHandler = null;
    }
    if (globalWs) {
        if (globalWs.readyState !== WebSocket.CLOSED && globalWs.readyState !== WebSocket.CLOSING) {
            console.log('关闭旧WebSocket连接');
            globalWs.close();
        }
        globalWs = null;
    }

    try {
        // 第二步：获取真实房间号（TypeScript 确保roomId是number）
        const realRoomId = await getRoomId(shortId);
        const danmuInfo = await getDanmuInfo(realRoomId);

        // 第三步：创建新WebSocket连接（TypeScript 显式指定选项类型）
        const ws = new WebSocket(
            `wss://${danmuInfo.host_list[0].host}:${danmuInfo.host_list[0].wss_port}/sub`,
            {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                },
                protocolVersion: 13 // WebSocket标准协议版本，TypeScript 可选配置
            }
        );
        globalWs = ws;

        // 第四步：绑定弹幕消息监听（单开场景，仅绑定一次）
        const handleMsgData = (content: DanmuMsgContent) => {
            // 只处理普通弹幕（TypeScript 类型守卫：确保cmd是字符串）
            if (content.cmd !== 'DANMU_MSG') return;

            try {
                // 解析弹幕数据（TypeScript 解构+类型检查，避免数组越界）
                const userInfo = content.info[2] as [number, string]; // [UID, 用户名]
                const [uid, userName] = userInfo || [0, '未知用户'];
                const danmuContent = content.info[1] as string;

                // 校验必要字段（TypeScript 显式判断，避免null/undefined）
                if (!uid || !userName || !danmuContent || !realRoomId) {
                    console.warn('[弹幕解析] 字段缺失，跳过转发：', {
                        uid, userName, danmuContent, realRoomId,
                        rawContent: JSON.stringify(content)
                    });
                    return;
                }

                // 1. 转发到Kafka（TypeScript 接口约束，确保参数结构正确）
                const kafkaData: DanmuToKafkaParams = {
                    uid,
                    username: userName,
                    content: danmuContent,
                    roomId: realRoomId
                };
                sendDanmuToKafka(kafkaData);

                // 2. 打印日志（TypeScript 模板字符串，类型安全）
                console.log(`[房间 ${realRoomId}] ${userName}（UID: ${uid}）的弹幕：${danmuContent}`);

                // 3. 直接发送到前端渲染的弹幕（在UI中显示）
                sendMsgToRenderer('danmu_msg', {
                    name: userName,
                    uid: uid,
                    content: danmuContent,
                    directFromWebSocket: true // 添加标记，区分直接来自WebSocket的消息
                });

                // 4. 抽奖关键词匹配
                checkForKeywords(danmuContent, userName, uid);

            } catch (error) {
                console.error('[弹幕处理异常]：', error, '原始数据：', JSON.stringify(content));
            }
        };

        // 绑定监听并保存引用（用于后续清理）
        danmuExtractor.on('MsgData', handleMsgData);
        globalMsgHandler = handleMsgData;

        // 第五步：WebSocket事件回调（TypeScript 完整类型）
        ws.on('open', () => {
            console.log(`[房间 ${realRoomId}] WebSocket 连接成功`);
            const currentUid = getUid();
            const currentBuvid = getBuvid3();
            if (currentUid && currentBuvid) {
                sendCertificate(ws, realRoomId, danmuInfo.token, currentUid, currentBuvid);
            } else {
                console.error('[认证失败] UID/BUVID 获取为空，无法发送鉴权包');
                ws.close();
            }
        });

        ws.on('message', (data) => {
            handleWebSocketMessages(ws, data);
        });

        ws.on('error', (err: Error) => {
            console.error(`[房间 ${realRoomId}] WebSocket 错误：`, err.message);
        });

        ws.on('close', (code: number, reason: Buffer) => {
            console.log(`[房间 ${realRoomId}] WebSocket 连接关闭：`, {
                code,
                reason: reason.toString('utf-8')
            });
            // 清理当前连接的监听
            if (globalMsgHandler) {
                danmuExtractor.off('MsgData', globalMsgHandler);
                globalMsgHandler = null;
            }
            globalWs = null;
        });

        // 第六步：监听前端"停止"事件（单开场景，全局唯一）
        const handleStop = () => {
            keyword = '';
            console.log(`[房间 ${realRoomId}] 接收前端停止指令，关闭WebSocket`);
            ws.close(1000, '前端主动停止'); // 1000=正常关闭码
            ipcMain.off('stop', handleStop); // 清理一次性事件
        };
        ipcMain.once('stop', handleStop); // once：确保只触发一次

        // 第七步：心跳包（10秒一次，TypeScript 定时器类型）
        const heartbeatTimer = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(generateHeartbeat());
            }
        }, 10000);

        // 连接关闭时清理定时器
        ws.on('close', () => {
            clearInterval(heartbeatTimer);
        });

    } catch (error) {
        console.error('WebSocket 初始化失败：', error);
        throw error;
    }
}

// -------------------------- 【关键】删除原全局MsgData监听（重复弹幕的根源！） --------------------------
// 确保代码中没有这部分全局绑定：
// danmuExtractor.on('MsgData', (content) => { ... })