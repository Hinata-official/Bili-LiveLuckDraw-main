import './App.css'
import {Button} from "@/components/ui/button.tsx";
import {useEffect, useRef, useState} from "react";
import {Input} from "@/components/ui/input.tsx";
import {
    Dialog, DialogClose,
    DialogContent,
    DialogDescription, DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger
} from "@/components/ui/dialog.tsx";
import {Avatar, AvatarFallback, AvatarImage} from "@/components/ui/avatar.tsx";
import {useToast} from "@/hooks/use-toast";
import {Toaster} from "@/components/ui/toaster.tsx";
import {ScrollArea} from "@/components/ui/scroll-area.tsx";
import {Badge} from "@/components/ui/badge.tsx";

interface DanMuItem {
    name: string;
    uid: number;
    content: string;
    timestamp?: number; // 添加时间戳字段用于去重
}

interface Participant {
    name: string;
    uid: number;
}

interface VtuberResponse {
    userId: number;
    userName: string;
    userAvatar?: string;
    response: string;
    timestamp: number;
}

function App() {
    const {toast} = useToast();
    const openPageHandle = () => {
        window.ipcRenderer.send('open-page', 'https://www.bilibili.com/')
    }

    const realtimeListRef = useRef<HTMLDivElement>(null);
    const realtimeUserListRef = useRef<HTMLDivElement>(null);
    const vtuberResponsesRef = useRef<HTMLDivElement>(null);

    const startHandle = () => {
        if (!isLogin) {
            toast({
                title: '请先登录',
                description: '登录后才能获取详细的弹幕信息哦',
            })
            return;
        }
        if (!roomId) {
            toast({
                title: '请先设置直播间 id',
                description: '要先设置好直播间 id 才能开始获取哦',
            })
            return;
        }
        if (luckyWord === '') {
            toast({
                title: '请先设置互动词',
                description: '要先设置好互动词才能开始获取哦',
            })
            return;
        }
        window.ipcRenderer.send('start', roomId)
        setIsFetching(true);
    }

    const stopHandle = () => {
        setIsFetching(false);
        window.ipcRenderer.send('stop')
    }

    const [user, setUser] = useState({
        name: '',
        avatar: '',
    });

    // 修复：将事件处理函数提取为变量，确保removeListener能正确移除相同的函数引用
    const handleUserInfo = (_event: any, data: any) => {
        console.log("获取到用户信息", data);
        // 这里能够获取到用户信息就说明 cookie 可以啦，可以开抓咯
        if (data === null) {
            return;
        }
        setUser({
            name: data.uname,
            avatar: data.face,
        });
        setIsLogin(true);
    };

    // 处理Vtuber应答消息
    const handleVtuberResponse = (_event: any, data: VtuberResponse) => {
        console.log("获取到Vtuber应答消息", data);
        setVtuberResponses(prev => {
            // 添加去重逻辑，避免相同的应答重复显示
            const now = Date.now();
            const isDuplicate = prev.some(
                msg => msg.userId === data.userId && 
                       msg.response === data.response && 
                       now - msg.timestamp < 5000 // 5秒内相同的用户和响应内容视为重复
            );
            
            if (isDuplicate) {
                return prev;
            }
            
            // 限制消息数量，避免无限增长
            const newResponses = [...prev, data];
            if (newResponses.length > 100) {
                newResponses.shift(); // 移除最早的消息
            }
            return newResponses;
        });
        
        // 滚动到底部
        setTimeout(() => {
            vtuberResponsesRef.current?.scrollIntoView({
                block: 'end',
                behavior: 'smooth',
            });
        }, 500);
    };

    const handleDanmuMsg = (_event: any, data: any) => {
        console.log("获取到弹幕信息", data);
        // 不再每次收到弹幕都设置isFetching，只在start/stop时设置
        
        // 过滤逻辑：
        // 1. 如果是直接来自WebSocket的消息(directFromWebSocket:true)，则直接添加
        // 2. 如果是来自Flink的消息(fromFlink:true)，则跳过，避免重复显示
        if (!data.fromFlink) {
            setMsgList((prev) => {
                // 添加去重逻辑，避免同一弹幕重复显示
                // 检查是否在最近500ms内有相同用户发送的相同内容
                const now = Date.now();
                const isDuplicate = prev.some(
                    msg => msg.uid === data.uid && 
                           msg.content === data.content && 
                           msg.timestamp && 
                           now - msg.timestamp < 500
                );
                
                if (isDuplicate) {
                    return prev;
                }
                
                return [...prev, {
                    name: data.name,
                    uid: data.uid,
                    content: data.content,
                    timestamp: now // 添加时间戳用于去重
                }]
            })
            // 滚动到底部
            setTimeout(() => {
                realtimeListRef.current?.scrollIntoView({
                    block: 'end',
                    behavior: 'smooth',
                });
            }, 500)
        }
    };

    const handleAddUser = (_event: any, data: any) => {
        console.log("获取到参与者信息", data);
        setParticipants((prev) => {
            // 添加去重逻辑，避免同一用户重复显示
            const isDuplicate = prev.some(
                participant => participant.uid === data.uid
            );
            
            if (isDuplicate) {
                return prev;
            }
            
            return [...prev, {
                name: data.name,
                uid: data.uid,
            }]
        });
        // 滚动到底部
        setTimeout(() => {
            realtimeUserListRef.current?.scrollIntoView({
                block: 'end',
                behavior: 'smooth',
            });
        }, 500)
    };

    useEffect(() => {
        window.ipcRenderer.on('user-info', handleUserInfo);
        window.ipcRenderer.on('danmu_msg', handleDanmuMsg);
        window.ipcRenderer.on('add_user', handleAddUser);
        window.ipcRenderer.on('vtuber-response', handleVtuberResponse);

        // 修复：移除监听器时传入与注册时完全相同的函数引用
        return () => {
            window.ipcRenderer.removeListener('user-info', handleUserInfo);
            window.ipcRenderer.removeListener('danmu_msg', handleDanmuMsg);
            window.ipcRenderer.removeListener('add_user', handleAddUser);
            window.ipcRenderer.removeListener('vtuber-response', handleVtuberResponse);
        }
    }, []);

    const [roomId, setRoomId] = useState<string>('');
    const [isLogin, setIsLogin] = useState<boolean>(false);

    const [msgList, setMsgList] = useState<DanMuItem[]>([]);
    const [participants, setParticipants] = useState<Participant[]>([]);
    const [vtuberResponses, setVtuberResponses] = useState<VtuberResponse[]>([]);

    const [isFetching, setIsFetching] = useState<boolean>(false);
    const [luckyWord, setLuckyWord] = useState<string>('');

    useEffect(() => {
        window.ipcRenderer.send('lucky-word', luckyWord)
    }, [luckyWord]);

    const resetHandle = () => {
        window.ipcRenderer.send('reset')
    }

    return (
        <>
            <div className="app-container flex flex-col mb-4">
                <div className="info-container flex justify-between items-center">
                    <div className="room-info-container">
                        <span> 当前直播间 id：</span>
                         {
                            roomId ? roomId : '未设置'
                        }
                        <Dialog>
                            <DialogTrigger>
                                <Button variant={'link'}> 去设置 </Button>
                            </DialogTrigger>
                            <DialogContent>
                                <DialogHeader>
                                    <DialogTitle>
                                        设置直播间 id
                                    </DialogTitle>
                                    <DialogDescription>
                                        <div className={'mb-4'}>
                                            请查看直播间地址中的数字部分（如：https://live.bilibili.com/12345678）并填写到下方输入框中 <br/>
                                            程序将自动解析直播间的弹幕信息，并尝试连接弹幕服务器
                                        </div>
                                        <Input value={roomId} onChange={(e) => {
                                            // 这里只允许输入数字
                                            setRoomId(e.target.value.replace(/[^\d]/g, ''))
                                        }}/>
                                    </DialogDescription>
                                </DialogHeader>
                                <DialogFooter className="sm:justify-start">
                                    <DialogClose asChild>
                                        <Button type="button" variant="secondary">
                                            可以啦
                                        </Button>
                                    </DialogClose>
                                </DialogFooter>
                            </DialogContent>
                        </Dialog>
                    </div>
                    {isLogin && (
                        <div className="user-info-container flex items-center">
                            <Avatar>
                                <AvatarImage src={user.avatar} alt={user.name}/>
                                <AvatarFallback>USER</AvatarFallback>
                            </Avatar>
                            <span className="ml-2">{user.name}</span>
                        </div>
                    )}
                    {!isLogin && (
                        <div className="user-info-container flex items-center">
                            <span className="text-sm text-gray-700">登录后才能获取详细的弹幕信息哦</span>
                            <Button variant={'ghost'} onClick={openPageHandle} className="ml-2">前往登录</Button>
                        </div>
                    )}
                </div>
                <div className="flex gap-4 mt-4">
                    <div className="flex-1">
                        <div className="mb-4">
                            <Input
                                placeholder="请输入互动关键词"
                                className="w-full"
                                value={luckyWord}
                                onChange={(e) => setLuckyWord(e.target.value)}
                            />
                        </div>
                        <div className="flex gap-2">
                            {isFetching ? (
                                <Button onClick={stopHandle} variant="destructive">
                                    停止获取
                                </Button>
                            ) : (
                                <Button onClick={startHandle}>
                                    开始获取
                                </Button>
                            )}
                            <Button onClick={resetHandle} variant="secondary">
                                清空参与者
                            </Button>
                            <Button onClick={openPageHandle} variant="secondary">
                                打开B站
                            </Button>
                        </div>
                    </div>
                </div>
            </div>
            <div className="flex gap-4">
                <div className="flex-1">
                    <div className="flex justify-between items-center mb-2">
                        <h2 className="text-lg font-semibold">实时弹幕列表</h2>
                        <Badge variant={isFetching ? "default" : "outline"}>
                            {isFetching ? '获取中' : '未获取'}
                        </Badge>
                    </div>
                    <ScrollArea className="h-96 border rounded-lg p-2">
                        {msgList.map((msg) => (
                            <div key={msg.uid + '-' + Date.now()} className="flex items-center gap-2 py-1">
                                <div className="font-medium text-sm w-20 truncate">
                                    {msg.name}
                                </div>
                                <div className="text-sm flex-1">
                                    {msg.content}
                                </div>
                            </div>
                        ))}
                        <div ref={realtimeListRef} />
                    </ScrollArea>
                </div>
                <div className="flex-1">
                    <div className="flex justify-between items-center mb-2">
                        <h2 className="text-lg font-semibold">参与抽奖的观众</h2>
                        <Badge variant="secondary">
                            {participants.length} 人
                        </Badge>
                    </div>
                    <ScrollArea className="h-96 border rounded-lg p-2">
                        {participants.map((participant) => (
                            <div key={participant.uid} className="flex items-center gap-2 py-1">
                                <div className="font-medium text-sm w-20 truncate">
                                    {participant.name}
                                </div>
                                <div className="text-sm text-gray-500">
                                    UID: {participant.uid}
                                </div>
                            </div>
                        ))}
                        <div ref={realtimeUserListRef} />
                    </ScrollArea>
                    </div>
                </div>
                
                {/* Vtuber应答消息区域 */}
                <div className="mt-4">
                    <div className="flex justify-between items-center mb-2">
                        <h2 className="text-lg font-semibold">Vtuber 个性化应答</h2>
                        <Badge variant="outline">
                            {vtuberResponses.length} 条
                        </Badge>
                    </div>
                    <ScrollArea className="h-48 border rounded-lg p-2 bg-gradient-to-br from-purple-50 to-blue-50">
                        {vtuberResponses.map((response, index) => (
                            <div key={index} className="py-2 border-b border-gray-100 last:border-0">
                                <div className="flex items-center gap-2 mb-1">
                                    <div className="font-medium text-sm w-20 truncate">
                                        {response.userName}
                                    </div>
                                    <div className="text-xs text-gray-400">
                                        {new Date(response.timestamp).toLocaleTimeString()}
                                    </div>
                                </div>
                                <div className="text-sm text-gray-700 pl-2 border-l-2 border-purple-300">
                                    {response.response}
                                </div>
                            </div>
                        ))}
                        <div ref={vtuberResponsesRef} />
                    </ScrollArea>
                </div>
                
                <Toaster />
            </>
        );
}

export default App;
