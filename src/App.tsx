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
}

interface Participant {
    name: string;
    uid: number;
}

function App() {
    const {toast} = useToast();
    const openPageHandle = () => {
        window.ipcRenderer.send('open-page', 'https://www.bilibili.com/')
    }

    const realtimeListRef = useRef<HTMLDivElement>(null);
    const realtimeUserListRef = useRef<HTMLDivElement>(null);

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

    const handleDanmuMsg = (_event: any, data: any) => {
        console.log("获取到弹幕信息", data);
        setIsFetching(true);
        setMsgList((prev) => {
            return [...prev, {
                name: data.name,
                uid: data.uid,
                content: data.content,
            }]
        })
        // 滚动到底部
        setTimeout(() => {
            realtimeListRef.current?.scrollIntoView({
                block: 'end',
                behavior: 'smooth',
            });
        }, 500)
    };

    const handleAddUser = (_event: any, data: any) => {
        console.log("获取到参与者信息", data);
        setParticipants((prev) => {
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

        // 修复：移除监听器时传入与注册时完全相同的函数引用
        return () => {
            window.ipcRenderer.removeListener('user-info', handleUserInfo);
            window.ipcRenderer.removeListener('danmu_msg', handleDanmuMsg);
            window.ipcRenderer.removeListener('add_user', handleAddUser);
        }
    }, []);

    const [roomId, setRoomId] = useState<string>('');
    const [isLogin, setIsLogin] = useState<boolean>(false);

    const [msgList, setMsgList] = useState<DanMuItem[]>([]);
    const [participants, setParticipants] = useState<Participant[]>([]);

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
                        {msgList.map((msg, index) => (
                            <div key={index} className="flex items-center gap-2 py-1">
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
                        {participants.map((participant, index) => (
                            <div key={index} className="flex items-center gap-2 py-1">
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
            <Toaster />
        </>
    );
}

export default App;
