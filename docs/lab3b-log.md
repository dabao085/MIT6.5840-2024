# 1. 要点难点解析
## 1.1 committed和applied
这两个概念起初我并没有认真对待，导致有测试用例没法通过。  
当Leader把日志通过AppendEntries复制到半数以上的节点(包括Leader自身)后，认为committed(即已提交)，并且通知其他节点。  
节点(例如Leader)把已提交的日志应用到状态机，即把ApplyMsg写入applyCh，认为applied。  

对于Follower:
(这是最常见的情况)接收到Leader的committed的通知后，把对应日志认为是committed。
Leader发送心跳给Follower时,可能会附带日志的committed，Follower可以根据这个信息来更新日志状态为committed。  

## 1.2 日志索引从0还是1开始？
为了使日志的下标和日志索引(log index)保持一致，并且从1开始（测试程序从1读取log），我在Raft节点初始化时，添加了一个空的logEntry到log里。

## 1.3 心跳的发送需要异步
我犯了一个低级错误，就是在依次发送心跳的时候，错误的使用了同步发送的模式。这导致如果其中一个Follower断开连接了，那么如果有后面一个Follower，那么后面这个Follower就无法收到心跳包了。我改成了异步发送心跳包解决了这个问题。

