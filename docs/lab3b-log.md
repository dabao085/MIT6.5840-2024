这个实验(`lab3B`)真是把我写吐了，我在工作之余的时候来写这个实验，写了半个月。有时候为了通过后面的测试用例改了代码，前面的用例就无法通过了。  
我读了很多遍论文中的算法描述，也读了多遍的测试代码(即Lab的框架代码)，加了很多日志，最后才能不断地往下走，真是太难了。

# 0. 注意事项
这个实验对于我本人来说是很难的，建议多读几遍论文中日志复制的算法描述部分、`test_test.go`和相关代码，不至于两眼一抹黑。

# 1. 写在前面
修正实验3A中出现的错误
## 1.1 选举限制
在进行实验3B的过程中，我发现在实验3A中的获取选票函数，即`func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply)`未能遵循`5.4.1 选举限制`的内容。后来我做了更新。

# 2. 函数功能介绍
为了能够尽快的找到编写实验的切入点，我会对几个重要函数的功能进行介绍
## 2.1 AppendEntries函数
函数原型是`func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)`  
该函数首先进行日志的一致性检查，只有在通过一致性检查之后，方能添加`AppendEntriesArgs`携带的日志。  
通过日志一致性检查后，`reply.Success`返回`true`，否则返回`false`。

## 2.2 RequestVote函数
需要增加**选举限制**的判断（参见1.1节）。

## 2.3 Start函数
Start函数根据入参构造`LogEntry`，添加到自己的`log`数组里，并发送`AppendEntry`给`Follower`，进行一次agreement。

## 2.4 sendAppendEntries函数
这个函数是我自己新增的，它被`Leader`调用，作用是把`Leader`的日志发送给各个`Follower`，并且在收到半数的成功回复后，将这条日志置为`committed`。  
注意这里发向每个`Follower`的日志不一定一样，因为有的`Follower`落后`Leader`太多，要发送它们之间不一致的日志。具体细节留到后面讨论。

## 2.5 sendAppendEntry函数
这个函数是我自己新增的，用于`Leader`发送给单个`Follower`，`sendAppendEntries`函数循环调用`sendAppendEntry`函数，里面的细节也很多。

## 2.6 applyLog函数
这个函数是我自己新增的，作用是循环检测是否有`committed`的日志。对于这些日志，需要进行应用(apply)。

## 2.7 sendHeartbeats函数
这个函数是我自己新增的，作用是循环调用`sendHeartbeat`函数，维持`Leader`的权威，也用于快速日志恢复。

## 2.8 sendHeartbeat函数
这个函数是我自己新增的，作用是维持`Leader`的权威，也用于快速日志恢复。

# 3. 要点难点解析
## 3.1 committed和applied
这两个概念起初我并没有认真对待，导致有测试用例没法通过，其实仔细阅读论文的描述即可弄清楚。  
这里重新描述一下: 当`Leader`把日志通过`AppendEntries`复制到半数以上的节点(包括`Leader`自身)后，认为`committed`(即已提交)，并且通知其他节点`Leader`的`commitIndex`。  
节点(例如`Leader`、`Follower`等)把已提交的日志应用到状态机，即把`ApplyMsg`写入`applyCh`，认为applied(即已应用)。  

对于`Follower`:
(这是最常见的情况)接收到`Leader`的`committed`的通知后，把对应日志认为是`committed`，并更新自身的`commitIndex`，供apply线程(`applyLog`函数)去应用。
`Leader`发送心跳给`Follower`时,可能会附带日志的`committed`，`Follower`可以根据这个信息来更新日志状态为`committed`。  

## 3.2 日志索引从0还是1开始？
为了使日志的下标和日志索引(`log index`)保持一致，并且从1开始（测试程序从1读取log），我在`Raft`节点初始化时，添加了一个空的`logEntry`到log里。

## 3.3 心跳的发送需要异步
我犯了一个低级错误，就是在依次发送心跳的时候，错误的使用了同步发送的模式。这导致如果其中一个`Follower`断开连接了，那么如果有后面一个`Follower`，那么后面这个`Follower`就无法收到心跳包了。我改成了异步发送心跳包解决了这个问题。

## 3.4 正确更新commitIndex和lastApplied
`Raft`节点的`commitIndex`表示本节点已经提交的日志的下标，`lastApplied`下标到`commitIndex`下标之间的内容，可以被应用(apply)。
`Leader`节点附加日志(`AppendEntry`)在收到半数确认后，即可更新`commitIndex`。

## 3.5 日志的一致性检查
重中之重的内容，放到后面进行讨论

# 4. test_test.go文件中函数和测试用例介绍
## 4.1 func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int函数

## 4.2 TestBasicAgree3B
用于测试验证3轮agreement是否都能达成。注意这里需要3轮agreement都要正确apply，否则无法通过测试。  
主要考察日志是否能正确添加append、提交commit、应用apply。

## 4.3 TestRPCBytes3B
测试RPC传输的数据量是否超标?

## 4.4 TestFollowerFailure3B
测试少数`Follower`断开后，集群是否能够正常提交与应用日志。  
测试多数`Follower`断开时，集群是否不能提交日志。

## 4.5 TestLeaderFailure3B
测试3个节点的集群中，1个`Leader`失联后，剩下的两个`Follower`能否正确选择`Leader`。
测试2个节点的集群中，1个`Leader`失联后，仅剩的一个节点是否不能提交（1个`Leader`节点无法提交，因为剩下的两个节点挂了，它永远达不到半数的确认）。

## 4.6 TestFailAgree3B
测试3个节点的集群中，一个`Follower`断开一段时间后重新连接，能否与`Leader`重新保持日志的一致。

# 5. 测试通过
[lab3b_pass](../images/Lab3b_pass.png)
## 5.1 一些待改进的地方
* 暂时未加上-race进行测试
* 连续测试20次的时候，会有5次报错
[Lab3B_20_times](../images/Lab3B_20_times.png)
错误的信息是
```text
apply error: commit index=2 server=0 102 != server=2 103
```
[Lab3b_Failed_TestRejoin](../images/Lab3b_Failed_TestRejoin.png)


(未完待续 to be continued)
