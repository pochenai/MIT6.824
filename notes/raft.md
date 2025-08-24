`go test  -run 2D 6.824/raft -v -timeout 24h >log.txt -count=500`

# 核心问题
## How to avoid split votes and ensure only 1 leader is elected?
- randomized re-election
- 一个节点一个term只能有一票，所以只有两种情况需要set votedFor
    - 收到term>currentTerm，reset votedFor为null
    - grantVote时才set VotedFor为投给的节点

## How to ensure the leader store all commited log entries?
确保candidate的log至少和多数人的log一样新=> 如何定义two logs哪个更up to date?
- term更大
- log 更长

## **如何处理old RPC request/response, 保证处理结果是幂等的?**
- 直接reject old RPC
    - sendRequestAppendEntries response: 根据 nextIndex reject
    - RequestInstallSnapshot request: 如果是leader了，直接reject

# leader election
## 初始都是follower
- if election timeout && (no AppendEntries RPC || granting vote to candidates)
    - start election
        - Increament currentTerm/Vote for self => transite to candicate
        - Reset election timer
        - Vote for self, Send Request vote to all other servers
    - select如下情形之一
        - votes received from majority servers: become leader
        - AppendEntries RPC received from new leader
            - term >= current term => convert to follower; else rejects
        - timeout elapsed with no winner:
            - timeouts, start new election

- 收到AppendEntries
    - stale term, rejects
    - larger term, update current term
    - remain as follower

## 成为leader后
- repeatedly sending empty AppendEntries RPC
- AppendEntries RPC received && term > currentTerm
    - convert to follower

## Candidate
- 收到AppendEntries
    - stale term, rejects
    - larger term, revert to follower
- 收到Request Votes
    - vote at most one candidate in a given term

## 核心技术问题，怎么保证状态转换成功后，cancel调正在进行的任务
- timeout 与定时任务处理
- heartbeat与election开不同线程处理吗？
- appendRPC状态对election的影响怎么处理
- 何时reset vote for
    - 成为candidate: voted for 设置为self
    - 成为leader: 不需要重置
    - grant votes: 重置为null
    - appendEntries Request/Response, requestVote, 需要convert为follower时(term>currentTerm)
        - requestVote&appendEntriesResp: leader/follower收到更高的term, 设置currentTerm=term后，voted for设置为null
            - 必须先设置currentTerm=term, 防止针对过去的currentTerm重复投票
        - appendEntrieRequest: 也需要要求term>currentTerm采reset VotedFor,否则term==currentTerm时直接reset可能导致splitVote
            - 因为如果此前投票给leader了，此时leaderRPC消息来了，term==currentTerm, reset VoteFor
                - 如果electionTimeout还没结束，此时又收到其他节点的term==currentTerm，由于votedFor为null(不阻止投票给其他节点),此时有可能对其他节点的同一个term重复投票
- 何时reset timer：用时钟转动的思路去想
    1.receive appendEntries with term >=currentTerm
        - 注意不像votedFor, term ==currentTerm 时也需要reset timer,来防止其他节点再次start election
    2.start election
    3.grant other peer a vote
election resetrction只针对于election，不需要针对requestEntries

## 2B
区分lastApplied和commitIndex
- commitIndex表示log被多数节点共识，但该log可能还未被执行
- lastApplied表示共识后的logEntry，其中哪些已经被执行了，所以commitIndex≥lastApplied
1. sendRpc to followers
2. 根据相应信息，更新nextIndex，appendEntries
3. leader确认commitLogIndex，可以放在heartBeats时每次检查
    - N > commitIndex
    - N = majority of maxchIndex[]
    - log[N].term = currentTerm确认自己在任期内
    - set commitIndex = N
- 注意prevLogIndex是即将发送的entries的前一个index，和requestVote里面的lastLogIndex不是一个index
- 选择新leader时的term，和对比log的lastLogTerm不是一个，所以即使新leader的term较高，但是其lastLogTerm低也不会被选上


## 2C

- 最困难的地方在于sendAppendEntries网络超时，容易收到过期的response,需要保证代码是幂等的
    - 其实之前requestVote的时候也遇到过类似的问题
- 快速backup: lecture 2020: 23分钟开始
- 极端情况处理
    - 需要注意网络超时时，不能直接在sendAppendEntries中retry，因为此时可能已经不是leader了
    - Start刚发给leader，由于网络超时，其他节点开始重新选举，新leader选举出来后，由于Start的日志不再发了，导致日志check通过不了，不过lab3有retry这个问题不严重
    - Start刚发给leader，其他节点也接收到该新log，但是旧leader还没收到reply的回复，网络超时选出新leader，由于raft要求leader只能commit自己term的日志，所以尽管新leader有改新leader，但是由于没有Start发送新log，导致永远commit不了，不过lab3有retry这个问题不严重。实际上，最好设置一个no-op的空日志，leader一上来就commit才行，不过commit index会和lab2冲突，这里不能这么做

- 代码测试500次通过


## 2D
sendAppendEntries:
1. nextIndex <= rf.lastIncudedIndex
    => InstallSnapShot
2. else: appendEntries

比较困难的地方在于，理解CondSnapshot和SnapShot()何时被调用以及参数的含义：
- 这两个函数都是在函数运行过程中，接收到想用的ApplyMsg channel之后，会被cfg自动调用，所以不能设置lock，否则直接死锁
- Snapshot中传入的index是cmd index，而实际记录的是logIndex需要进行转换
    - Snapshot是异步调用的，并且不能加锁(否则有可能在applyCh中trigger snapshot导致死锁)，所以需要特殊处理下，增加一个invalidCmd，确保Snapshot在当前RPC中被处理完
- CondInstallSnapshot时需要确保如下两个事情之一:
    - 确认自己是follower
    - 不检查自己是不是follower，但不要trim所有的log，因为有可能收到old RequestInstallSnapShot的RPC，此时自己已经是leader了，如果直接把log置nil，会导致len(rf.Log) < nextIndex[server]

还有就是需要特殊处理下appendEntries时，previousLogIndex==rf.lastIncudedIndex，这种情况不需要处理，此时previousLog一定不会conflict，因为该snapshot一定是由marjority确认后，leader才会commit，进而传给follower做snapshot


## 3A
- 需要注意server和follower共用一套代码，代码上需要进行合理的区分
    - 只有leader需要reply
    - 但是leader和follower都需要有状态
- 2021年的3A测试文件有问题，会导致linearizability测试不通过，需要换成2020年的
