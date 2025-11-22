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

## **如何处理old RPC request/response, 保证异步处理结果是幂等的?**
- 直接reject old RPC
    - sendRequestAppendEntries response: 根据 nextIndex reject
    - RequestInstallSnapshot request: 如果是leader了，直接reject
## 超时、去重、幂等、并发请求和响应对应、channel退出防止内存泄漏等问题，需要定义清楚##

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
    - Start刚发给leader，其他节点也接收到该新log，但是旧leader还没收到reply的回复，网络超时选出新leader，由于raft要求leader只能commit自己term的日志，所以尽管新leader有该新leader的log，但是由于没有Start发送新log，导致永远commit不了，不过lab3有retry这个问题不严重。实际上，最好设置一个no-op的空日志，leader一上来就commit才行，不过commit index会和lab2冲突，这里不能这么做

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
- **对于超时+去重处理：如何保证response是对于同一个client的？如何保证同一个client的request不被处理两次**
    - 同一client的request seq number不能变，否则有可能被执行两次。但遇到以下问题，若seq=6先被处理了，seq5才来，那么有几种处理方式:
        - 1.直接按最大seq丢弃seq5（目前的做法），这样的好处理时只用维护ClientID=>maxSeq的对应关系，坏处是seq5被丢弃
        - 2.判断是否重复，若不重复在处理，坏处是需要维护所有处理过的历史信息
    - 不同client共用了同一个cmdIndex,这种时候需要在applyCh到来时判断result.Cid != args.Cid || result.Seq != args.Seq，如果不一致，那么超时client的request(发动给leader A)一定是由新的leader B commit的消息触发的，此处需要返回Wrongleader错误并重试
    - 同一client的消息超时之后重新发送给新leader，但是都是同一个cmdIndex，需要注意double close channel
        - 在旧leader A超时，打算异步close ch，但假设还没获取lock， pendingCh还未被delete
        - 新leader B正常响应，触发自己的reqest handler(但获取的同一个pendingCh对象)，处理完了准备异步close ch
            - 此时leaderA开始close ch,delete pendingCh
            - leaderB close相同的ch，导致double close channel
- **channel同步如果没有正确关闭，会导致的go routine内存泄露问题**
    - applyCh没有发送/接收方，会导致卡死程序无法退出，进而导致GoGC无法进行垃圾回收
    - 可以通过profiling 观察goroutine数目看出来(内置profiling的程序到go程序里)


## 4B
- Group是否需要自行update config(即除了moveshard/updateShardState外，单独设置一个update Config op不带state的op)，以防止client访问到旧的shards?
    - 不论网络分区时间长短的都是没有必要的，因为一旦其他shards收到move shards的请求，会导致拥有该shard的server切换shard，并且client端请求的也一直是最新的shard config
        - 极限情况是Group A在config n拥有shard S, 然后Config n+1 move shard A to Group B，但是还没move完，Config n+2又把S分配给Group A(A会请求Group B move shard)
            - 可以看出此时A会向上一个shard owner请求move，所以client此时不能直接访问group A中的S，因为A还没有ready
- Move shards从哪一个config开始同步呢？最新的还是从最开始的开始？，尤其是如果Config有间断怎么处理
    - 假设从latestConfig开始，G:group, s:shard
        - ctrl更新
            - C1: G1(s1,s2)
            - C2: G1(s1) G2(s2)
        - 此时G2收到C2 update (c1没收到，因为ctrl更新的太快了)
            - 向G1申请move s2
                - G1检查当前config，发现是0，可以返回1.空的s2; 2.shards不可用
                    - 1.空的s2，是否可能导致一致性出问题，即s2实际上不是空的呢？有可能，比如
                        - C1: G1(S1,S2,S3)， 此时G1收到了C1配置，并处理了与s3相关请求
                        - C2: G1(s1, s2), G2:(s3),  此时G1收到了C2的配置
                        - C3: G1(s1), G2(s2), G3(s3), 此时G2,G3直接收到了C3配置(跳过了C1,C2), G3向G2请求move s3, G2检查config发现是0，直接发送空的就出错了，因为S3在C1处理过请求
                    - 2.shards不可用：
                        - 此时G1返回shard不可用错误,并异步请求C1中的G1 move到C1(s1,s2)，G2等待后重试
                            - G1收到move C1请求发现，前一个config是0，则更新自己到C1
                - G1每次updateShards时(update了shards一部分需要update config，并且标记当前的num中available情况)，并且要保证顺序更新
                    - 否则，在上例中
                        - G2先收到C3 update, 然后pull C2中G1的s2, 成功，此时若直接更新G2到C3，会导致G3 pull G2的s3得到空的结果
                        - 因此，需要在update Cn 时进行前一个config(Cn-1)的可用性校验，假设当前config为Cc (Cn必须大于Cc)
                            - Cc=0, 需要确保前一个Cn-1中没有自己own的shards，可以直接更新Config+(部分)可用性状态
                            - Cc<Cn-1, 则reject, 返回waitingForSync (此时client端需要重试)
                            - Cc=Cn-1, 则判断当前available与否，如果available则更新Config+(部分)可用性状态；否则返回waitingForSync
                            - Cc=Cn, 直接更新
                            - Cc>Cn,说明状态过期，不用处理
    - 合理的方式是从当前的config开始更新，那么问题是被请求的shard该如何更新？
        - invariant: 只有当前config是avaliable才能更新到下一个config；只能返回当前config之前的
        - 如果发现请求的group是自己，
            - 并且NewConfig=当前config+1
                - 若当前config avaliable，那么直接update到下一个Config(注意不要返回旧state，防止move了旧的state，然后client又在相同的shards写入，然后updateShards把旧的state覆盖到新的state上)
                - 若当前config unavaliable，那么reject，需要等当前config请求到其他的shards
            - 若newConfig<=当前Config，则reject，因为没必要更新
        - 如果请求是来自其他client 
            - 同样NewConfig=当前config+1
                - 若当前config avaliable，则update到下一个config，并且把旧state返回(必须update到下一个config，防止后面client继续写入该shard)
                - 若当前config unavaliable, 同样reject，需要等当前config请求到其他的shards
            - 若new config <= 当前config，则返回newconfig -1的state


- 如何保证new config时，没有move的shard可以继续支持访问？
    - 判断当前的config num的shards，所以判断service的话首先是num，其次是available。
        - client访问时是否需要携带num信息？对于client影响不大，因为client能找到正确的group是谁
    

- msgSeq是否同步?
    - 如果不同步msgSeq，那么moveshard之前client 请求append，group1也正确处理了，并moveshards给了Group2, 然后还没来得及返回给client，断网了；
        - 然后，client会重新请求Group2 append，导致重复请求write，违反linearizability

- 节点move shards时该如何同步？
    - 尤其是有的shards可能落后了很多Config才加入，这时候能否直接更新到最新的Config？
        - 不能，除非确定当前的Config是0，才能从其他节点MoveShards成功之后一次性更新，并且要保证其他节点的状态都是对的。否则可能存在如下情况：
         - C1：G1(s1), G2(s2), G3(s3)
         - C2: G1(s1, s3), G2(s2)
         - C3: G1(s1), G2(s2), G3(s3)  => 此时G3如果只判断自己在前一个Config(C2)没有s3，就更新是不行的因为C2可能已经更新了s3.
    - 新shards进来之后，如果从第0个开始更新那么必然非常耗时，那么该如何做呢？
        - 进来之后判断之前的Config是否是0，是的话MoveShards来满足当前Config(Cn)所需，如果move失败，则再尝试更新到Cn-1，如果失败则往前更新config，直到Ci更新成功，再逐次更新到Cn，以防中间请求的moveshards还没有同步完。
            - 这种方式会存在消息不断重试的问题，不如让shardcontroller记录每个节点的第一个config，然后从哪个config开始同步，这样后面的因为有snapshot，不会落后太多

### dead lock问题
- kvraft中由于timeout更高，导致server在timeout之后applyCh中的cmd发送的Ch无人接收，直接将server block住了
```
select {
	case result := <-ch:
    case <-time.After(raft.RaftElectionTimeout):
    ...
}

kv.mu.Lock()
```

### 最终的protocol, client:
- 查latestConfig, Cfg_lts, 假设当前Config为Cc
    - Cfg_lts > Cc
        - if Cc.synced, sync from Cc+1 -> C_lts
           Cn.next=Cc+1, Cn.prev = Cc
            ...
           Cn.next=C_lts, Cn.prev = C_lts-1
        - if Cc.not synced, sync from Cc -> C_lts
           Cn.next=Cc, Cn.prev = Cc-1
           ...
           Cn.next=C_lts, Cn.prev = C_lts-1
```    
func sync(Cn):
    1.Move owned shards -> 不用update shards，直至成功；即使leader切换也没影响
    2.Move shards from others -> update shards 直至成功；
        - 如果本方leader切换并已经切换到Cn且已经更新了一些状态, 会导致old leader(当前leader)拿到旧的state，然后向新leader更新旧state，此时需要再updateShards中拒绝掉旧状态

```

> 对于新加入的group，落后的config过多，可以通过shardController记录每个group的初始config来优化
> client收到错误会导致死循环的情况:
    - 节点的config正常增长，那么server返回的错误最终会消失，因为最终会收敛到所有节点都是相同的Config
        - 对于MoveShards走1，正常；UpdateShards走到2，也是正常
    - 核心是节点的leader切换带来的问题:
        - 请求方leader切换: 新leader更新到新的状态了如Cc+i，对于MoveShard和UpdateShards的几种情况没本质影响；
        - 处理方leader切换，此时若请求没进去会返回wrong leader错误，重试；若请求被处理方新leader处理，则也没影响

### 最终的protocol, kvserver MoveShards(Cn):
如下所有条件，若fromGid==kv.gid，则返回值不需要携带state；否则，返回值需要携带state
- 1.Cn.prev < Cc, 返回state for shards
- 2.Cn.prev = Cc:
    - 2.1若synced，则返回state，并更新到Cn(注意Cc和Cn共同owned shards(fromGid==kv.gid)不能返回，否则这中间对这些shards写操作，然后update shards时会用旧shards来覆盖掉)
        - Cc和Cn共同owned shards更新为avaliable，其他的置为unavaliable(这个是幂等操作，下次一的config比当前的大才能update，所以不用担心重复请求)
    - 2.2若非synced, 则reject，返回ErrWaitSyncing错误
- 3.Cn.prev > Cc: reject, 返回ErrWaitSyncing错误

### 最终的protocol, kvserver UpdateShards(Cn):
- 1.Cn.next < Cc, 返回成功，说明已经更新过了
- 2.Cn.next= Cc, update unvaliable的shards state,返回成功
- 3.Cn.next=Cc+1：
    - 3.1若synced，则更新到Cn.next,并设置avaliable shards和state为: 1.Cc和Cn共同owned shards 2.CC.state
    - 3.2否则，reject
- 4.Cn.next>Cc+1: reject


#### 一些小细节
- restoreState的kv.avaliable需要先初始化，否则会merge上次的config，而不单单是重置读取到的，如果不初始化会出现unexpected behavior.
- pullShards的时候需要注意这里面是一个循环，如果kv.killed之后需要退出，否则会导致主进程退出后子进程依然再run，出现undefined behavior影响下一次测试(这个bug找了很久才找到!)
- shardctrler.Client.Query里面也是一个循环，如果节点被killed之后会一直在里面死循环不退出，导致GC没法回收内存OOM，需要设置超时后直接返回默认值(应该返回错误更好，就没管了)
- lab4B TestStaticShards测试本身有个bug导致测试多轮会OOM，需要把channel 改成non-blocking的
