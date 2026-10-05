Raft Lite là một implementation tối giản của thuật toán Raft Consensus bằng Java.  
Hỗ trợ các thành phần core của Raft gồm: leader election, log replication, state machine apply và snapshot.

## Tính năng
- Leader election (pre-vote có kiểm tra leader còn sống, check quorum, no-op khi nhậm chức)
- Log replication; mọi thao tác ghi đĩa của Raft (log, term/vote, snapshot) nằm ngoài lock của node, fsync được gộp cho nhiều entry
- State machine apply
- Snapshot save/load, InstallSnapshot cho follower tụt lại; `createSnapshot()` hoàn tất bất đồng bộ
- Thêm và gỡ node bằng joint consensus (`onJoinPeerCluster`, `onLeavePeerCluster`); leader tự gỡ mình sẽ trao quyền cho follower khác, node bị gỡ tự shutdown khi biết chắc mình đã rời cluster
- RPC in-process (`InMemoryRpcClient`) và qua socket (`SocketRpcClient` + `SocketRpcServer`)

## Tuỳ chọn trong `NodeOptions`
- `shutdownOnRemoved` (mặc định `true`): node tự shutdown khi bị gỡ khỏi cluster; `false` thì node chỉ đứng yên
- `departingTimeoutMs` (mặc định 10000): leader cố gửi cấu hình cuối cho node vừa bị gỡ trong bao lâu

## Viết state machine
Xem Javadoc của `StateMachine`. `onSnapshotSave` nên chụp bản sao state rồi ghi file ở thread khác (như `ListStateMachine`);
`onSnapshotLoad` trả về `false` hoặc ném exception đều làm node dừng hẳn để restart dựng lại từ đĩa.

## Yêu cầu
- JDK 21+
- Maven

## Chạy
```bash
mvn test                                  # test cluster in-memory
mvn compile
# demo 3 node in-process: partition leader, rejoin, thêm node D, snapshot
java -cp "target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" com.namnv.AppRaftInMem
# demo tương tự qua socket trên localhost:8080-8083
java -cp "target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" com.namnv.AppRaftSocket
```
Cả hai demo xoá thư mục `data/` ở thư mục hiện tại khi khởi động.

## Dữ liệu trên đĩa
Mỗi node ghi vào thư mục cấu hình trong `NodeOptions`:
- `raft_meta.json`: term, votedFor, commit index
- `log_<firstIndex>.jsonl`: các segment của log, mỗi dòng một entry; snapshot xoá nguyên segment cũ
- `snapshot_<index>/`: file do state machine ghi (`ListStateMachine` dùng `snapshot.data`) kèm meta của snapshot; ghi vào `temp/` rồi rename nên luôn nguyên vẹn
