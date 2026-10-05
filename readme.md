# Raft Lite

Raft Lite là một cài đặt gọn của thuật toán đồng thuận Raft bằng Java, viết để học và thử nghiệm.
Nó nhân bản một log lệnh qua nhiều node, áp dụng log đó lên state machine của bạn theo cùng một thứ tự trên mọi node,
và giữ được dữ liệu khi node chết, mất điện hay mạng bị chia cắt.

> **Trạng thái:** đủ tốt để học, demo và làm nền thử nghiệm. Chưa nên dùng cho hệ thống giữ dữ liệu thật;
> xem [Giới hạn](#giới-hạn-đã-biết).

## Mục lục

- [Tính năng](#tính-năng)
- [Bắt đầu nhanh](#bắt-đầu-nhanh)
- [Dùng như một thư viện](#dùng-như-một-thư-viện)
- [Tuỳ chọn của node](#tuỳ-chọn-của-node)
- [Viết state machine](#viết-state-machine)
- [Kiến trúc](#kiến-trúc)
- [Cách hoạt động](#cách-hoạt-động)
- [Dữ liệu trên đĩa](#dữ-liệu-trên-đĩa)
- [Vận hành](#vận-hành)
- [Transport](#transport)
- [Test](#test)
- [Giới hạn đã biết](#giới-hạn-đã-biết)

## Tính năng

| Nhóm | Có gì |
|---|---|
| Bầu cử | Pre-vote, từ chối pre-vote khi leader còn sống, leader tự step-down khi mất đa số (check quorum), no-op khi nhậm chức |
| Nhân bản log | Gom nhiều entry vào một request, lùi nhanh khi log lệch, một request đang bay cho mỗi follower |
| Độ bền | Log chia segment có CRC32; fsync trước mọi lời hứa với node khác; mọi thao tác ghi đĩa nằm ngoài lock của node |
| Snapshot | Tạo bất đồng bộ, lưu atomic, gửi cho follower tụt lại (InstallSnapshot), compact log bằng cách xoá segment |
| Thành viên | Thêm và gỡ node bằng joint consensus, trao quyền leader, node bị gỡ tự tắt |
| Client | Ghi có chống trùng (exactly-once theo `clientId` + `sequence`), đọc nhất quán bằng ReadIndex |
| Transport | In-memory cho test, socket TCP có lọc deserialization và dùng lại kết nối |
| Kiểm thử | Mô phỏng tất định theo seed, fault injection chạy thread thật, test tất định cho từng quy tắc an toàn |

## Bắt đầu nhanh

Yêu cầu: JDK 21 trở lên và Maven.

```bash
mvn test                                  # toàn bộ test, khoảng 45 giây
mvn compile

# demo 3 node trong một tiến trình: ghi lệnh, cô lập leader, nối lại, thêm node D, snapshot
java -cp "target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" com.namnv.AppRaftInMem

# demo tương tự qua socket trên localhost:8080-8083
java -cp "target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" com.namnv.AppRaftSocket
```

Cả hai demo xoá thư mục `data/` ở thư mục hiện tại khi khởi động, rồi chạy tới khi bạn tắt.

## Dùng như một thư viện

### Khởi động một node

Mỗi node cần: một id, danh sách thành viên ban đầu, ba thư mục lưu trữ (có thể trùng nhau), một state machine
và một `RpcProcessor` để gửi RPC tới node khác.

```java
List<String> peers = List.of("localhost:8080", "localhost:8081", "localhost:8082");
RpcProcessor rpc = new SocketRpcClient(1000);          // timeout 1 giây cho mỗi RPC

NodeOptions options = NodeOptions.builder()
        .raftConfig(RaftConfig.builder().self("localhost:8080").peers(peers).build())
        .raftMetaUri("data/n1")                        // term, phiếu bầu
        .logUri("data/n1")                             // log
        .snapshotUri("data/n1")                        // snapshot
        .electionTimeoutMinMs(300)
        .electionTimeoutMaxMs(500)
        .heartbeatIntervalMs(100)
        .stateMachine(new ListStateMachine())
        .build();

RaftNode node = new RaftNode(options, rpc);
new SocketRpcServer(8080, node).start();               // nhận RPC từ node khác
node.start();
```

Với transport socket, id của node chính là địa chỉ `host:port` mà node khác dùng để gọi nó.
`RaftNode` không tự mở cổng: bạn tạo `SocketRpcServer` và trỏ nó vào node.

### Ghi lệnh

Chỉ leader nhận lệnh. `node.getState()` cho biết vai trò hiện tại, `node.getLeaderId()` cho biết leader mà node đang biết.

```java
// không chống trùng: nếu bạn gửi lại sau khi không nhận được kết quả, lệnh có thể được apply hai lần
CompletableFuture<Boolean> done = node.appendClientCommand("set x=1".getBytes());

// có chống trùng: mỗi client một clientId cố định, mỗi lệnh mới một sequence lớn hơn lệnh trước
boolean ok = node.appendClientCommand("client-7", 42, "set x=1".getBytes()).get();
```

Future trả về `true` khi lệnh đã được commit và apply trên leader. `false` nghĩa là **không biết kết quả**:
node không phải leader, mất quyền giữa chừng, hoặc quá 5 giây chưa commit. Lệnh đó vẫn có thể được commit sau này.
Với bản có `clientId`, cách xử lý đúng là gửi lại y nguyên `(clientId, sequence)` tới leader hiện tại cho tới khi nhận `true`;
lệnh được apply nhiều nhất một lần dù gửi bao nhiêu lần. Client phải chờ lệnh trước có kết quả rồi mới gửi sequence kế tiếp.

### Đọc nhất quán

Đọc thẳng từ state machine của một node có thể trả về dữ liệu cũ (node đó có thể là follower tụt lại, hoặc leader vừa mất quyền
mà chưa biết). `read` đảm bảo kết quả chứa mọi lệnh đã được xác nhận trước khi nó được gọi:

```java
ListStateMachine machine = ...;                        // state machine của chính node này
List<String> data = node.read(machine::getStore).get();
```

Hàm truyền vào chạy khi node đang giữ lock, nên cần nhanh và không được gọi ngược vào node.
Future thất bại với `NotLeaderException` (kèm `getLeaderId()` để thử lại) nếu node không phải leader hoặc mất quyền trong lúc chờ.

### Snapshot

```java
node.createSnapshot();
```

Hàm trả về ngay. State machine ghi dữ liệu của nó, sau đó snapshot được lưu và phần log đã nằm trong snapshot bị xoá, đều ở thread nền.
Raft Lite không tự tạo snapshot: bạn gọi theo lịch hoặc theo kích thước log.

### Thêm và gỡ node

Gọi trên leader. Mỗi lần chỉ một thay đổi; lời gọi trả về `false` nếu node không phải leader hoặc thay đổi trước chưa xong.

```java
// node mới khởi động với danh sách peers rỗng, rồi leader thêm nó vào
RaftNode newNode = new RaftNode(NodeOptions.builder()
        .raftConfig(RaftConfig.builder().self("localhost:8083").build())
        /* ... các tuỳ chọn khác ... */
        .build(), rpc);
newNode.start();
leader.onJoinPeerCluster("localhost:8083");

leader.onLeavePeerCluster("localhost:8081");           // gỡ một node, kể cả chính leader
```

Thay đổi hoàn tất khi `node.getConf()` không còn ở trạng thái joint và chứa đúng danh sách mới.
Leader gỡ chính mình sẽ trao quyền cho một follower rồi tự tắt. Node bị gỡ tự tắt khi biết chắc mình đã rời cluster.

### Tắt node

```java
node.shutdown();
```

## Tuỳ chọn của node

`NodeOptions`:

| Tuỳ chọn | Mặc định | Ý nghĩa |
|---|---|---|
| `raftConfig` | bắt buộc | `self` là id của node, `peers` là thành viên ban đầu (rỗng với node sẽ được thêm sau) |
| `stateMachine` | bắt buộc | Nơi lệnh đã commit được áp dụng |
| `raftMetaUri`, `logUri`, `snapshotUri` | bắt buộc | Thư mục lưu term/phiếu bầu, log và snapshot |
| `electionTimeoutMinMs`, `electionTimeoutMaxMs` | bắt buộc | Follower không nghe leader trong một khoảng ngẫu nhiên giữa hai giá trị này thì bắt đầu bầu cử. Leader mất liên lạc với đa số quá giá trị max thì step-down |
| `heartbeatIntervalMs` | bắt buộc | Nhịp leader gửi heartbeat; nên nhỏ hơn nhiều so với election timeout |
| `shutdownOnRemoved` | `true` | Node tự `shutdown()` khi bị gỡ khỏi cluster; `false` thì node chỉ đứng yên |
| `departingTimeoutMs` | `10000` | Leader cố gửi cấu hình cuối cho node vừa bị gỡ trong bao lâu trước khi bỏ cuộc |
| `runtime` | `null` | Nguồn thời gian, timer và thread ghi đĩa. `null` nghĩa là dùng thread và đồng hồ thật (`ThreadedRuntime`) |
| `diskFaults` | không làm gì | Điểm chèn lỗi ghi đĩa, chỉ dùng trong test |

Thời hạn chờ một lệnh hay một lần đọc được cố định là 5 giây.

## Viết state machine

Cài đặt interface `StateMachine` (xem Javadoc trong mã nguồn; `ListStateMachine` là ví dụ đầy đủ):

- **`onApply(node, entry)`** áp dụng một lệnh đã commit. Được gọi theo đúng thứ tự index khi node đang giữ lock, nên cần nhanh.
  Lệnh gửi lại bị trùng không bao giờ tới đây.
- **`onSnapshotSave(writer, done)`** lưu state vào thư mục `writer.getPath()`, đăng ký từng file bằng `writer.addFile(name)`,
  rồi gọi `done`. Hàm này được gọi khi node đang giữ lock: hãy chụp một bản sao của state ngay trong lời gọi,
  còn việc ghi file thì làm ở thread khác. Ghi đồng bộ vẫn đúng nhưng chặn node trong lúc ghi.
- **`onSnapshotLoad(reader)`** thay toàn bộ state bằng snapshot trong `reader.getPath()`.
  Trả về `false` hoặc ném exception đều được coi là state có thể đã hỏng: node dừng hẳn để lần khởi động sau dựng lại từ đĩa.

State machine phải tất định: cùng một chuỗi lệnh phải cho cùng một state trên mọi node.

## Kiến trúc

```
com.namnv
├── core
│   ├── RaftNode            toàn bộ logic Raft của một node
│   ├── RaftRuntime         đồng hồ, timer, thread ghi đĩa, nguồn ngẫu nhiên (tiêm được)
│   ├── ThreadedRuntime     runtime mặc định: thread và đồng hồ thật
│   ├── NodeState           LEADER / CANDIDATE / FOLLOWER
│   └── NotLeaderException
├── config                  NodeOptions, RaftConfig
├── entity                  LogEntry, ConfigurationEntry (cấu hình thành viên và phép tính quorum)
├── state
│   ├── PersistentState     term, phiếu bầu, commit index; sở hữu log và snapshot store
│   ├── VolatileState       commitIndex, lastApplied
│   └── LeaderState         nextIndex/matchIndex và các sổ theo dõi khác của leader
├── storage
│   ├── FileLogStorage      log chia segment
│   ├── SnapshotStore       thư mục snapshot, lưu atomic
│   ├── Checksum, FileUtil  CRC32, ghi file atomic, fsync thư mục
│   └── DiskFaultInjector   điểm chèn lỗi đĩa cho test
├── statemachine            interface StateMachine, SnapshotReader/Writer/Meta
├── timer                   ElectionTimer, HeartbeatTimer
├── rpc
│   ├── RaftServerService   các RPC một node phải xử lý
│   ├── client              RpcProcessor, InMemoryRpcClient, SocketRpcClient
│   ├── server              SocketRpcServer
│   └── model               request/response của từng RPC
├── ListStateMachine        state machine mẫu: danh sách các lệnh đã apply
└── AppRaftInMem, AppRaftSocket   hai demo
```

### Luồng xử lý và lock

Mỗi `RaftNode` có **một lock** bảo vệ toàn bộ state trong bộ nhớ. Mọi thứ đi vào node đều lấy lock này:
RPC handler, callback của RPC gửi đi, timer, và lời gọi của client.

Nguyên tắc: **không ghi đĩa khi đang giữ lock** trên các đường chạy thường xuyên. Việc ghi đĩa được tách thành hai bước:

1. Trong lock: cập nhật bộ nhớ và ghi nối vào file (chưa fsync).
2. Ngoài lock: fsync, rồi lấy lại lock để dùng kết quả.

Leader đưa bước 2 sang thread IO của `RaftRuntime`; follower làm bước 2 ngay trên thread đang xử lý RPC, giữa hai lần lấy lock.
Nhiều lệnh đến cùng lúc được gộp vào một lần fsync.

### `RaftRuntime`

`RaftNode` không tự tạo thread hay đọc đồng hồ hệ thống; nó xin mọi thứ qua `RaftRuntime`:
thời gian hiện tại, hẹn giờ, chạy việc ghi đĩa, và số ngẫu nhiên cho election timeout.
Nhờ vậy test có thể thay bằng một runtime chạy trên một thread với thời gian ảo, và cả cluster chạy tất định theo một seed.

## Cách hoạt động

### Bầu cử

1. Follower không nghe leader trong một election timeout thì hỏi **pre-vote**: "nếu tôi ứng cử, bạn có bầu không?".
   Nhờ bước này một node bị cô lập không làm tăng term của cả cluster khi nó quay lại.
2. Node nhận pre-vote từ chối nếu log của ứng viên cũ hơn log của nó, nếu nó đang là leader,
   hoặc nếu nó vừa nghe leader, vừa bỏ phiếu, hay đang tự ứng cử trong khoảng election timeout tối thiểu.
3. Đủ đa số pre-vote thì node thành candidate: tăng term, tự bầu, **ghi phiếu đó xuống đĩa**, rồi mới gửi RequestVote.
4. Node nhận RequestVote bầu nếu chưa bầu cho ai trong term đó và log của ứng viên mới ít nhất bằng log của nó.
   Phiếu bầu được ghi xuống đĩa trước khi câu trả lời được gửi đi.
5. Đủ đa số phiếu thì thành leader, ghi ngay một entry **no-op** của term mới và bắt đầu gửi heartbeat.
   No-op cần thiết vì leader chỉ được commit trực tiếp entry của term mình; entry của term cũ được commit theo nó.
6. Leader theo dõi lần trả lời gần nhất của từng follower. Nếu quá `electionTimeoutMaxMs` không còn liên lạc được với đa số,
   nó tự step-down thay vì tiếp tục tưởng mình là leader.

Election timer luôn được hẹn lại, nên một vòng bầu cử thất bại (chia phiếu, không ai trả lời) sẽ được thử lại.

### Nhân bản log

- Mỗi follower chỉ có **một request đang bay**. Khi nó trả lời mà leader còn entry chưa gửi, leader gửi tiếp ngay;
  entry đến trong lúc chờ được gom vào request sau.
- Follower kiểm tra entry đứng trước (`prevLogIndex`, `prevLogTerm`). Nếu không khớp, nó trả về một gợi ý để leader lùi nhanh
  thay vì lùi từng entry một.
- Follower **chỉ cắt log khi thật sự xung đột term**. Entry đã có sẵn (do request gửi trùng hoặc đến muộn) được bỏ qua;
  nhờ vậy một request cũ đến sau không xoá mất entry mới hơn.
- Follower chỉ trả lời thành công sau khi entry đã được fsync.
- Leader gửi entry cho follower **song song** với việc fsync log của chính nó, và chỉ tính mình vào quorum cho phần log đã nằm trên đĩa.
- Commit index là index lớn nhất mà đa số đã có (với joint config: đa số ở cả hai cấu hình), với điều kiện entry đó thuộc term hiện tại.
- Phần follower cần đã bị compact thì leader gửi snapshot thay cho entry.

### Các quy tắc về độ bền

Mọi thứ một node hứa với node khác phải nằm trên đĩa trước khi lời hứa được gửi đi:

| Lời hứa | Phải bền vững trước |
|---|---|
| "Tôi bầu cho bạn" | term và votedFor |
| RequestVote của candidate | phiếu tự bầu của nó |
| "Tôi đã có các entry này" | các entry đó |
| Leader tính mình vào quorum | phần log tương ứng của leader |
| Xác nhận với client | entry đã commit (đã bền vững trên đa số) |

Một ngoại lệ có chủ ý: khi leader hoặc candidate thấy term cao hơn trong một response, nó step-down ngay và ghi term mới sau.
Điều này an toàn vì phiếu bầu luôn được ghi xong trước khi có hiệu lực, nên node không thể bầu hai lần trong một term.

Commit index cũng được lưu, nhưng chỉ để lần khởi động sau apply lại nhanh hơn; mất nó không ảnh hưởng tính đúng đắn.

### Snapshot

**Tạo snapshot** (`createSnapshot`):

1. Trong lock: ghi nhận `lastApplied`, term của nó, cấu hình thành viên và bảng chống trùng tại thời điểm đó,
   rồi gọi `onSnapshotSave` với một thư mục `temp/`.
2. State machine ghi file của nó vào `temp/`.
3. Ở thread IO: tính CRC32 của từng file, ghi file meta, fsync, rồi **rename** `temp/` thành `snapshot_<index>/`.
   Snapshot cũ bị xoá sau đó.
4. Trong lock: xoá các segment log đã nằm trọn trong snapshot.

Vì bước 3 là một lần rename, snapshot hoặc có đủ hoặc không có gì, dù mất điện ở bất kỳ lúc nào.

**Cài snapshot** (follower nhận InstallSnapshot): ghi file vào `temp/` ngoài lock, cho state machine load từ `temp/`
(việc apply bị hoãn trong lúc đó), rồi mới rename, cuối cùng cập nhật log và index trong lock.
Load trước rồi mới lưu: nếu load hỏng thì snapshot và log cũ trên đĩa còn nguyên để lần khởi động sau dựng lại.

### Thay đổi thành viên

Dùng **joint consensus** hai bước:

1. Leader ghi cấu hình joint C(old, new). Trong giai đoạn này mọi quyết định (bầu cử, commit) cần đa số ở **cả hai** cấu hình.
2. Khi C(old, new) commit, leader ghi tiếp C(new). Khi C(new) commit, thay đổi hoàn tất.

Chi tiết:

- Cấu hình có hiệu lực ngay khi entry vào log, kể cả chưa commit (đúng theo Raft). Nếu entry đó bị cắt bỏ, cấu hình quay về bản trước.
- Cấu hình tại thời điểm snapshot được lưu cùng snapshot, vì entry cấu hình có thể đã bị compact khỏi log.
- Node mới khởi động với danh sách thành viên rỗng nên không tự bầu cử; nó nhận log (hoặc snapshot) từ leader sau khi được thêm.
- Leader tiếp tục gửi log cho node vừa bị gỡ tới khi node đó nhận được C(new) và biết C(new) đã commit, hoặc tới khi hết
  `departingTimeoutMs`. Leader mới đắc cử giữa chừng tự suy ra các node đang rời đi từ log.
- Leader gỡ chính mình: điều phối tới khi C(new) commit, gửi `TimeoutNow` lần lượt cho các follower (log đầy đủ nhất trước)
  để một node bầu cử ngay, rồi step-down.
- Node bị gỡ tự `shutdown()` khi biết C(new) đã commit, trừ khi `shutdownOnRemoved = false`.

### Đọc nhất quán (ReadIndex)

Khi `read` được gọi trên leader:

1. Leader ghi nhận **readIndex** = commit index hiện tại, nhưng không nhỏ hơn index của no-op mà nó ghi khi nhậm chức
   (trước khi no-op commit, leader mới chưa chắc commit index của mình đã đủ mới).
2. Leader gửi một vòng heartbeat mới và chờ **đa số trả lời các request được gửi sau thời điểm đó**.
   Response của heartbeat gửi trước đó không được tính: chúng chỉ chứng minh node là leader trước khi có yêu cầu đọc.
3. Leader chờ state machine apply tới readIndex.
4. Hàm đọc chạy trong lock và kết quả được trả về.

Cơ chế này không ghi gì vào log và không phụ thuộc đồng hồ, nên đúng cả khi đồng hồ các node chạy lệch nhau.
Nếu leader mất quyền trong lúc chờ, lần đọc thất bại với `NotLeaderException`.

### Chống ghi trùng

Một lệnh có `clientId` và `sequence` được ghi vào log kèm hai giá trị đó. Mỗi node giữ một bảng
`clientId → sequence lớn nhất đã apply`. Khi apply một entry có sequence không lớn hơn giá trị trong bảng,
node bỏ qua, không gọi state machine.

- Bảng chỉ phụ thuộc vào các entry đã apply, nên mọi node quyết định giống nhau.
- Bảng được lưu trong snapshot và gửi kèm InstallSnapshot, nên sống qua restart và compact log.
- Leader trả lời `true` ngay cho lệnh đã apply mà không ghi thêm vào log.
- Bảng không bao giờ được dọn: mỗi `clientId` từng xuất hiện chiếm một dòng mãi mãi.

## Dữ liệu trên đĩa

Mỗi node ghi vào ba thư mục cấu hình trong `NodeOptions` (có thể là cùng một thư mục):

```
data/n1/
├── raft_meta.json            term, votedFor, commit index
├── log_1.jsonl               segment log, tên file là index đầu tiên của segment
├── log_5001.jsonl
├── snapshot_5000/
│   ├── snapshot.data         file do state machine ghi
│   └── __raft_snapshot_meta.json
└── temp/                     snapshot đang ghi dở, bị xoá khi khởi động
```

- **`raft_meta.json`**: dòng đầu là CRC32 của phần còn lại, tiếp theo là JSON. Được thay bằng cách ghi file tạm, fsync, rename, fsync thư mục.
- **`log_<firstIndex>.jsonl`**: mỗi dòng là `<crc32> <entry dạng JSON>`. Entry mới được ghi nối vào segment cuối.
  Sau mỗi snapshot, entry mới sang một segment mới để segment cũ có thể bị xoá nguyên file ở lần snapshot kế tiếp.
- **`snapshot_<index>/`**: file của state machine và file meta. File meta chứa index và term cuối cùng của snapshot,
  cấu hình thành viên, bảng chống trùng, danh sách file và CRC32 của từng file.

### Khi khởi động lại

| Tình huống trên đĩa | Node làm gì |
|---|---|
| Dòng cuối của log hỏng hoặc ghi dở, sau nó không còn entry nguyên vẹn | Coi là đuôi ghi dở do crash (chưa từng được ack) và cắt bỏ |
| Một dòng hỏng ở giữa log, sau nó còn entry nguyên vẹn | Từ chối khởi động |
| `raft_meta.json` sai checksum | Từ chối khởi động |
| Snapshot mới nhất sai checksum hoặc thiếu file | Từ chối khởi động |
| Còn thư mục `temp/` | Xoá |
| Còn snapshot cũ hơn snapshot mới nhất | Xoá |
| Segment chứa toàn entry đã nằm trong snapshot | Bỏ qua và xoá |

Node từ chối khởi động thay vì chạy tiếp trên dữ liệu sai: term hoặc phiếu bầu bị đổi âm thầm có thể khiến node bầu hai lần,
và một entry bị đổi có thể khiến các node áp dụng những lệnh khác nhau.

## Vận hành

- **Node có dữ liệu hỏng** không tự hồi phục. Cách xử lý: gỡ nó khỏi cluster (`onLeavePeerCluster`), xoá thư mục dữ liệu của nó,
  khởi động lại với danh sách peers rỗng (nên dùng id mới), rồi thêm lại (`onJoinPeerCluster`).
  Đừng chỉ xoá dữ liệu rồi bật lại với id cũ khi nó còn là thành viên: node đó có thể đã bầu hoặc đã ack những thứ nó không còn nhớ.
- **Mất đa số** thì cluster ngừng nhận lệnh ghi và lệnh đọc nhất quán cho tới khi đủ đa số trở lại. Không có cơ chế ép đổi cấu hình.
- **Snapshot** cần được gọi định kỳ, nếu không log và bộ nhớ của node sẽ lớn mãi: toàn bộ entry chưa compact được giữ trong RAM.
- **Định dạng đĩa** đã đổi nhiều lần trong quá trình phát triển và không có cơ chế nâng cấp; dữ liệu của bản cũ không đọc lại được.
- **Log** dùng Log4j2; cấu hình ở `src/main/resources/log4j2.xml`.

## Transport

`RaftNode` gửi RPC qua interface `RpcProcessor` và nhận RPC qua interface `RaftServerService` (do chính nó cài đặt).
Có năm RPC: PreVote, RequestVote, AppendEntries, InstallSnapshot, TimeoutNow.

- **`InMemoryRpcClient`**: gọi thẳng handler của node đích trong cùng tiến trình. Có bảng `reachable` để giả lập chia cắt mạng. Dùng cho test và demo.
- **`SocketRpcClient` + `SocketRpcServer`**: TCP với Java serialization.
  - Mỗi node đích một kết nối dùng lại, các lời gọi tới cùng một node chạy tuần tự trên kết nối đó.
  - Kết nối lỗi thì đóng và nối lại ở lời gọi sau; có timeout cho cả lúc kết nối lẫn lúc chờ trả lời.
  - Server chỉ cho deserialize các class message của Raft; class khác bị từ chối trước khi code của nó chạy.
  - Không mã hoá và không xác thực. Snapshot được gửi nguyên khối trong một message.

Muốn dùng transport khác (gRPC, Netty...), cài đặt `RpcProcessor` cho phía gửi và gọi các hàm `handle...Request` của node ở phía nhận.
Một lời gọi thất bại chỉ cần làm future hoàn tất với exception; node tự gửi lại ở nhịp sau.

## Test

```bash
mvn test                                                    # tất cả

# mô phỏng tất định: nhiều seed hơn, hoặc chạy lại đúng một seed
mvn test -Dtest=RaftSimulationTest -Dsim.runs=500
mvn test -Dtest=RaftSimulationTest -Dsim.seed=123456

# fault injection chạy thread thật, lâu hơn
mvn test -Dtest=RaftChaosTest -Dchaos.runs=10 -Dchaos.seconds=15
```

| Bộ test | Kiểm tra gì |
|---|---|
| `RaftSimulationTest` | Cả cluster, mạng, đĩa, client và nemesis chạy trên một thread với thời gian ảo. Một seed luôn cho đúng một lịch sử, nên lỗi tìm ra thì chạy lại được y hệt. Có lượt 5 node, 7 node và một lượt dài nửa giờ ảo |
| `RaftChaosTest` | Cùng kịch bản nhưng với thread và đồng hồ thật, để bắt lỗi tranh chấp giữa các thread. Seed ở đây không tái hiện chắc chắn |
| `RaftClusterTest` | Test tất định cho từng hành vi và từng quy tắc an toàn, trên cluster in-memory |
| `SocketRpcTest` | Transport socket: từng loại RPC, lời gọi đồng thời, nối lại, timeout, lọc deserialization, cluster qua socket thật |
| `FileLogStorageTest` | Segment, cắt đầu/cắt đuôi, ghi dở, dữ liệu hỏng, segment sống lại sau mất điện |
| `SnapshotStoreTest` | Trạng thái đĩa ở từng thời điểm crash trong lúc ghi snapshot, snapshot hỏng |
| `ConfigurationEntryTest` | Phép tính quorum, kể cả cấu hình joint |

### Lỗi được chèn vào trong mô phỏng

- Mạng: mất request hoặc response, trễ, đảo thứ tự, request đến hai lần, gói tin kẹt rất lâu rồi mới tới, chia cắt mạng, cô lập đúng leader.
- Node: tắt bình thường, mất điện (mọi thứ chưa fsync biến mất), bật lại, đồng hồ chạy lệch từ chậm 25% tới nhanh 30%.
- Đĩa: thao tác ghi log, fsync, ghi meta và lưu snapshot thất bại ngẫu nhiên.
- Vận hành: tạo snapshot, gỡ và thêm thành viên, kể cả gỡ leader.
- Client: ghi liên tục và gửi lại khi không biết kết quả; đọc nhất quán liên tục từ bất kỳ node nào tự nhận là leader.

### Bất biến được kiểm tra

- Mỗi term có nhiều nhất một leader.
- Hai log có entry cùng index và cùng term thì giống hệt nhau từ đó trở về trước.
- Các node không bao giờ apply hai lệnh khác nhau ở cùng một vị trí.
- Lệnh đã xác nhận với client không bị mất; không lệnh nào bị apply hai lần, kể cả khi client gửi lại.
- Lệnh được xác nhận trước khi lệnh khác được gửi thì đứng trước lệnh đó.
- Mỗi lần đọc nhất quán thấy mọi lệnh được xác nhận trước khi nó bắt đầu, không thấy lệnh nào được gửi sau khi nó kết thúc,
  và các lần đọc nối tiếp nhau không đi lùi.
- Khi hết lỗi, cluster trở về đủ thành viên, nhận được lệnh ghi và mọi node hội tụ về cùng một state.

### Kiểm tra ngược

Để biết test có thật sự bắt được lỗi, từng quy tắc an toàn đã được cố tình phá trong mã nguồn rồi chạy lại test
(ví dụ: commit không cần quorum, bầu hai lần trong một term, trả lời trước khi fsync, đọc không xác nhận với đa số, bỏ chống trùng,
bỏ kiểm tra checksum). Mỗi lỗi như vậy đều làm ít nhất một test fail. Những lỗi mà mô phỏng ngẫu nhiên khó chạm tới đều có test tất định riêng.

## Giới hạn đã biết

- **Chưa được kiểm chứng ở mức production.** Chưa có kiểm thử kiểu Jepsen trên nhiều máy thật, chưa chạy liên tục nhiều giờ dưới tải.
- **Quy mô đã thử còn nhỏ:** 10.000 lệnh, cluster tối đa 7 node, nửa giờ thời gian ảo.
- **Toàn bộ log chưa compact nằm trong bộ nhớ**, và snapshot không tự kích hoạt.
- **Snapshot được gửi nguyên khối** trong một message, nên snapshot lớn sẽ nằm trọn trong RAM ở cả hai đầu.
- **Transport socket** không có TLS, không xác thực, dùng Java serialization.
- **Bảng chống trùng không được dọn**, và mỗi client chỉ được gửi một lệnh tại một thời điểm.
- **Đọc nhất quán chỉ phục vụ được ở leader** và mỗi lần đọc cần một vòng heartbeat; không có lease read hay đọc từ follower.
- **Thay đổi thành viên** mỗi lần chỉ một node; không có learner (node mới được tính vào quorum ngay khi cấu hình joint có hiệu lực,
  trước khi nó bắt kịp log).
- **State machine vẫn chạy `onApply` trong lock của node.**
- **Không có metrics, backpressure hay giới hạn kích thước batch.** Thời hạn chờ của client cố định 5 giây.
- **Mô phỏng mất điện** chỉ áp lên log và file meta; các thời điểm crash quanh snapshot được kiểm tra bằng test dựng lại trạng thái đĩa.
- **Một quy tắc lệch khỏi Raft sách giáo khoa** (term học được từ response được ghi đĩa trễ, xem
  [Các quy tắc về độ bền](#các-quy-tắc-về-độ-bền)) dựa trên lập luận và test, chưa có chứng minh hình thức.
