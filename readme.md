# Silk Road Raft

Silk Road Raft là một cài đặt gọn của thuật toán đồng thuận Raft bằng Java, viết để học và thử nghiệm.
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
| Nhân bản log | Gom nhiều entry vào một request có giới hạn kích thước, lùi nhanh khi log lệch, một request đang bay cho mỗi follower |
| Độ bền | Log chia segment có CRC32; fsync trước mọi lời hứa với node khác; mọi thao tác ghi đĩa nằm ngoài lock của node |
| Snapshot | Tạo bất đồng bộ (thủ công hoặc tự động theo số entry), lưu atomic, gửi cho follower tụt lại theo từng mẩu |
| Thành viên | Thêm, gỡ hoặc thay nhiều node một lần bằng joint consensus; node mới bắt kịp log trước khi được tính vào quorum; trao quyền leader; node bị gỡ tự tắt |
| Client | Ghi có chống trùng (`clientId` + `sequence`, gửi đồng thời được), đọc nhất quán bằng ReadIndex trên cả leader lẫn follower, backpressure |
| Transport | In-memory cho test; TCP tự viết với mã hoá nhị phân, nhiều lời gọi song song trên một kết nối, TLS xác thực hai chiều |
| Quan sát | `metrics()` trả về trạng thái và các bộ đếm của node |
| Kiểm thử | Mô phỏng tất định theo seed, fault injection chạy thread thật, test tất định cho từng quy tắc an toàn |

## Bắt đầu nhanh

Yêu cầu: JDK 21 trở lên và Maven.

```bash
mvn test                                  # toàn bộ test của mọi module, khoảng 1 phút

# classpath của module demo (gồm các module nó phụ thuộc, lấy thẳng từ thư mục target/classes của chúng)
mvn -q compile dependency:build-classpath -pl silkroad-raft-samples -am -Dmdep.includeScope=runtime -Dmdep.outputFile=target/samples-cp.txt
CP="silkroad-raft-samples/target/classes:$(cat target/samples-cp.txt)"

# demo 3 node trong một tiến trình: ghi lệnh, cô lập leader, nối lại, thêm node D, snapshot
java -cp "$CP" com.namnv.samples.AppRaftInMem

# demo tương tự qua socket trên localhost:8080-8083
java -cp "$CP" com.namnv.samples.AppRaftSocket
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
        .snapshotIntervalEntries(10_000)               // tự snapshot sau mỗi 10.000 entry
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

// có chống trùng: mỗi client một clientId cố định, sequence bắt đầu từ 1 và tăng liền nhau
boolean ok = node.appendClientCommand("client-7", 1, "set x=1".getBytes()).get();

// client sẽ không gửi lại lệnh nào nữa: cho cluster quên nó
node.closeClientSession("client-7");
```

Future trả về `true` khi lệnh đã được commit và apply trên leader. `false` nghĩa là **không biết kết quả**:
node không phải leader, mất quyền giữa chừng, quá `clientTimeoutMs` chưa commit, hoặc leader đang quá tải
(số lệnh chờ commit vượt `maxPendingCommands`). Lệnh đó vẫn có thể được commit sau này.

Với bản có `clientId`, cách xử lý đúng khi nhận `false` là gửi lại y nguyên `(clientId, sequence)` tới leader hiện tại
cho tới khi nhận `true`; lệnh được apply nhiều nhất một lần dù gửi bao nhiêu lần. Client có thể gửi nhiều lệnh cùng lúc
mà không cần chờ lệnh trước, miễn là không bỏ sót sequence nào.

**Ghi theo lô.** Khi client có sẵn nhiều lệnh, gom chúng vào một lô: cả lô đi trong một request và nằm trong **một** entry
của log, được commit và apply cùng nhau (lần lượt từng lệnh qua `onApply`, chung index), và được chống ghi trùng như một
lệnh theo `(clientId, sequence)`. Chi phí của Raft cho mỗi entry được chia cho mọi lệnh trong lô:

```java
node.appendClientBatch("client-7", 2, List.of(cmd1, cmd2, cmd3));   // trên node
raftClient.writeBatch(List.of(cmd1, cmd2, cmd3));                   // qua mạng
```

Đo với `ClusterBenchmark -Dbench.batch=N` (lệnh 128 byte, TPS tính theo số lệnh):

| | 1 lệnh mỗi lần | lô 10 | lô 100 |
|---|---|---|---|
| có fsync, 1 client | ~1.570 TPS | ~15.100 | ~128.000 (p50 0,8 ms) |
| có fsync, 32 client | ~24.000 | ~240.000 | ~734.000 |
| có fsync, 512 client | ~212.000 | ~1.130.000 | ~1.780.000 (p50 29 ms) |
| không fsync, 32 client | ~218.000 | ~1.600.000 | ~3.230.000 |

### Đọc nhất quán

Đọc thẳng từ state machine của một node có thể trả về dữ liệu cũ (node đó có thể là follower tụt lại, hoặc leader vừa mất quyền
mà chưa biết). `read` đảm bảo kết quả chứa mọi lệnh đã được xác nhận trước khi nó được gọi, và gọi được trên **bất kỳ node nào**:

```java
ListStateMachine machine = ...;                        // state machine của chính node này
List<String> data = node.read(machine::getStore).get();
```

Trên follower, node hỏi leader vị trí cần đọc rồi tự phục vụ từ dữ liệu của mình, nên việc đọc được chia tải ra cả cluster.
Hàm truyền vào chạy khi node đang giữ lock, nên cần nhanh và không được gọi ngược vào node.
Future thất bại với `NotLeaderException` nếu node không biết leader, không liên lạc được với leader, hoặc leader mất quyền trong lúc chờ.

### Snapshot

```java
node.createSnapshot();
```

Hàm trả về ngay. State machine ghi dữ liệu của nó, sau đó snapshot được lưu và phần log đã nằm trong snapshot bị xoá, đều ở thread nền.
Đặt `snapshotIntervalEntries` để node tự làm việc này mỗi khi có đủ số entry đã apply mà chưa compact.

### Thay đổi thành viên

Gọi trên leader. Mỗi lần chỉ một thay đổi; lời gọi trả về `false` nếu node không phải leader, danh sách không đổi,
hoặc thay đổi trước chưa xong.

```java
// node mới khởi động với danh sách peers rỗng, rồi leader thêm nó vào
RaftNode newNode = new RaftNode(NodeOptions.builder()
        .raftConfig(RaftConfig.builder().self("localhost:8083").build())
        /* ... các tuỳ chọn khác ... */
        .build(), rpc);
newNode.start();
leader.onJoinPeerCluster("localhost:8083");

leader.onLeavePeerCluster("localhost:8081");           // gỡ một node, kể cả chính leader

// thêm và gỡ nhiều node trong một lần
leader.changePeers(List.of("localhost:8080", "localhost:8083", "localhost:8084"));
```

Thay đổi hoàn tất khi `node.getConf()` không còn ở trạng thái joint và chứa đúng danh sách mới.
Node mới phải bắt kịp log trong `catchUpTimeoutMs`; nếu không, yêu cầu bị huỷ và cấu hình giữ nguyên.
Leader gỡ chính mình sẽ trao quyền cho một follower rồi tự tắt. Node bị gỡ tự tắt khi biết chắc mình đã rời cluster.

### Client qua mạng

Các lời gọi ở trên chạy trong cùng tiến trình với node. Để ứng dụng ở tiến trình hoặc máy khác dùng được cluster,
mở cổng cho client trên mỗi node rồi dùng `RaftClient`:

```java
// phía node: cùng cổng với RPC giữa các node; hàm thứ hai trả lời câu hỏi đọc từ state machine của node này
var clientService = new RaftClientService(node, query -> answerFromStateMachine(query));
new SocketRpcServer(8080, node, null, clientService).start();

// phía ứng dụng
RaftClient client = new RaftClient(List.of("localhost:8080", "localhost:8081", "localhost:8082"),
        "client-7", 1000, 30_000);          // timeout mỗi lần gửi, tổng thời gian chờ tối đa
client.write("set x=1".getBytes()).get();    // tự tìm leader, tự gửi lại, mỗi lệnh được apply nhiều nhất một lần
byte[] answer = client.read(query).get();    // đọc nhất quán
byte[] local = client.readFrom("localhost:8081", query).get();   // đọc nhất quán từ đúng một node, ví dụ một follower
```

`RaftClient` tự gán sequence cho từng lệnh, chuyển sang node khác khi leader đổi hoặc không trả lời, và gửi lại với cùng
`(clientId, sequence)`. Một `clientId` chỉ nên được dùng bởi một `RaftClient` tại một thời điểm.

### Theo dõi

```java
RaftMetrics m = node.metrics();
// m.state(), m.term(), m.leaderId(), m.commitIndex(), m.lastApplied(), m.firstLogIndex(), m.lastLogIndex(),
// m.pendingCommands(), m.pendingReads(), m.electionsStarted(), m.timesElectedLeader(), m.commandsAccepted(),
// m.commandsRejected(), m.duplicateCommands(), m.readsServed(), m.snapshotsCreated(), m.snapshotsInstalled()
```

Các bộ đếm tính từ lúc node khởi động. Silk Road Raft không tự xuất metrics ra hệ thống nào; bạn đọc và đẩy đi theo cách của mình.

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
| `clientTimeoutMs` | `5000` | Một lệnh hoặc một lần đọc chờ tối đa bao lâu trước khi nhận kết quả "không rõ" |
| `maxInflightAppends` | `32` | Số AppendEntries gửi liên tiếp cho một follower mà chưa có câu trả lời (xem [Pipelining](#pipelining)). `1` = tắt |
| `maxPendingCommands` | `100000` | Leader từ chối lệnh mới khi số lệnh đang chờ commit vượt mức này |
| `maxEntriesPerRequest` | `1024` | Số entry tối đa trong một AppendEntries |
| `snapshotIntervalEntries` | `0` (tắt) | Tự tạo snapshot khi số entry đã apply mà chưa compact đạt mức này |
| `snapshotChunkBytes` | `1048576` | Kích thước tối đa của một mẩu snapshot gửi cho follower |
| `commitIndexFlushIntervalMs` | `1000` | Commit index được ghi xuống đĩa nhiều nhất mỗi khoảng này một lần; nó chỉ giúp khởi động lại nhanh hơn. `0` là ghi sau mỗi lần commit |
| `logSync` | `true` | `false`: không fsync log, coi entry là bền vững ngay khi đã ghi vào file như Aeron Cluster. Nhanh hơn nhiều trên đĩa chậm, nhưng một node mất điện có thể quên entry đã hứa và làm mất lệnh đã commit. Chỉ dùng khi các node có nguồn điện độc lập |
| `logPreallocate` | `true` | Cấp phát sẵn file segment kế tiếp ở thread nền. fsync trên file cấp phát sẵn nhanh hơn vài lần so với file thưa; đổi lại mỗi segment được ghi hai lần, nên log chỉ làm việc này khi nó lớn chậm (dưới khoảng 40 MB/giây) và `logSync` bật |
| `logSegmentBytes` | `67108864` | Kích thước mỗi file segment của log; một entry không được lớn hơn một segment |
| `logCacheEntries` | `16384` | Số entry mới nhất của log được giữ trong bộ nhớ; entry cũ hơn được đọc lại từ đĩa khi cần (follower tụt xa, khởi động lại) |
| `catchUpTimeoutMs` | `30000` | Node mới phải bắt kịp log trong thời gian này thì mới được đưa vào cấu hình |
| `shutdownOnRemoved` | `true` | Node tự `shutdown()` khi bị gỡ khỏi cluster; `false` thì node chỉ đứng yên |
| `departingTimeoutMs` | `10000` | Leader cố gửi cấu hình cuối cho node vừa bị gỡ trong bao lâu trước khi bỏ cuộc |
| `runtime` | `null` | Nguồn thời gian, timer, thread của node và thread ghi đĩa. `null` nghĩa là dùng thread và đồng hồ thật (`ThreadedRuntime`) |
| `diskFaults` | không làm gì | Điểm chèn lỗi ghi đĩa, chỉ dùng trong test |

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

### Kho KV mẫu: LMDB và RocksDB

`com.namnv.kv.LmdbKvStateMachine` là một state machine key-value thật, chạy trên LMDB (thư viện `lmdbjava`, khai báo
`optional` trong `pom.xml`: muốn dùng thì tự thêm dependency). LMDB và bbolt (kho của etcd) cùng thiết kế B+tree
copy-on-write trên mmap, và lớp này làm theo cách backend của etcd:

- `onApply` chỉ ghi vào bộ đệm trong bộ nhớ, nên thread của node không bao giờ chờ đĩa.
- Một thread backend ghi bộ đệm vào LMDB theo lô, mỗi 100 ms hoặc mỗi 10.000 lệnh, một transaction mỗi lô, có fsync
  (giống batch-interval và batch-limit mặc định của etcd).
- `get` xem bộ đệm trước rồi mới tới LMDB, nên luôn thấy mọi lệnh đã apply.
- Snapshot chép toàn bộ kho ra một file ở thread riêng, từ một transaction đọc của LMDB cộng các lô chưa ghi tại đúng
  thời điểm chụp.
- Độ bền nằm ở Raft log: kho bị xoá mỗi lần khởi động rồi dựng lại từ snapshot và phần log sau nó.

`com.namnv.kv.RocksDbKvStateMachine` dùng chung khung đó (`BufferedKvStateMachine`) nhưng chạy trên RocksDB (LSM-tree,
`rocksdbjni`, cũng `optional`). Mỗi lô là một `WriteBatch`. `sync = false` (mặc định) tắt hẳn WAL của RocksDB, vì Raft log
đã là WAL. Snapshot là một checkpoint, tạo đúng lúc mọi lệnh tới thời điểm chụp đã vào kho: các file SST chỉ được
hard link sang thư mục snapshot, không chép, nên snapshot rẻ hơn hẳn bản LMDB (vốn chép toàn bộ kho).

Đo với `ClusterBenchmark -Dbench.kv=...` (1 triệu key, value 128 byte, xem [Đo hiệu năng](#đo-hiệu-năng)):

| | LMDB | RocksDB |
|---|---|---|
| ghi không fsync, tối đa | ~466.000 TPS, max 18–40 ms | ~530.000 TPS, max 6–16 ms |
| ghi có fsync, tối đa | ~230.000–284.000 TPS, max tới ~240 ms | ~260.000–289.000 TPS, max tới ~76 ms |
| đọc nhất quán, tối đa | ~700.000–770.000/s | ~600.000–680.000/s |
| dung lượng kho | ~267 MB | ~83 MB (nén LZ4) |

Phần lớn chênh lệch về ghi và độ trễ đuôi đến từ snapshot: bản LMDB chép khoảng 140 MB sau mỗi 1 triệu lệnh.

```java
var kv = new LmdbKvStateMachine(Path.of("data/kv"));
var node = new RaftNode(NodeOptions.builder().stateMachine(kv) /* ... */ .build(), rpc);
var clientService = new RaftClientService(node, kv::query);
// phía client
client.write(KvCommands.put(key, value));
byte[] value = KvCommands.value(client.read(KvCommands.get(key)).get());
```

### Kết quả của lệnh

State machine trả được kết quả cho client của từng lệnh (ví dụ "không đủ tiền") bằng `onApplyWithResult`; kết quả
phải chỉ phụ thuộc vào state và lệnh để mọi node tính ra giống nhau, và chỉ kết quả trên leader đi về client:

```java
byte[] result = node.submit(null, 0, command, false).get();         // trên node
byte[] result = raftClient.submit(command).get();                   // qua mạng
byte[] results = raftClient.submitBatch(commands).get();            // mỗi lệnh một kết quả, đọc bằng CommandBatch.forEach
```

Future thất bại với `UnknownOutcomeException` khi không biết lệnh đã được apply hay chưa. Lệnh bị chống trùng theo
`(clientId, sequence)` không được apply lại và kết quả lần đầu không còn, nên khi cần kết quả hãy chống trùng bằng id
nghiệp vụ của chính lệnh và để `clientId` null (như sổ cái bên dưới).

Nếu state machine ném lỗi khi apply, node ngừng apply và tự tắt thay vì chạy tiếp với một state có thể đã đổi dở và khác
các bản sao khác (fail-stop, như etcd). Khởi động lại sẽ dựng state từ snapshot và log.

### Sổ cái (`silkroad-raft-ledger`)

Một dịch vụ sổ cái kép dựng trên thư viện, theo cách của Binance Ledger và TigerBeetle: tài khoản (`LedgerAccount`) thuộc
một sổ (đơn vị tiền), có cờ "không được âm" cho tài khoản khách hàng; chuyển tiền (`LedgerTransfer`) cộng cùng một số
tiền vào tổng nợ của tài khoản nợ và tổng có của tài khoản có, nên tổng nợ của cả sổ luôn bằng tổng có.

- Mọi kiểm tra chạy trong state machine một thread, không khoá: tài khoản tồn tại và khác nhau, cùng sổ, số tiền dương,
  không tràn số, không vượt số dư. Tài khoản "nóng" không làm chậm hệ thống như khoá dòng của cơ sở dữ liệu quan hệ.
- Id của giao dịch do client chọn: gửi lại một giao dịch đã ghi trả về `EXISTS`, không ghi hai lần. Mỗi giao dịch có
  kết quả riêng (`LedgerResult`) gửi về client.
- Tài khoản nằm trong bộ nhớ dưới dạng mảng số nguyên thuỷ. Lịch sử giao dịch (`TransferStore`) có giới hạn bộ nhớ:
  giao dịch mới nằm trong một đoạn cố định (mặc định 1 triệu giao dịch); đoạn đầy được một thread nền ghi xuống RocksDB
  (một `WriteBatch`, không WAL vì Raft log đã giữ độ bền) rồi dùng lại. Chống trùng không đọc RocksDB cho mỗi giao dịch:
  một bloom filter của mọi id (khoảng 1,5 byte mỗi giao dịch) trả lời "chắc chắn chưa có"; chỉ khi nó nói "có thể có"
  (khoảng 1%) mới đọc RocksDB, nên kết quả luôn chính xác và giống nhau trên mọi node. RocksDB ghi không kịp thì thread của
  node chờ khi đã có 4 đoạn chưa ghi, thay vì dồn bộ nhớ.
- Snapshot: các mảng tài khoản, checkpoint của RocksDB (hard link tới các SST, tạo đúng lúc mọi giao dịch trước thời điểm
  chụp đã xuống RocksDB) và bloom filter; phần ghi file chạy ở thread riêng.

```java
var client = new LedgerClient(List.of("host1:9001", "host2:9001", "host3:9001"), 10_000);
client.createAccounts(List.of(new LedgerAccount(1, 840, 0),
        new LedgerAccount(2, 840, LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS))).get();
List<LedgerResult> results = client.transfers(List.of(new LedgerTransfer(100, 1, 2, 500, 840))).get();
LedgerTotals totals = client.totals().get();          // totals.balanced() luôn đúng
```

Một node: `java ... com.namnv.ledger.node.LedgerNode host:port host1:port1,host2:port2,host3:port3 thư-mục-dữ-liệu`.

#### Learner và dòng sự kiện (CQRS)

Hệ thống khác (app, báo cáo, thông báo, đối soát, chống gian lận) không đọc thẳng từ sổ cái: chúng nghe một dòng sự kiện
và tự dựng mô hình truy vấn của riêng mình. Ở đây dòng đó được phát từ một **learner**: một node nhận log từ leader như
mọi follower nhưng không nằm trong cấu hình, nên không bỏ phiếu, không được tính vào quorum và không bao giờ thành leader.
Learner chậm hay chết không làm chậm việc ghi.

- `RaftConfig.learners`: danh sách learner cố định, khai báo giống nhau trên mọi node.
- Mỗi tài khoản mới hoặc giao dịch đã ghi sinh một `LedgerEvent` có vị trí `(index, position)`: index của entry trong Raft
  log và thứ tự của lệnh trong lô. Vị trí này giống nhau trên mọi node. Lệnh bị từ chối và `EXISTS` không sinh sự kiện.
- `EventPublisher` đưa sự kiện từ thread của node sang thread riêng theo lô, qua một hàng đợi có giới hạn (sink chậm thì
  node chờ, không phình bộ nhớ). `EventSink` là nơi nhận, ví dụ Kafka hay một bảng; `JsonLinesEventSink` là bản mẫu ghi
  file JSON lines, fsync mỗi lô.
- **Đúng một lần:** khởi động lại, node dựng lại sổ cái từ snapshot và log rồi sinh lại các sự kiện sau snapshot với đúng
  vị trí cũ. Sink bỏ qua những gì nó đã có (so vị trí với sự kiện cuối cùng của nó), và dòng ghi dở bị cắt khi mở lại.
  Snapshot chỉ được chụp khi mọi sự kiện trước nó đã nằm trong sink, nên không bao giờ có lỗ hổng.

```bash
# trên mọi node: -Dledger.learners=host4:9001 ; riêng learner thêm nơi phát sự kiện
java -Dledger.learners=host4:9001 -Dledger.eventsFile=/data/events.jsonl ... com.namnv.ledger.node.LedgerNode \
     host4:9001 host1:9001,host2:9001,host3:9001 /data/ledger
```

#### Cổng HTTP (`LedgerGateway`)

Hệ thống thật gửi lệnh chuyển tiền qua HTTP/JSON (hoặc gRPC) tới một dịch vụ cổng, không nói giao thức của cụm. Cổng nhận
request lẻ, đổi thành lệnh nhị phân gửi tới leader, và trả mã HTTP theo kết quả:

```
POST /transfers   {"debitAccountId":2,"creditAccountId":3,"amount":100,"ledger":840}   Idempotency-Key: order-8812
POST /accounts    {"id":2,"ledger":840,"flags":1}
GET  /accounts/{id}   GET /transfers/{id}   GET /totals   GET /stats
```

| Mã | Ý nghĩa |
|---|---|
| 201 | ghi mới |
| 200 | đã có từ lần gửi trước (`EXISTS`): coi như thành công |
| 409 | id / key đã dùng cho một giao dịch khác |
| 422 | bị từ chối theo quy tắc của sổ: không đủ tiền, sai sổ, tài khoản không tồn tại... (body có `result`) |
| 400 / 404 / 405 | request sai / không có / sai method |
| 503 | không rõ kết quả (đổi leader, quá hạn, quá tải): gửi lại với cùng `Idempotency-Key` |

- **Chống trùng:** id của giao dịch là `id` trong body, hoặc nếu không có thì là 63 bit đầu SHA-256 của `Idempotency-Key`.
  Client gửi lại sau timeout hay 503 không bao giờ làm ghi hai lần: lần sau nhận 200. Cổng không cần lưu key ở đâu cả,
  chính sổ cái là nơi chống trùng.
- **Tự gom lô (`TransferBatcher`)**, kiểu Nagle không chờ theo thời gian: dưới `maxInflight` lô đang bay thì gửi ngay
  (tải thấp: độ trễ như gửi lẻ); đủ rồi thì request mới xếp hàng và đi chung lô kế tiếp, ngay khi một lô trả lời (tải cao:
  lô tự lớn, tới `maxBatch`). Mỗi giao dịch trong lô vẫn được kiểm tra và trả kết quả riêng.
- Chạy trên Netty (epoll trên Linux), `-Dgateway.eventLoops` event loop (mặc định 1/4 số CPU). Không thread nào đứng chờ
  cụm Raft: request được đọc trên event loop (body bằng parser streaming của Jackson, không dựng object), trả lời được ghi
  khi kết quả về. Nhiều request liền trên một kết nối (HTTP pipelining) nhận trả lời đúng thứ tự; body tối đa 64 KB (413).

```bash
java -Dgateway.maxBatch=1000 -Dgateway.maxInflight=4 ... com.namnv.ledger.gateway.LedgerGateway 8080 host1:9001,host2:9001,host3:9001
# -Dgateway.batch=false: không gom, mỗi request một lệnh Raft
```

`LedgerBenchmark` chạy 3 node thành 3 tiến trình, tạo 100.000 tài khoản khách hàng không được âm, nạp tiền cho từng tài
khoản, rồi cho nhiều client chuyển tiền theo lô 100 giao dịch (kịch bản "hot": một nửa số giao dịch đi vào hoặc ra khỏi
cùng một tài khoản). Cuối lượt nó kiểm tra trên cả ba node: tổng nợ bằng tổng có, ba bản sao giống hệt nhau, không khách
hàng nào âm, tổng số dư khách hàng đúng bằng số đã nạp. Trên một máy (i5-13500, NVMe, có fsync, qua loopback):

| Kịch bản | Giao dịch/giây | p50 / p99 / max của một lô |
|---|---|---|
| đều, 1 client | ~116.000 | 0,89 / 1,2 / 6 ms |
| đều, 32 client | ~1.050.000 | 2,8 / 7,2 / 17 ms |
| đều, 256 client | ~1.310.000 | 17 / 42 / 47 ms |
| tài khoản nóng, 32 client | ~780.000 | 3,4 / 17 / 49 ms |
| tài khoản nóng, 256 client | ~1.110.000 | 21 / 42 / 51 ms |

Mỗi node chạy với heap 2 GB. Một lượt 30 giây ghi hơn 35 triệu giao dịch (khoảng 1,13 triệu mỗi giây) mà bộ nhớ không
tăng theo; lịch sử trên đĩa khoảng 20 byte mỗi giao dịch nhờ nén.

Để so sánh, Binance công bố sổ cái của họ đạt hơn 10.000 giao dịch/giây, đa số trong 10 ms, trên 4 máy AWS
(M6i.4xlarge, ổ EBS gp3). Điều kiện khác hẳn: nhiều máy qua mạng thật, ổ đĩa mạng, nghiệp vụ đầy đủ; số ở đây là một máy.

```bash
mvn -q compile dependency:build-classpath -pl silkroad-raft-ledger -am -Dmdep.includeScope=runtime -Dmdep.outputFile=target/ledger-cp.txt
java -cp "silkroad-raft-ledger/target/classes:$(cat target/ledger-cp.txt)" com.namnv.ledger.bench.LedgerBenchmark
```

Đo bằng [wrk](https://github.com/wg/wrk) (client C, gần như không tốn CPU so với cổng): `bench/wrk/run.sh` là một file
làm hết: tìm wrk (không có thì tự build), build project, dựng cụm 3 node và hai cổng (từng lệnh / gom lô, bằng
`GatewayBenchmark -Dgateway.modes=serve`), chạy wrk với script Lua nhúng sẵn (mỗi request là một `POST /transfers` với
`Idempotency-Key` mới) rồi in bảng. 4 thread wrk, `-Dledger.snapshotInterval=2000000`, cổng 5 event loop.

Không fsync (thấy rõ chi phí của riêng tầng HTTP), so với bản cổng trước đây chạy trên `HttpServer` của JDK:

| Cổng | Kết nối | Netty: req/s | p50 / p99 | `HttpServer` JDK: req/s | p50 / p99 |
|---|---|---|---|---|---|
| từng lệnh | 1 | ~19.000 | 44 / 140 µs | ~16.500 | 51 / 149 µs |
| từng lệnh | 64 | ~250.000 | 0,20 / 1,6 ms | ~171.000 | 0,28 / 2,3 ms |
| từng lệnh | 512 | ~220.000–304.000 | 1,2 / 5,0 ms | ~193.000 | 2,1 / 6,6 ms |
| từng lệnh | 2048 | ~273.000 | 4,6 / 12 ms | | |
| gom lô | 1 | ~18.200 | 46 / 159 µs | ~16.000 | 53 / 143 µs |
| gom lô | 64 | ~223.000 | 0,22 / 1,3 ms | ~165.000 | 0,28 / 2,0 ms |
| gom lô | 512 | ~288.000 | 1,1 / 4,1 ms | ~196.000 | 2,1 / 19 ms |

Có fsync (Netty; các lượt rơi vào pha fsync chậm của ổ đĩa, xem dưới, đã bỏ):

| Cổng | Kết nối | req/s | p50 / p99 | Lô TB |
|---|---|---|---|---|
| từng lệnh | 1 | ~1.600 | 0,54 / 1,1 ms | 1 |
| từng lệnh | 2048 | ~238.000 | 6,5 / 32 ms | 1 |
| gom lô | 512 | ~226.000 | 2,1 / 6,0 ms | 75 |
| gom lô | 2048 | ~291.000 | 6,0 / 12 ms | 143 |

- Qua HTTP, một giao dịch có fsync mất khoảng 0,55 ms đầu-cuối, gần như toàn bộ là fsync; HTTP + JSON thêm khoảng 25–45 µs.
- Cổng Netty tốn khoảng 18 µs CPU mỗi request (3,9 core ở 220k req/s); bản trên `HttpServer` của JDK tốn khoảng 46 µs
  (8,8 core ở 190k) và là trần của cả đường HTTP. Giờ trần nằm ở các chỗ chỉ có một thread: thread NIO của `LedgerClient`
  trong cổng (mọi lệnh tới cụm và mọi kết quả trở về) và thread xử lý của leader. Nhiều cổng song song (chúng không giữ
  trạng thái gì, chống trùng nằm ở sổ cái) chia được phần đầu.
- Gom lô ở cổng không làm nhanh hơn khi không fsync: Raft đã gom các lệnh đồng thời vào cùng một lần fsync và một lần gửi.
  Có fsync và tải cao thì lô lớn (75–143) giữ p99 thấp hơn. Ở tải vừa, `maxInflight` nhỏ làm request chờ lô trước trả
  lời: với 64 kết nối có fsync, `maxInflight=4` cho khoảng 33k req/s, `maxInflight=64` khoảng 44k (ngang không gom).
- Ổ NVMe của máy đo (KIOXIA BG6, không DRAM) có những pha fsync chậm gấp 10 lần (p50 5 ms, max 170 ms, đo bằng một tiến
  trình fsync độc lập) sau các lượt ghi nặng: các lượt có fsync dao động mạnh, nên so sánh cần chạy vài lần.
- Snapshot đếm theo số entry: khi mỗi request là một entry, `snapshotInterval=100000` nghĩa là khoảng mỗi 100k giao dịch
  lại chụp một lần (58 MB mỗi node, phần lớn là bloom filter cỡ `expectedTransfers`), đủ để đĩa bận và làm chậm fsync
  của các lượt đo sau. Đặt khoảng snapshot theo lượng giao dịch thực tế mỗi entry.

```bash
bench/wrk/run.sh                       # 1 64 512 kết nối, có fsync
FSYNC=false DURATION=5 bench/wrk/run.sh 64 512 2048
# các biến khác ở đầu file: WRK, THREADS, WARMUP, MODES, MAX_BATCH, MAX_INFLIGHT, EVENT_LOOPS, WRK_CPUS (taskset), KEEP
```

`GatewayBenchmark` (không cần wrk) đo cùng cụm theo ba cách — nhị phân (`LedgerClient.transfer`), HTTP từng lệnh, HTTP gom
lô — với một client HTTP/1.1 tối giản trong Java, và in thêm CPU của client / cổng / node:

```bash
java -cp "silkroad-raft-ledger/target/classes:$(cat target/ledger-cp.txt)" com.namnv.ledger.bench.GatewayBenchmark
# -Dgateway.clients=1,64,512 -Dgateway.modes=binary,http,http-batch -Dledger.logSync=false
```

## Kiến trúc

### Module

Dự án chia thành các module Maven theo tầng, kiểu Aeron (`aeron-client`, `aeron-driver`, `aeron-cluster`...).
Phụ thuộc chỉ đi xuống, nên ứng dụng chỉ kéo về đúng phần mình dùng: một client chỉ cần `silkroad-raft-client`, một node
cần `silkroad-raft-cluster` cùng một transport.

| Module | Nội dung | Phụ thuộc |
|---|---|---|
| `silkroad-raft-agent` | `AgentLoop` (vòng làm việc kiểu agent), `Utf8Cache` | — |
| `silkroad-raft-log` | `LogEntry`, `ConfigurationEntry`, `CommandBatch`, log nhị phân (`BinaryLogStorage`, `LogOptions`), `SnapshotStore`, snapshot reader/writer | agent |
| `silkroad-raft-protocol` | request/response, `RpcCodec`, các interface `RaftServerService`, `ClientService`, `RpcProcessor`, `MessageTransport` | log |
| `silkroad-raft-transport` | `SocketRpcClient`/`SocketRpcServer`, `NioRpcClient`/`NioRpcServer`, `InMemoryRpcClient`, TLS | protocol, agent |
| `silkroad-raft-client` | `RaftClient` | transport |
| `silkroad-raft-cluster` | đồng thuận: `RaftNode`, `RaftRuntime`, `NodeOptions`, state bền vững, timer, `StateMachine` | protocol, log, agent |
| `silkroad-raft-kv` | `LmdbKvStateMachine`, `RocksDbKvStateMachine` | cluster |
| `silkroad-raft-ledger` | dịch vụ sổ cái kép: `Ledger` (state machine), `LedgerClient`, `LedgerNode`, cổng HTTP `LedgerGateway`, dòng sự kiện `EventPublisher`, `LedgerBenchmark`, `GatewayBenchmark` | cluster, transport, client |
| `silkroad-raft-samples` | `AppRaftInMem`, `AppRaftSocket` | cluster, transport |
| `silkroad-raft-benchmarks` | `ClusterBenchmark`, `RaftBenchmark`, `BenchNode`, cluster tham chiếu dùng SOFAJRaft | tất cả |
| `silkroad-raft-system-tests` | test đi qua mạng thật: cluster, transport và client cùng chạy | tất cả (test) |

Thư viện chỉ phụ thuộc `slf4j-api`; ứng dụng tự chọn backend log (demo và benchmark dùng Log4j2).

### Package

```
com.namnv
├── agent
│   └── AgentLoop           vòng làm việc kiểu agent của Aeron: poll socket, chạy việc của node, ghi theo đợt, idle strategy
├── core
│   ├── RaftNode            toàn bộ logic Raft của một node
│   ├── RaftRuntime         đồng hồ, timer, thread ghi đĩa, nguồn ngẫu nhiên (tiêm được)
│   ├── ThreadedRuntime     runtime mặc định: thread và đồng hồ thật
│   ├── RaftMetrics         ảnh chụp trạng thái và bộ đếm
│   ├── NodeState           LEADER / CANDIDATE / FOLLOWER
│   └── NotLeaderException
├── config                  NodeOptions, RaftConfig
├── entity
│   ├── LogEntry            lệnh, no-op, thay đổi cấu hình hoặc kết thúc phiên client
│   ├── ConfigurationEntry  cấu hình thành viên và phép tính quorum
│   └── ClientSession       các sequence của một client đã được apply
├── state
│   ├── PersistentState     term, phiếu bầu, commit index; sở hữu log và snapshot store
│   ├── VolatileState       commitIndex, lastApplied
│   └── LeaderState         nextIndex/matchIndex và các sổ theo dõi khác của leader
├── storage
│   ├── binary              BinaryLogStorage, LogOptions: log nhị phân, ghi theo khối (kiểu Aeron Archive)
│   ├── SnapshotStore       thư mục snapshot, lưu atomic
│   ├── Checksum, FileUtil  CRC32, ghi file atomic, fsync thư mục
│   └── DiskFaultInjector   điểm chèn lỗi đĩa cho test
├── statemachine            interface StateMachine, SnapshotReader/Writer/Meta
├── timer                   ElectionTimer, HeartbeatTimer
├── rpc                     (silkroad-raft-protocol)
│   ├── RaftServerService   các RPC một node phải xử lý; ClientService cho client bên ngoài
│   ├── RpcProcessor        phía gửi RPC của node; MessageTransport cho client
│   ├── RpcCodec            khung và mã hoá nhị phân
│   └── model               request/response của từng RPC
├── transport               (silkroad-raft-transport) SocketRpcClient, SocketRpcServer, InMemoryRpcClient, TlsContexts
│   └── nio                 NioRpcClient, NioRpcServer: transport chạy trên vòng của node
├── client                  (silkroad-raft-client) RaftClient
├── kv                      (silkroad-raft-kv) kho KV trên LMDB, RocksDB
├── ledger                  (silkroad-raft-ledger) dịch vụ sổ cái kép
│   ├── model               LedgerAccount, LedgerTransfer, LedgerBalance, LedgerTotals, LedgerResult
│   ├── codec               LedgerCodec: lệnh và kết quả dạng nhị phân, dùng chung giữa client và state machine
│   ├── state               Ledger (state machine) cùng phần nội bộ: TransferStore (lịch sử trên RocksDB),
│   │                       TransferSegment, IdFilter (bloom filter của id), LongIndex
│   ├── event               LedgerEvent, EventSink, EventPublisher, JsonLinesEventSink: dòng sự kiện ra ngoài
│   ├── client              LedgerClient
│   ├── node                LedgerNode: một node (Raft + Ledger + transport NIO), voter hoặc learner
│   ├── gateway             LedgerGateway (cổng HTTP trên Netty), TransferBatcher
│   └── bench               LedgerBenchmark, GatewayBenchmark, LocalCluster
├── util                    Utf8Cache
├── ListStateMachine        state machine mẫu: danh sách các lệnh đã apply
└── samples                 (silkroad-raft-samples) AppRaftInMem, AppRaftSocket
```

### Luồng xử lý: một thread cho mỗi node

Mỗi `RaftNode` có **một thread xử lý** (giống agent của Aeron). Mọi sự kiện của node đều được xếp vào một hàng và thread này
xử lý lần lượt: lệnh ghi và đọc của client, AppendEntries và ReadIndex đến từ leader/follower, response của RPC gửi đi,
timer, và việc đĩa đã xong. Thread transport chỉ xếp sự kiện vào hàng rồi đọc tiếp socket, không bao giờ đứng chờ node.

Thread của node lấy cả hàng trong một lượt: nhiều lệnh client đến cùng lúc được ghi vào log rồi gửi cho follower trong
**một** AppendEntries. Hết việc, thread quay chờ thêm một lúc (`-Draft.agent.idle`, `-Draft.agent.spinMicros`; xem [Transport](#transport)) trước khi ngủ, vì
đánh thức một thread đang ngủ tốn hàng chục micro giây.

Lock của node vẫn còn, nhưng gần như không ai tranh: thread của node giữ nó trong mỗi lượt xử lý, còn các lời gọi hiếm
(đổi thành viên, tạo snapshot, RequestVote/PreVote/InstallSnapshot/TimeoutNow, `metrics()`) và test lấy nó để chen vào an toàn.

Nguyên tắc: **thread của node không chờ đĩa**. Việc ghi đĩa được tách thành hai bước:

1. Trên thread của node: cập nhật bộ nhớ và ghi nối vào file (chưa fsync).
2. Trên thread IO của `RaftRuntime`: fsync, rồi chuyển kết quả về thread của node như một sự kiện.

Cả leader lẫn follower gom mọi entry đến trong lúc đĩa đang bận vào một lần fsync. Follower chỉ trả lời AppendEntries
sau khi lần fsync phủ entry đó xong. Với `logSync = false` bước 2 không còn gì phải chờ nên được làm luôn trên thread của node.

Kết quả trả cho người gọi nằm ngoài lock: future của `appendClientCommand` và `read` được hoàn tất sau mỗi lượt xử lý,
lần lượt theo thứ tự commit. Callback gắn vào future vì thế gọi ngược vào node được, nhưng một callback chậm
vẫn làm kết quả của các lệnh sau nó đến trễ. Vì lệnh được ghi vào log trên thread của node, `appendClientCommand` trả về
trước khi entry nằm trong log.

### Cấp phát trên đường nóng

Những gì được tạo ra cho mỗi lệnh đều sẽ thành việc của GC, nên đường đi của một lệnh tránh tạo object khi không cần:

- Leader gửi cho follower đúng các khung mà log đã dựng lúc append (`LogStorage.Block`), ghi thẳng từng khung ra
  socket, không nối chúng thành một mảng mới cho mỗi lần gửi.
- Bộ đệm của kết nối NIO là direct buffer: socket đọc và ghi thẳng vào đó. Với heap buffer, JDK ngầm chép mọi byte
  qua một direct buffer tạm ở mỗi lần đọc/ghi.
- Transport NIO mã hoá message thẳng vào bộ đệm ghi của kết nối và giải mã ngay trong bộ đệm đọc
  (`RpcCodec.encodeBody`, `RpcCodec.decode(ByteBuffer, int)`), không qua stream hay mảng trung gian.
- Follower đọc các entry tại chỗ trong khung AppendEntries, và ghi vào log đúng khung leader gửi thay vì mã hoá lại.
- `EntryFrame` ghi và đọc theo vị trí tuyệt đối; CRC32 dùng lại theo thread.
- clientId và id của node được giữ dạng UTF-8 trong một bảng nhỏ theo thread (`Utf8Cache`).

Đo với `ClusterBenchmark` (512 client, không fsync), so với trước các thay đổi này: 530.000–554.000 → 618.000–622.000 TPS,
p99 1,8–2,0 → 1,55 ms, số lần GC thế hệ trẻ trên ba node 55–59 → 39, tổng thời gian dừng 155–172 → 106–109 ms.
Mỗi lần dừng vẫn khoảng 2,5–3 ms: thời gian đó phụ thuộc lượng object còn sống (~20 MB), không phụ thuộc lượng rác.
Phần còn được cấp phát cho mỗi lệnh chủ yếu là thứ API cần giữ: mảng lệnh, khung của entry trong log, `LogEntry` và
future trả về cho người gọi.

### `RaftRuntime`

`RaftNode` không tự tạo thread hay đọc đồng hồ hệ thống; nó xin mọi thứ qua `RaftRuntime`:
thời gian hiện tại, hẹn giờ, thread xử lý của node, chạy việc ghi đĩa, và số ngẫu nhiên cho election timeout.
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

- Mỗi follower chỉ có **một request đang bay**, chứa nhiều nhất `maxEntriesPerRequest` entry. Khi nó trả lời mà leader còn entry
  chưa gửi, leader gửi tiếp ngay; entry đến trong lúc chờ được gom vào request sau.
- Follower kiểm tra entry đứng trước (`prevLogIndex`, `prevLogTerm`). Nếu không khớp, nó trả về một gợi ý để leader lùi nhanh
  thay vì lùi từng entry một.
- Follower **chỉ cắt log khi thật sự xung đột term**. Entry đã có sẵn (do request gửi trùng hoặc đến muộn) được bỏ qua;
  nhờ vậy một request cũ đến sau không xoá mất entry mới hơn.
- Follower chỉ trả lời thành công sau khi entry đã được fsync.
- Leader gửi entry cho follower **song song** với việc fsync log của chính nó, và chỉ tính mình vào quorum cho phần log đã nằm trên đĩa.
- Commit index là index lớn nhất mà đa số đã có (với joint config: đa số ở cả hai cấu hình), với điều kiện entry đó thuộc term hiện tại.
- Phần follower cần đã rời khỏi bộ nhớ (xem `logCacheEntries`) thì leader đọc lại từ segment trên đĩa ở một thread riêng,
  ngoài lock, rồi mới gửi; các lệnh mới không phải chờ lần đọc đó.
- Phần follower cần đã bị compact thì leader gửi snapshot thay cho entry.
- Khi số lệnh chờ commit vượt `maxPendingCommands`, leader từ chối lệnh mới thay vì để hàng chờ lớn mãi.

### Pipelining

Leader theo dõi từng follower theo hai trạng thái, như `Progress` của etcd/raft:

- **Dò:** chưa biết log của follower khớp tới đâu (vừa lên làm leader, follower vừa từ chối, một request bị mất).
  Mỗi lúc một AppendEntries; `nextIndex` chỉ đổi khi có câu trả lời.
- **Pipeline:** lần gửi trước đã khớp. Leader gửi liên tiếp tới `maxInflightAppends` (mặc định 32) request mà không chờ,
  đẩy `nextIndex` lên ngay khi gửi. Transport giữ thứ tự trên một kết nối, nên follower nhận đúng thứ tự gửi.

Follower từ chối, hoặc một request bị mất hay hết hạn, thì leader quay về dò từ phần đã chắc chắn (`matchIndex + 1`).
Response của các request gửi trước đó vẫn cập nhật được `matchIndex` (một lần ack thành công luôn đúng), nhưng không còn
được tính là request đang bay. `matchIndex` chỉ tăng khi follower trả lời, không bao giờ tăng lúc gửi.

Đo với độ trễ mạng thật ở tầng kernel giữa các node (`tc netem`, xem `bench/netem/netem.sh`), có fsync, lệnh 128 byte:

| RTT giữa các node | Client | 1 request mỗi lần | Pipelining (32) | Pipelining + lô 100 lệnh |
|---|---|---|---|---|
| 0,27 ms | 512 | 225.000 TPS, p50 1,8 ms | 230.000, p50 1,9 ms | — |
| 0,27 ms | 32 | 24.900, p50 1,3 ms | 22.800, p50 1,1 ms | 785.000, p50 3,8 ms |
| 1,07 ms | 32 | 12.900, p50 2,3 ms | 14.400, p50 2,1 ms | 797.000, p50 3,8 ms |
| 1,07 ms | 512 | 161.000, p50 3,0 ms | 199.000, p50 2,4 ms | 1.780.000, p50 31 ms |
| 10,2 ms | 32 | 1.900, p50 16,5 ms | 3.000, p50 10,7 ms | 140.000 |
| 10,2 ms | 512 | 29.000, p50 17,5 ms | 45.700, p50 11 ms | 808.000 |
| 10,2 ms | 4096 | 86.000, p50 47 ms | 113.000, p50 14 ms | 172.000 |

Khi RTT nhỏ hơn thời gian fsync, đĩa là giới hạn và pipelining không đổi được gì. RTT càng lớn thì pipelining càng có ích:
mỗi lệnh chờ khoảng một RTT thay vì gần hai. Ở RTT 10 ms và tải rất nặng (hàng nghìn client, hoặc lô lớn) độ trễ đuôi
lên tới hàng trăm mili giây đến hơn một giây và dao động mạnh giữa các lần đo: cửa sổ hiện đếm theo số request, chưa đếm
theo số byte, nên một follower có thể bị dồn hàng chục MB trên một kết nối (TCP báo cửa sổ nhận bằng 0).

Không có quyền root thì có thể giả lập độ trễ trong Java bằng `-Dbench.peerRttMicros` (truyền cho node qua
`-Dbench.nodeArgs`); `-Dbench.maxInflight=1` tắt pipelining. Trên loopback (gần như không có độ trễ) pipelining làm trần
thấp hơn chừng 5-10% vì leader gửi nhiều request nhỏ hơn; khi các node cùng một máy hoặc một rack thì có thể đặt
`maxInflightAppends = 1`.

### Các quy tắc về độ bền

Mọi thứ một node hứa với node khác phải nằm trên đĩa trước khi lời hứa được gửi đi:

| Lời hứa | Phải bền vững trước |
|---|---|
| "Tôi bầu cho bạn" | term và votedFor |
| RequestVote của candidate | phiếu tự bầu của nó |
| Mọi câu trả lời có kèm term | term đó |
| "Tôi đã có các entry này" | các entry đó |
| Leader tính mình vào quorum | phần log tương ứng của leader |
| Xác nhận với client | entry đã commit (đã bền vững trên đa số) |

Khi leader hoặc candidate thấy term cao hơn trong một response, nó step-down ngay trong bộ nhớ và việc ghi term mới được xếp
vào thread IO; node không gửi ra câu trả lời hay yêu cầu nào mang term đó trước khi việc ghi hoàn tất.

Commit index cũng được lưu, nhưng chỉ để lần khởi động sau apply lại nhanh hơn; mất nó không ảnh hưởng tính đúng đắn.

### Snapshot

**Tạo snapshot** (`createSnapshot`, hoặc tự động theo `snapshotIntervalEntries`):

1. Trong lock: ghi nhận `lastApplied`, term của nó, cấu hình thành viên và bảng chống trùng tại thời điểm đó,
   rồi gọi `onSnapshotSave` với một thư mục `temp/`.
2. State machine ghi file của nó vào `temp/`.
3. Ở thread IO: tính CRC32 của từng file, ghi file meta, fsync, rồi **rename** `temp/` thành `snapshot_<index>/`.
   Snapshot cũ bị xoá sau đó.
4. Trong lock: bỏ các segment log đã nằm trọn trong snapshot khỏi log (chỉ trong bộ nhớ). File của chúng được xoá ngay sau đó
   ở thread nền: xoá trong lock làm cả node đứng hàng chục mili giây mỗi lần snapshot.

Vì bước 3 là một lần rename, snapshot hoặc có đủ hoặc không có gì, dù mất điện ở bất kỳ lúc nào.

**Gửi snapshot** cho follower tụt lại: leader đọc và gửi từng mẩu không quá `snapshotChunkBytes`, mỗi lần một mẩu,
nên không bên nào phải giữ cả snapshot trong bộ nhớ. Follower ghi từng mẩu vào `temp/` và fsync.
Mẩu không nối tiếp đúng chỗ (mất mẩu, đổi leader, snapshot mới thay snapshot đang gửi) làm lần truyền bắt đầu lại từ đầu.

**Cài snapshot** khi mẩu cuối về: state machine load từ `temp/` ngoài lock (việc apply bị hoãn trong lúc đó), rồi mới rename,
cuối cùng cập nhật log và index trong lock. Load trước rồi mới lưu: nếu load hỏng thì snapshot và log cũ trên đĩa còn nguyên
để lần khởi động sau dựng lại.

### Thay đổi thành viên

1. **Bắt kịp.** Node mới nhận log (hoặc snapshot) từ leader như một learner: nó chưa thuộc cấu hình, không được tính vào quorum
   và không bầu cử. Nhờ vậy thêm một node trống, chậm hay đang chết không làm cluster chậm lại hay mất khả năng commit.
2. **Joint.** Khi mọi node mới chỉ còn cách leader không quá một request, leader ghi cấu hình joint C(old, new).
   Trong giai đoạn này mọi quyết định (bầu cử, commit) cần đa số ở **cả hai** cấu hình.
3. **Kết thúc.** Khi C(old, new) commit, leader ghi tiếp C(new). Khi C(new) commit, thay đổi hoàn tất.

Chi tiết:

- Cấu hình có hiệu lực ngay khi entry vào log, kể cả chưa commit (đúng theo Raft). Nếu entry đó bị cắt bỏ, cấu hình quay về bản trước.
- Cấu hình tại thời điểm snapshot được lưu cùng snapshot, vì entry cấu hình có thể đã bị compact khỏi log.
- Nếu node mới không bắt kịp trong `catchUpTimeoutMs`, hoặc leader đổi trong lúc chờ, yêu cầu bị bỏ và phải gọi lại.
- Leader tiếp tục gửi log cho node vừa bị gỡ tới khi node đó nhận được C(new) và biết C(new) đã commit, hoặc tới khi hết
  `departingTimeoutMs`. Leader mới đắc cử giữa chừng tự suy ra các node đang rời đi từ log.
- Leader gỡ chính mình: điều phối tới khi C(new) commit, gửi `TimeoutNow` lần lượt cho các follower (log đầy đủ nhất trước)
  để một node bầu cử ngay, rồi step-down.
- Node bị gỡ tự `shutdown()` khi biết C(new) đã commit, trừ khi `shutdownOnRemoved = false`.

### Đọc nhất quán (ReadIndex)

Trên leader:

1. Leader ghi nhận **readIndex** = commit index hiện tại, nhưng không nhỏ hơn index của no-op mà nó ghi khi nhậm chức
   (trước khi no-op commit, leader mới chưa chắc commit index của mình đã đủ mới).
2. Leader gửi một vòng heartbeat mới và chờ **đa số trả lời các request được gửi sau thời điểm đó**.
   Response của heartbeat gửi trước đó không được tính: chúng chỉ chứng minh node là leader trước khi có yêu cầu đọc.
3. Node chờ state machine của mình apply tới readIndex, rồi chạy hàm đọc trong lock.

Trên follower: node gửi RPC `ReadIndex` cho leader; leader làm bước 1 và 2 rồi trả về readIndex; follower làm bước 3 trên
dữ liệu của chính nó. Mỗi lúc follower chỉ có một `ReadIndex` đang bay: các lần đọc đến trong lúc đó được gom lại và đi chung
request kế tiếp, nên số request tới leader không tăng theo số lần đọc đồng thời.

Cơ chế này không ghi gì vào log và không phụ thuộc đồng hồ, nên đúng cả khi đồng hồ các node chạy lệch nhau.

### Chống ghi trùng

Một lệnh có `clientId` và `sequence` được ghi vào log kèm hai giá trị đó. Mỗi node giữ, cho từng client, mốc
"mọi sequence tới đây đã apply" cùng các sequence lẻ phía trên mốc (xuất hiện khi client gửi nhiều lệnh cùng lúc và chúng
vào log không theo thứ tự). Entry có sequence đã apply bị bỏ qua, không gọi state machine.

- Bảng chỉ phụ thuộc vào các entry đã apply, nên mọi node quyết định giống nhau.
- Bảng được lưu trong snapshot và gửi kèm khi cài snapshot, nên sống qua restart và compact log.
- Leader trả lời `true` ngay cho lệnh đã apply mà không ghi thêm vào log.
- `closeClientSession` ghi một entry làm mọi node quên client đó. Không có cơ chế tự hết hạn: client không được đóng
  sẽ chiếm một dòng trong bảng mãi mãi.

## Dữ liệu trên đĩa

Mỗi node ghi vào ba thư mục cấu hình trong `NodeOptions` (có thể là cùng một thư mục):

```
data/n1/
├── raft_meta.json            term, votedFor
├── commit_index              commit index gần nhất, ghi không fsync
├── log_1.rec                 segment log kích thước cố định, tên file là index đầu tiên của segment
├── log_5001.rec
├── snapshot_5000/
│   ├── snapshot.data         file do state machine ghi
│   └── __raft_snapshot_meta.json
└── temp/                     snapshot đang ghi dở, bị xoá khi khởi động
```

- **`raft_meta.json`**: dòng đầu là CRC32 của phần còn lại, tiếp theo là JSON. Được thay bằng cách ghi file tạm, fsync, rename, fsync thư mục.
- **`commit_index`**: commit index được ghi đè theo nhịp `commitIndexFlushIntervalMs`, có CRC32 nhưng **không fsync**: nó chỉ giúp lần
  khởi động sau apply lại nhanh hơn, và file hỏng hay mất thì bị bỏ qua. Ghi nó vào `raft_meta.json` (có fsync) từng làm mọi lệnh
  đang ghi khựng lại hàng trăm mili giây mỗi giây, vì một lần fsync trên ext4 kéo theo cả lượng log đang chờ xuống đĩa.
- **`log_<firstIndex>.rec`**: các khung nhị phân nối nhau, xem [Định dạng log](#định-dạng-log) bên dưới.
- **`snapshot_<index>/`**: file của state machine và file meta. File meta chứa index và term cuối cùng của snapshot,
  cấu hình thành viên, bảng chống trùng, danh sách file và CRC32 của từng file.

### Định dạng log

Log được lưu theo kiểu Aeron Archive bởi `com.namnv.storage.binary.BinaryLogStorage`:

- Các file `log_<firstIndex>.rec` có **kích thước cố định** (`logSegmentBytes`, file thưa). 32 byte đầu là header của segment
  (magic, version, index đầu tiên, mốc "đã lên đĩa"). Sau đó là các **khung** nối nhau, mỗi khung bắt đầu ở vị trí chia hết cho 32:
  `[độ dài][CRC32][index][term][sequence]` rồi cờ, `clientId`, cấu hình và lệnh (chép nguyên byte). Một khung không vắt qua hai segment.
- Việc ghi chia làm **hai tầng** như Aeron. Append (trong lock của node) chỉ dựng khung trong bộ nhớ và xếp vào hàng chờ: nó
  không bao giờ chạm tới đĩa nên không thể bị hệ điều hành giữ lại. `sync()` (ngoài lock) ghi cả khối khung đang chờ xuống file
  bằng một lời gọi rồi fsync; entry chỉ được coi là bền vững sau bước này. Mỗi lúc chỉ một thread ghi file.
- Khi log được fsync và lớn chậm, file của segment kế tiếp được **cấp phát sẵn** ở thread nền (`logPreallocate`): fsync trên
  file thưa phải chờ filesystem ghi nhận các block mới cấp phát, chậm hơn vài lần so với fsync trên file đã cấp phát.
- Segment chỉ bị xoá khi mọi entry của nó đã nằm trong snapshot, nên log trên đĩa giữ thừa nhiều nhất một segment.
- Cùng các khung đó được gửi cho follower trong AppendEntries, nên leader không mã hoá lại entry cho từng follower.
  Khung chỉ được giữ trong bộ nhớ một lúc ngắn (vài MB gần nhất): giữ lâu hơn làm GC phải chép đi chép lại chúng.
- Khác Aeron ở một điểm: mặc định `sync()` vẫn fsync trước khi entry được coi là bền vững (xem `logSync`), nên các quy tắc về
  độ bền ở trên không đổi.
- Khi mở lại, mỗi segment được đọc tới khung nguyên vẹn cuối cùng. Phần đứng sau mốc "đã lên đĩa" là đuôi chưa từng được ack và bị bỏ;
  khung hỏng đứng trước mốc đó làm việc mở log thất bại thay vì bị âm thầm cắt. Mốc này được ghi sau mỗi lần fsync nhưng chỉ
  lên đĩa cùng lần fsync kế tiếp, nên một entry hỏng nằm giữa hai mốc sẽ bị coi là đuôi ghi dở.

Log dạng JSON (`log_*.jsonl`) của các phiên bản trước không còn đọc được: node từ chối khởi động trên thư mục còn các file đó
thay vì coi như log rỗng.

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
- **Bộ nhớ:** chỉ `logCacheEntries` entry mới nhất của log nằm trong RAM; phần còn lại nằm trên đĩa cho tới khi snapshot compact nó. Đặt `snapshotIntervalEntries` để log trên đĩa không lớn mãi.
- **Định dạng đĩa và định dạng RPC** đã đổi nhiều lần trong quá trình phát triển và không có cơ chế nâng cấp:
  dữ liệu của bản cũ không đọc lại được, và các node phải chạy cùng một bản.
- **Log** đi qua SLF4J; demo dùng Log4j2 với cấu hình ở `silkroad-raft-samples/src/main/resources/log4j2.xml`.
- **JVM:** đặt heap cố định (`-Xms` bằng `-Xmx`, ví dụ 2 GB) và `-XX:+AlwaysPreTouch`. Với G1, cách này giảm số lần GC
  khoảng 3,5 lần và lần dừng lâu nhất từ ~10 ms xuống ~4 ms so với để JVM tự chọn kích thước heap. ZGC gần như không dừng
  (dưới 0,03 ms) và cho throughput ngang G1, nhưng nó dọn rác song song nên cần CPU rảnh: trên máy bị giới hạn CPU, độ trễ
  đuôi dao động mạnh hơn hẳn (đã đo tới hàng trăm mili giây).
- **CPU:** trên CPU có lõi hiệu năng và lõi tiết kiệm (Intel thế hệ 12 trở đi), thread của node có thể bị xếp lên lõi chậm.
  Gắn tiến trình node vào các lõi hiệu năng (`taskset -c ...`) tăng throughput khoảng 15% (512 client, không fsync:
  ~600.000 → ~700.000 TPS, p99 1,5 → 1,24 ms).

## Transport

`RaftNode` gửi RPC qua interface `RpcProcessor` và nhận RPC qua interface `RaftServerService` (do chính nó cài đặt).
Có sáu RPC: PreVote, RequestVote, AppendEntries, InstallSnapshot, TimeoutNow, ReadIndex.

- **`InMemoryRpcClient`**: gọi thẳng handler của node đích trong cùng tiến trình. Có bảng `reachable` để giả lập chia cắt mạng. Dùng cho test và demo.
- **`SocketRpcClient` + `SocketRpcServer`**: TCP tự viết, không phụ thuộc thư viện mạng nào.
  - **Khung:** mỗi message là `[4 byte độ dài][8 byte id của request][1 byte loại][nội dung nhị phân]` (`RpcCodec`).
    Nội dung được mã hoá theo từng trường, `byte[]` đi nguyên dạng.
  - **Ghép kênh:** mỗi node đích một kết nối dùng lại. Mọi lời gọi tới node đó đi chung kết nối và không chờ nhau:
    response mang id của request, một thread đọc response về và trả cho đúng lời gọi. Phía server trả lời theo id nên
    một lời gọi chậm không chặn heartbeat: RPC có thể chờ đĩa (AppendEntries, RequestVote, InstallSnapshot) chạy ở thread riêng,
    còn yêu cầu của client và `ReadIndex` chỉ đăng ký việc rồi được trả lời sau, nên được xử lý ngay trên thread đọc.
  - **Ghi theo đợt:** mỗi chiều của một kết nối có một hàng ghi (`FrameWriter`). Message được xếp hàng mà không chờ mạng, và một
    thread ghi hết những gì đang chờ rồi mới flush một lần, nên khi tải cao nhiều message đi chung một lời gọi hệ thống.
  - **Thời hạn:** có timeout cho lúc kết nối và cho từng lời gọi. Response về sau khi lời gọi đã hết hạn bị bỏ qua, không lẫn sang lời gọi khác.
  - **Kết nối hỏng:** mọi lời gọi đang chờ trên nó thất bại ngay và lời gọi sau mở kết nối mới. Lời gọi đi trên một kết nối cũ
    mà phía kia đã đóng được tự gửi lại một lần trên kết nối mới.
  - **Dữ liệu không hợp lệ:** bên nhận chỉ dựng được 12 loại message của Raft; mọi độ dài đọc vào được đối chiếu với kích thước khung,
    khung lớn hơn 64 MiB bị từ chối, và kết nối bị đóng khi gặp bất cứ thứ gì không hợp lệ.
  - **TLS:** truyền một `SSLContext` vào cả client và server để mã hoá và xác thực hai chiều. Server luôn đòi chứng chỉ của client,
    nên chỉ node có chứng chỉ được truststore tin mới gọi được RPC.

```java
SSLContext tls = TlsContexts.fromKeyStores(Path.of("node.p12"), Path.of("cluster-trust.p12"), password);
RpcProcessor rpc = new SocketRpcClient(1000, tls);
new SocketRpcServer(8080, node, tls).start();
```

- **`NioRpcClient` + `NioRpcServer`** (gói `rpc.nio`): cùng định dạng khung, nhưng chạy trên vòng của node
  (`ThreadedRuntime.loop()`, một `AgentLoop`) theo cách Aeron làm: socket không chặn, và chính thread của node poll chúng
  trong mỗi vòng làm việc (`selectNow`). Request đến được chuyển vào hàng sự kiện của node ngay trên thread đó, response
  và request đi ra được gom lại và ghi ra socket một lần ở cuối vòng. Không có thread đọc hay thread ghi riêng, nên trên
  đường đi của một lệnh không có lần đánh thức thread nào. Ở tải thấp, việc đánh thức thread chính là phần tốn nhất:
  một client ghi tuần tự đạt khoảng 2,3 lần số lệnh/giây so với `SocketRpc*` (xem [Đo hiệu năng](#đo-hiệu-năng)).
  RequestVote, PreVote, InstallSnapshot và TimeoutNow có thể chờ đĩa nên chạy ở thread riêng, vì thread của node
  không bao giờ được chờ đĩa. Transport này chưa hỗ trợ TLS; cần TLS thì dùng `SocketRpc*`. Hai loại transport nói chuyện
  được với nhau, nên client có thể dùng `SocketRpcClient` để gọi node chạy `NioRpcServer`.
  - Idle strategy (`-Draft.agent.idle`): `backoff` (mặc định) quay chờ `-Draft.agent.spinMicros` (mặc định 50) rồi chặn
    trong `select()`. Khi đang chặn, thread được đánh thức ngay lúc có dữ liệu mạng hoặc có việc mới, không ngủ mù 1 ms như
    `BackoffIdleStrategy` của Aeron. `busy` thì không bao giờ ngủ, giống `BusySpinIdleStrategy`: độ trễ thấp nhất, nhưng
    mỗi node chiếm trọn một CPU kể cả lúc rảnh.

```java
var runtime = new ThreadedRuntime();
var node = new RaftNode(NodeOptions.builder().runtime(runtime) /* ... */ .build(),
        new NioRpcClient(runtime.loop(), 1000));
new NioRpcServer(8080, node, clientService, runtime.loop()).start();
// phía client: một vòng riêng, dùng chung được cho nhiều RaftClient
var client = new RaftClient(new NioRpcClient(1000), servers, "client-1", 15_000);
```

Muốn dùng transport khác (gRPC, Netty...), cài đặt `RpcProcessor` cho phía gửi và gọi các hàm `handle...Request` của node ở phía nhận.
Một lời gọi thất bại chỉ cần làm future hoàn tất với exception; node tự gửi lại ở nhịp sau.

## Test

```bash
mvn test                                                    # tất cả
mvn test -pl silkroad-raft-cluster -am                          # một module (cùng các module nó cần)

# mô phỏng tất định: nhiều seed hơn, hoặc chạy lại đúng một seed
ONE="-pl silkroad-raft-cluster -am -Dsurefire.failIfNoSpecifiedTests=false"
mvn test $ONE -Dtest=RaftSimulationTest -Dsim.runs=500
mvn test $ONE -Dtest=RaftSimulationTest -Dsim.seed=123456

# fault injection chạy thread thật, lâu hơn
mvn test $ONE -Dtest=RaftChaosTest -Dchaos.runs=10 -Dchaos.seconds=15
```

| Bộ test | Kiểm tra gì |
|---|---|
| `RaftSimulationTest` | Cả cluster, mạng, đĩa, client và nemesis chạy trên một thread với thời gian ảo. AppendEntries đi qua mã hoá nhị phân như trên mạng thật. Một seed luôn cho đúng một lịch sử, nên lỗi tìm ra thì chạy lại được y hệt. Có lượt 5 node, 7 node và một lượt dài nửa giờ ảo |
| `RaftChaosTest` | Cùng kịch bản nhưng với thread và đồng hồ thật, để bắt lỗi tranh chấp giữa các thread. Seed ở đây không tái hiện chắc chắn |
| `RaftClusterTest` | Test tất định cho từng hành vi và từng quy tắc an toàn, trên cluster in-memory |
| `LmdbKvStateMachineTest`, `RocksDbKvStateMachineTest` | Cùng một bộ test cho mỗi kho KV: đọc thấy lệnh chưa vào LMDB, xoá, snapshot đúng thời điểm khi còn lô chưa ghi, nạp snapshot thay state cũ; cluster 3 node qua mạng, một follower tắt lâu rồi bật lại và nhận snapshot từ leader |
| `RaftClientTest`, `NioRaftClientTest` | Client đi qua TCP tới cluster 3 node với từng loại transport: ghi/đọc, nhiều client, lệnh 300 KB, leader chết giữa chừng |
| `SocketRpcTest` | Transport TCP: từng loại RPC, lời gọi đồng thời và lời gọi chậm trên cùng kết nối, nối lại, timeout, khung không hợp lệ, TLS, cluster qua socket thật |
| `RpcCodecTest` | Mã hoá nhị phân: mọi loại message đi và về đúng từng byte, message bị cắt cụt hoặc thừa byte, 20.000 khung ngẫu nhiên |
| `BinaryLogStorageTest` | Mọi loại entry, nhiều segment, cắt đầu/cắt đuôi, khung ghi dở, đuôi chưa sync sau một lỗ hổng, dữ liệu đã sync bị hỏng |
| `SnapshotStoreTest` | Trạng thái đĩa ở từng thời điểm crash trong lúc ghi snapshot, snapshot hỏng |
| `ConfigurationEntryTest` | Phép tính quorum, kể cả cấu hình joint |

### Lỗi được chèn vào trong mô phỏng

- Mạng: mất request hoặc response, trễ, đảo thứ tự, request đến hai lần, gói tin kẹt rất lâu rồi mới tới, chia cắt mạng, cô lập đúng leader.
- Node: tắt bình thường, mất điện (mọi thứ chưa fsync biến mất), bật lại, đồng hồ chạy lệch từ chậm 25% tới nhanh 30%.
- Đĩa: thao tác ghi log, fsync, ghi meta và lưu snapshot thất bại ngẫu nhiên.
- Vận hành: snapshot thủ công và tự động, snapshot gửi qua nhiều mẩu nhỏ, gỡ và thêm thành viên, kể cả gỡ leader.
- Client: ghi liên tục và gửi lại khi không biết kết quả; đọc nhất quán liên tục từ một node bất kỳ, leader hay follower.

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
(ví dụ: commit không cần quorum, bầu hai lần trong một term, trả lời trước khi fsync, đọc không xác nhận với đa số, đọc trên follower
không chờ apply, bỏ chống trùng, bỏ kiểm tra checksum, TLS không đòi chứng chỉ của client). Mỗi lỗi như vậy đều làm ít nhất một test fail.
Những lỗi mà mô phỏng ngẫu nhiên khó chạm tới đều có test tất định riêng.

## Đo hiệu năng

Các chương trình trong module `silkroad-raft-benchmarks` (chạy bằng tay):

```bash
mvn -q compile dependency:build-classpath -pl silkroad-raft-benchmarks -am -Dmdep.includeScope=runtime -Dmdep.outputFile=target/bench-cp.txt
CP="silkroad-raft-benchmarks/target/classes:$(cat target/bench-cp.txt)"

# 3 node là 3 tiến trình riêng, client ở tiến trình thứ tư, mọi thứ đi qua TCP
java -cp "$CP" com.namnv.bench.ClusterBenchmark
# không fsync log (NodeOptions.logSync = false)
java -Dbench.logSync=false -cp "$CP" com.namnv.bench.ClusterBenchmark
# chỉ đo một phần của bảng, ví dụ lệnh 4 KB với 512 client:
java -Dbench.payloads=4096 -Dbench.clients=512 -Dbench.phases=write -cp "$CP" com.namnv.bench.ClusterBenchmark

# transport cũ (thread đọc/ghi riêng) thay cho NIO trên vòng của node; hoặc vòng không bao giờ ngủ
java -Dbench.transport=socket -cp "$CP" com.namnv.bench.ClusterBenchmark
# các node chạy kho KV (put key 8 byte tăng dần, get một key), như công cụ benchmark của etcd;
# -Dbench.kvSync=false: kho không fsync mỗi lô (RocksDB: tắt WAL) trong khi Raft log vẫn fsync
java -Dbench.kv=lmdb -cp "$CP" com.namnv.bench.ClusterBenchmark
java -Dbench.kv=rocksdb -cp "$CP" com.namnv.bench.ClusterBenchmark
# ghi theo lô: mỗi lần ghi là một lô 100 lệnh (TPS tính theo số lệnh)
java -Dbench.batch=100 -cp "$CP" com.namnv.bench.ClusterBenchmark
# độ trễ mạng thật giữa các node (cần sudo): mỗi node một địa chỉ 127.0.1.N, tc netem làm chậm riêng đường giữa chúng
bench/netem/netem.sh on 500us
java -Dbench.nodeIps=true -cp "$CP" com.namnv.bench.ClusterBenchmark
bench/netem/netem.sh off
java -Draft.agent.idle=busy -Dbench.nodeArgs=-Draft.agent.idle=busy -cp "$CP" com.namnv.bench.ClusterBenchmark

# Aeron Cluster theo cùng kịch bản, để so sánh: project riêng trong bench/aeron-cluster (xem README trong đó)

# 3 node trong một tiến trình: so sánh transport, đĩa thật với tmpfs, và bộ mã hoá
java -cp "$CP" com.namnv.bench.RaftBenchmark
```

Khi có fsync, kết quả phụ thuộc gần như hoàn toàn vào tốc độ fsync của ổ đĩa. Độ trễ đuôi cũng vậy: một số SSD có những đợt
vài giây mà fsync chậm đi nhiều lần, và trong các đợt đó mọi lệnh ghi chậm theo.

`ClusterBenchmark` trên một máy i5-13500, ổ NVMe (fsync khoảng 1 ms), lệnh 128 byte, transport NIO, idle `backoff`:

| | 1 client | 32 client | 512 client |
|---|---|---|---|
| ghi, có fsync | ~1.350 TPS, p50 0,76 ms | ~29.000 | ~250.000 |
| ghi, không fsync | ~35.000 TPS, p50 21 µs | ~208.000 | ~620.000 |
| đọc nhất quán qua leader | ~42.000/s, p50 19 µs | ~227.000 | ~675.000 |
| đọc nhất quán qua follower | ~30.000/s, p50 28 µs | ~242.000 | ~690.000 |

Với `SocketRpc*` thì ghi không fsync đạt khoảng 15.000 TPS (1 client), 226.000 (32 client) và 500.000 (512 client).

## Giới hạn đã biết

- **Chưa được kiểm chứng ở mức production.** Chưa có kiểm thử kiểu Jepsen trên nhiều máy thật, chưa chạy liên tục nhiều giờ dưới tải.
- **Quy mô đã thử còn nhỏ:** 10.000 lệnh, cluster tối đa 7 node, nửa giờ thời gian ảo.
- **State machine chạy `onApply` trong lock của node**, nên một lệnh apply chậm chặn cả node.
- **Không có lease read:** mỗi lần đọc nhất quán cần một vòng heartbeat của leader. Đây là lựa chọn có chủ ý, vì lease read phụ thuộc đồng hồ.
- **Bảng chống trùng không tự hết hạn** (phải gọi `closeClientSession`), và sequence của một client phải bắt đầu từ 1, tăng liền nhau.
- **TLS không kiểm tra tên máy chủ** trong chứng chỉ; việc xác thực dựa hoàn toàn vào truststore. Các mẩu snapshot không có checksum riêng
  khi truyền (chỉ dựa vào TCP/TLS), dù snapshot được kiểm tra CRC khi lưu và khi mở lại.
- **Yêu cầu thay đổi thành viên đang chờ node mới bắt kịp bị mất khi leader đổi**, và không có cơ chế ép đổi cấu hình khi mất đa số.
- **Metrics chỉ là một ảnh chụp trong tiến trình**, không có exporter.
- **Mô phỏng mất điện** chỉ áp lên log và file meta; các thời điểm crash quanh snapshot được kiểm tra bằng test dựng lại trạng thái đĩa.
- **Tính đúng đắn dựa trên test và lập luận**, không có chứng minh hình thức hay model checking.
