❯ - Độ trễ đuôi: ở 512 client, p99 là 15–20 ms và lần chậm nhất 35–95 ms, gấp nhiều lần p50. Với hệ thống cần độ trễ ổn
định thì đây là điểm yếu. -> tìm nguyên nhân và fix đi

Searched for 1 pattern, ran 4 shell commands

Đã xác định nguyên nhân: các lần chậm khớp với GC dừng 35–127 ms do mỗi lệnh tạo timer riêng tồn tại tới 10 giây. Mình
đã thay bằng một nhịp quét chung, giờ chạy lại test để đo lại.

Read 1 file, ran 4 shell commands

Vẫn chưa hết: sau khi giới hạn log trong bộ nhớ, GC vẫn dừng tới 170–220 ms, nên còn một nguồn giữ bộ nhớ khác. Mình
đang xem chi tiết log GC để tìm nó.

Searched for 1 pattern, ran 5 shell commands

Benchmark đơn tiến trình cho thấy độ trễ đuôi giảm rõ rệt, nhưng với ba tiến trình p99 vẫn cao (24–29 ms), cho thấy còn
nguyên nhân khác ở phía client. Tôi sẽ kiểm tra GC và dòng thời gian của các tiến trình node để xác định nguyên nhân còn
lại.

Ran 5 shell commands

Đã tìm ra và sửa nguyên nhân nằm trong code; phần độ trễ đuôi còn lại đến từ chính ổ đĩa và không sửa được bằng phần
mềm. Suite 105 test cộng 150 seed mô phỏng pass sau các thay đổi; chưa commit gì.

Nguyên nhân trong code (đã sửa)

Các lần chậm trùng khớp với các lần GC dừng 35–220 ms. Log GC cho thấy nó phải chép một chuỗi object nối tiếp rất dài:
hàng đợi của thread IO trên leader.

Mỗi lệnh ghi xếp một yêu cầu fsync riêng vào hàng đợi đó. Thread IO chỉ xử lý được khoảng 1.000 yêu cầu mỗi giây (mỗi
lần fsync khoảng 1 ms), trong khi ở tải cao có hơn 100.000 yêu cầu đến mỗi giây. Hàng đợi vì thế lớn dần không giới hạn,
và GC càng lúc càng dừng lâu.

Cách sửa: mỗi lúc chỉ có nhiều nhất một yêu cầu fsync đang xếp hàng. Một lần fsync vốn đã bao trọn mọi entry đến trước
nó, nên các yêu cầu thừa không có tác dụng gì.

Kết quả trong benchmark một tiến trình (lệnh 128 byte, 512 client, đĩa thật):

┌──────────────────┬────────────────────────┬────────────────────────┐
│ │ Trước │ Sau │
├──────────────────┼────────────────────────┼────────────────────────┤
│ GC dừng lâu nhất │ 130–220 ms │ khoảng 9 ms │
├──────────────────┼────────────────────────┼────────────────────────┤
│ p99 │ 6–20 ms │ 5,4–6,0 ms │
├──────────────────┼────────────────────────┼────────────────────────┤
│ Lần chậm nhất │ 60–170 ms │ 10–13 ms │
├──────────────────┼────────────────────────┼────────────────────────┤
│ TPS │ khoảng 113.000–120.000 │ khoảng 121.000–126.000 │
└──────────────────┴────────────────────────┴────────────────────────┘

Hai thay đổi khác làm trong lúc tìm

Cả hai không phải thủ phạm chính, nhưng giảm lượng rác sống lâu nên mình giữ lại:

- Bỏ timer hết hạn riêng cho từng lệnh và từng lần đọc. Thay bằng một nhịp quét chung mỗi 100 ms. Trước đây mỗi lệnh để
  lại một timer sống tới hết clientTimeoutMs dù lệnh đã xong.
- Log không còn nằm trọn trong bộ nhớ. Chỉ logCacheEntries entry mới nhất (mặc định 16.384) được giữ trong RAM; entry cũ
  hơn được đọc lại từ segment trên đĩa khi cần. Việc này cũng xoá một mục trong danh sách giới hạn của README. Mô phỏng
  giờ chạy với bộ đệm chỉ 32 entry để ép đường đọc từ đĩa, và 200 seed pass.

Nguyên nhân còn lại: ổ đĩa

Sau khi sửa vẫn còn những đợt 2–3 giây mà thông lượng tụt còn một phần ba và độ trễ lên 17–28 ms. Mình đo riêng ổ đĩa,
không có Raft: ba luồng ghi rồi fsync liên tục trong 20 giây. Ổ này tự có một đợt khoảng 3,5 giây mà tốc độ fsync giảm
từ khoảng 800 xuống 210 lần mỗi nửa giây, mỗi lần mất 10–41 ms. Đây là hành vi của ổ đĩa (KIOXIA KBG60), không phải của
code.

Trong benchmark ba tiến trình trên đĩa thật, p99 vì thế vẫn là 24–29 ms khi lần chạy trúng một đợt như vậy. Khi đặt dữ
liệu trên tmpfs để loại ổ đĩa ra, p99 là khoảng 10 ms và lần chậm nhất 16–18 ms, với GC của cả ba node không dừng quá 10
ms.

Cần biết

- Benchmark ba tiến trình còn một trần khác: ngay cả trên tmpfs, 512 client chỉ đạt khoảng 78.000 TPS với p50 6,4 ms,
  trong khi bản một tiến trình đạt 120.000 TPS với p50 3,3 ms. Tức là đường đi của client qua mạng đang giới hạn thông
  lượng, độc lập với ổ đĩa. Mình chưa tìm nguyên nhân; đây là vấn đề về p50 chứ không phải độ trễ đuôi.
- Đọc entry cũ từ đĩa diễn ra trong lock của node. Chỉ xảy ra khi follower tụt xa hơn bộ đệm hoặc khi khởi động lại,
  nhưng lúc đó nó chặn node trong thời gian đọc.
- Leader giờ thử fsync lại ở nhịp heartbeat nếu lần trước thất bại, thay vì chờ lệnh kế tiếp.
- README: thêm tuỳ chọn logCacheEntries, sửa ghi chú về bộ nhớ, và thêm một câu về việc độ trễ đuôi phụ thuộc ổ đĩa.

Nếu muốn độ trễ đuôi ổn định trên đĩa thật thì cần ổ có fsync đều hơn (SSD có bảo vệ mất điện thường không có các đợt
này). Bạn có muốn mình tìm tiếp nguyên nhân của trần 78.000 TPS ở đường client không?

✻ Sautéed for 22m 58s · done 16:59
