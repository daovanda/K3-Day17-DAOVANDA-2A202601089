# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đào Văn Đa  **MSSV:** 2A202601089  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

Do môi trường thực hiện là Windows PowerShell, lệnh tương đương được dùng là:

```powershell
& .\.venv\Scripts\python.exe -X utf8 tools\verify.py
```

Kết quả đầy đủ ba lượt chạy:

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 63.8s
run 2/3 … 68.3s
run 3/3 … 69.2s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

Tổng kết: **4/4 tiêu chí chính đạt, Extra A đạt, Extra B đạt — 110/100 điểm kỹ thuật**.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau một lượt phát lại 14 ngày, `gold_training_set` có 13.790 hàng thay vì 12.480; 1.310 `ticket_id` bị lặp. Khi chạy lại, kích thước tiếp tục tăng dù nguồn không đổi. |
| **Nguyên nhân** | `gold_training_set` là incremental model nhưng không khai báo `unique_key`, nên dbt dùng phép ghi kiểu append/`INSERT`. Một ticket có thể xuất hiện ở nhiều partition ngày do CDC có bản ghi cập nhật `op='u'`; mỗi lần backfill hoặc retry lại chèn thêm cùng entity thay vì cập nhật hàng đã có. `catchup=True` và không giới hạn `max_active_runs` làm tăng khả năng kích hoạt lỗi, nhưng không phải root cause của việc nhân bản. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, khai báo `unique_key='ticket_id'` và `incremental_strategy='merge'`, giữ nguyên bộ lọc partition theo `run_date`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | Trước: 13.790 hàng, 1.310 ticket lặp sau một lượt. Sau: 12.480 hàng, không lặp; checksum ba lượt đều là `8dd7c98653`. |

Grain của bảng là entity, một hàng trên một `ticket_id`. Vì vậy merge theo khóa
entity phù hợp hơn append hoặc delete/insert theo ngày: một ticket tạo ngày D1
và cập nhật ngày D2 phải thay thế trạng thái cũ, kể cả khi hai bản ghi CDC nằm
ở hai partition khác nhau.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 hàng thay vì 9.100, thiếu 455 cặp `(event_date, customer_id)`. Các cặp thiếu tập trung ở những ngày cũ từ 03/08 đến 13/08. |
| **P99 độ trễ đo được** | **2,725833 ngày**. P50 = 0,128090 ngày, P95 = 1,813693 ngày, max = 2,944688 ngày; 5,0509% event tới muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày**, làm tròn P99 lên ngày nguyên kế tiếp. Điều kiện lấy lại dữ liệu từ `max(event_date) - interval 3 day`. |
| **Nguyên nhân** | Bộ lọc incremental cũ chỉ nhận `event_date > max(event_date)` đã có trong target. Vì watermark dựa trên thời điểm xảy ra sự kiện thay vì thời điểm dữ liệu tới kho, một event xảy ra ngày 12 nhưng ingest ngày 15 không còn lớn hơn watermark và bị bỏ qua vĩnh viễn. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, mở cửa sổ tính lại 3 ngày; khai báo khóa ghép `['event_date', 'customer_id']` và dùng chiến lược `merge` để kết quả tính lại thay thế hàng cũ. |
| **Bằng chứng** | Trước: 8.645 hàng, thiếu 455. Sau: đúng 9.100 hàng; checksum ba lượt đều là `3db448685c`. |

P99 được dùng làm căn cứ vì nó bao phủ gần như toàn bộ độ trễ quan sát được
nhưng không để một outlier cực đoan làm tăng chi phí vĩnh viễn. Trong dataset
này, cửa sổ 3 ngày cũng bao phủ giá trị max 2,944688 ngày. Nếu chọn theo max
và tương lai xuất hiện một outlier rất lớn, mọi lượt chạy sau đều phải quét và
tính lại thêm nhiều partition. Chi phí của mỗi ngày lookback được trả ở mọi
lượt chạy, không chỉ một lần.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Trước khi sửa có 6.606 hàng trong `silver_tickets` có priority ngoài miền 1..4 hoặc NULL, trong khi `dbt test` vẫn báo 9/9 pass; `quarantine_tickets` rỗng dù nguồn có 312 bản ghi lỗi. |
| **Nguyên nhân** | Macro chỉ dùng `try_cast(priority_raw as integer)`: nó biến các nhãn hợp lệ sau schema evolution thành NULL, đồng thời chấp nhận các số ngoài contract như 0, 5 và -1. Contract đang tắt và chưa có test miền giá trị nên pipeline chạy thành công nhưng dữ liệu sai lọt vào Silver. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm số hợp lệ `'1'..'4'`: cast và giữ nguyên. Nhóm nhãn hợp lệ: `urgent→1`, `high→2`, `medium→3`, `low→4`. Nhóm lỗi `P1`, `P2`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng và NULL: trả về NULL làm tín hiệu đưa vào quarantine. |
| **Cách khắc phục** | Viết `CASE` dùng chung trong `dbt/macros/normalize_priority.sql`; ở `silver_tickets.sql` lọc bản ghi không chuẩn hóa được trước rồi mới `row_number`; ở `quarantine_tickets.sql` lấy các hàng macro trả NULL; trong `schema.yml` bật `contract.enforced: true`, thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | Sau sửa: `quarantine_tickets` đúng 312 hàng; `silver_tickets` giữ đủ 12.480 ticket; priority sạch; `dbt test` 11/11 pass; checksum quarantine ba lượt đều là `ebb89036fb`. |

Bronze nên giữ dữ liệu thô, kể cả bản ghi lỗi, để có bằng chứng điều tra, khả
năng replay và cơ hội áp dụng lại logic sau khi hiểu nguyên nhân. Việc chuẩn
hóa và định tuyến lỗi nên thực hiện ở Silver. Nếu Bronze từ chối dữ liệu, bản
gốc có thể mất và không còn đủ thông tin để khắc phục sự cố về sau.

Không nên dừng toàn bộ DAG vì 312 bản ghi lỗi khi hơn 130.000 event và 31.200
chunk hợp lệ vẫn cần được phục vụ. Quarantine cô lập phần lỗi để người vận hành
xử lý, trong khi pipeline tiếp tục cung cấp dữ liệu hợp lệ. Chỉ nên fail toàn
DAG khi lỗi cho thấy toàn bộ dataset hoặc schema đầu ra không còn đáng tin cậy.

---

## 4 · Bài mở rộng A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard chỉ cần một khách hàng trong một ngày nhưng phải scan 5.000.000 đơn vị công đọc trên 5.000 file Parquet; dataset thực chỉ có 130.683 hàng. |
| **Nguyên nhân** | 5.000 file nhỏ không partition khiến DuckDB phải mở mọi file và tính tối thiểu một block scan cho từng file. Đường dẫn không chứa cột lọc; đồng thời `strftime(event_time, ...)` bọc cột trong hàm nên predicate không sargable và không thể dùng partition pruning hoặc min/max statistics hiệu quả. |
| **Cách khắc phục** | `tools/compact.py` ghi lại dataset, partition theo `event_date` (14 giá trị), sắp `event_date, customer_name, event_time, event_id`, dùng row group 2.048. `queries/dashboard.sql` đọc dataset Hive partitioned và lọc trực tiếp `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | Rows scanned: **5.000.000 → 9.324**, giảm **536,3×**; files: **5.000 → 14**; rows on disk giữ nguyên **130.683 → 130.683**; result hash giữ nguyên `4379e4c5d9f3`. |

Không partition theo `customer_name` vì cột này có 650 giá trị, sẽ tạo quá
nhiều thư mục/file nhỏ và tái tạo chính vấn đề cần sửa. Partition theo 14 ngày
cho phép loại toàn bộ 13 ngày không liên quan từ đường dẫn. Trong từng ngày,
sắp theo khách hàng làm các hàng của ACME nằm gần nhau, còn row group 2.048
giúp thống kê min/max đủ hẹp để loại các nhóm khách hàng khác.

---

## 5 · Bài mở rộng B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | Consumer gốc bị kill ở batch 7 sau khi commit offset 3.500 nhưng trước khi ghi batch; restart chỉ còn 19.500/20.000 hàng, mất đúng 500 message. |
| **Nguyên nhân** | Offset được commit trước database write, tạo semantics at-most-once: broker xem batch đã xử lý dù side effect chưa xảy ra. Chỉ đảo thành ghi trước–commit sau sẽ tạo at-least-once, nhưng `INSERT` thuần lại tạo bản sao khi batch được replay. |
| **Cách khắc phục** | Đặt `event_id` làm primary key; ghi mỗi batch bằng một statement atomic `INSERT ... ON CONFLICT (event_id) DO UPDATE`; chỉ commit offset sau khi database write thành công. |
| **Bằng chứng** | Crash ở batch 7: offset còn 3.000 nên batch được đọc lại. Sau restart: **20.000 hàng / 20.000 event_id**, không mất, không trùng, `C == A`; `BÀI MỞ RỘNG B: ĐẠT ✓`. |

`DO NOTHING` chỉ chống trùng và giữ nội dung của lần ghi đầu. Nếu cùng
`event_id` được replay với payload đã sửa, thay đổi sẽ bị bỏ qua. `DO UPDATE`
vẫn idempotent theo khóa nhưng cập nhật các cột bằng payload mới nhất, phù hợp
với trường hợp nguồn sửa message hoặc quá trình replay dùng dữ liệu đã được
chuẩn hóa lại. Exactly-once không đến từ transport; kết quả thực tế đạt được
bằng at-least-once kết hợp một phép ghi idempotent.

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain và natural key, sau đó kiểm tra phép ghi có idempotent khi retry/backfill hay không. |
| 2 | So sánh event time với ingestion time và đo percentile độ trễ trước khi chọn watermark/lookback. |
| 3 | Kiểm tra cả kiểu dữ liệu lẫn miền giá trị, phân biệt schema evolution hợp lệ với dữ liệu hỏng và bảo toàn bản gốc trong Bronze. |
