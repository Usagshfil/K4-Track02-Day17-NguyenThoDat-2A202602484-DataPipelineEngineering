# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Thọ Đạt / 2A202602484
**Repo:** https://github.com/Usagshfil/K4-Track02-Day17-NguyenThoDat-2A202602484-DataPipelineEngineering.git
**Commit bài nộp:** (điền git commit SHA sau khi bạn commit)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity (Gemini 3.8 Flash) hỗ trợ phân tích nguyên nhân lỗi, hướng dẫn giải thích khái niệm slide và rà soát cú pháp truy vấn.
**Nguồn tham khảo khác (nếu có):** Slide K4 Track 02 Day 17 Data Pipeline Engineering, Debezium docs, dbt Microbatch docs.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets has exactly one row per ticket_id` fail (24 rows for 12 tickets); T-91 có 3 trạng thái đồng thời; `rerun3` fail do replay batch cũ ghi đè batch mới. | `gold_feature_daily reconciles with full recompute` fail (`c50b8851affe` != `8630e04a61d1`); u05 ngày 08-12 chỉ có 2 events thay vì 5; `LOOKBACK_DAYS=0 < 3`. | `deleted ticket T-97 is a tombstone` fail (T-97 còn nguyên PII); T-97 vẫn sót trong training snapshot v2026-08-16 và RAG doc chunks (9 chunks thay vì 8). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dùng lệnh `INSERT` nối đuôi; thiếu cơ chế dedup/upsert theo khoá và không có LSN guard bảo vệ thứ tự thời gian. | `LOOKBACK_DAYS = 0` nên partition ngày 08-12 không được tính lại khi batch 08-15 chứa event trễ của u05 cập cảng. | Staging chỉ trích xuất `after->>'ticket_id'`; khi `op='d'`, `after=null` khiến dòng bị rớt; Silver không nhận được sự kiện xoá để tạo tombstone. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Thực hiện DELETE-INSERT theo `ticket_id` với điều kiện LSN guard (`c._lsn >= t._lsn` mới thay thế; chặn batch cũ ghi đè). | `pipeline/config.py`: Đặt `LOOKBACK_DAYS = 3` (dựa trên kết quả đo `ceil(P99) = 3` ngày từ Bronze). | `pipeline/staging.py`: `coalesce` lấy `ticket_id` từ `after`, `before` hoặc `key`. `pipeline/silver.py`: khi `op='d'`, set `is_deleted=True` và mask toàn bộ PII thành `NULL`. |
| **Khái niệm trên slide** | Silver — Có khoá, Keyed Upsert, LSN guard, Idempotent pipeline. | Event time vs Ingest time, Dữ liệu về muộn (Late data), Lookback window, Overwrite-partition. | CDC log-based (Debezium envelope), Xoá phải lan (Delete propagation), Tombstone, Point-in-time snapshot. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver biểu diễn trạng thái thực thể hiện tại nên cần MERGE theo entity key, trong khi Gold feature tổ chức theo chuỗi thời gian (event_date) nên overwrite-partition xử lý dữ liệu đến muộn hiệu quả và tự nhiên hơn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ lại dòng với cờ tombstone và LSN giúp duy trì tính bất biến của lịch sử CDC, ngăn chặn việc replay batch cũ vô tình hồi sinh lại thực thể đã bị xoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính tái lập (reproducibility) trong ML; model huấn luyện tại thời điểm quá khứ phải nhìn thấy chính xác dữ liệu như thời điểm đó để tránh data leakage.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Kích thước dữ liệu vừa vặn trong bộ nhớ đơn máy, DuckDB cho tốc độ xử lý vector hóa cực nhanh không overhead phân tán, kết hợp dbt giúp quản trị data contract và microbatch dễ dàng mà không cần cụm máy chủ cồng kềnh.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?**
   - *Trả lời*: Đây là mâu thuẫn cốt lõi giữa Tính bất biến của ML (Reproducibility/Auditability) và Luật bảo vệ dữ liệu (GDPR "Right to be forgotten" / Nghị định 13/2023/NĐ-CP). Giải pháp thực tế trong Data Platform:
     1. Tách biệt dữ liệu định danh (PII) và thuộc tính học máy: Lưu PII trong một khoá giải mã (Crypto-shredding). Khi có yêu cầu xoá, ta huỷ khoá mã hoá của người dùng đó; toàn bộ dữ liệu văn bản trong các snapshot lịch sử sẽ biến thành chuỗi vô nghĩa không thể khôi phục mà không làm xáo trộn cấu trúc các partition snapshot cũ.
     2. Gắn nhãn trạng thái snapshot: Các snapshot cũ chứa dữ liệu đã xoá được chuyển sang trạng thái "quarantine/deprecated" chỉ dùng cho audit nội bộ với quyền hạn bị cô lập nghiêm ngặt, cấm không cho các pipeline retrain mô hình mới truy cập. Nếu bắt buộc phải purge triệt để theo lệnh pháp lý, hệ thống sẽ chạy quy trình "compaction & restatement" để sinh ra phiên bản snapshot chỉnh sửa có gắn tag lineage rõ ràng.

2. **Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?**
   - *Trả lời*:
     1. *Đặt chốt ở tầng nào:* Cần đặt chốt kép (Defense-in-depth). Chốt 1 tại tầng **Staging ➔ Silver** (lọc cơ bản và gán cờ rủi ro trước khi dữ liệu mở rộng cho phân tích). Chốt 2 tại cổng **Silver ➔ Gold (Training/RAG)** để đảm bảo an toàn tuyệt đối trước khi nạp vào vector DB hoặc huấn luyện mô hình.
     2. *Công cụ sử dụng:* Thay vì chỉ dùng Regex tĩnh, sử dụng mô hình **NER (Named Entity Recognition)** chuyên cho tiếng Việt (như PhoBERT-NER / spaCy / Microsoft Presidio) để nhận diện thực thể Tên người (PER), Địa chỉ (LOC), Tổ chức (ORG).
     3. *Đo lường:* Xây dựng một benchmark dataset nội bộ được gán nhãn thủ công (Gold test set) để đo lường định kỳ **Precision, Recall và F1-score** của pipeline phát hiện PII. Trong môi trường production, giám sát tỷ lệ % số văn bản bị che và tỷ lệ false-positive / false-negative thông qua cơ chế human-in-the-loop review ngẫu nhiên 1% traffic.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                                                  [100%]
34 passed in 3.29s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
08:32:52  Running with dbt=1.12.5
08:32:52  Registered adapter: duckdb=1.11.0
08:32:53  Unable to do partial parsing because saved manifest not found. Starting full parse.
08:32:54  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
08:32:54  
08:32:54  Concurrency: 1 threads (target='dev')
08:32:54  
08:32:54  1 of 19 START sql view model main.stg_events ................................... [RUN]
08:32:55  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.16s]
08:32:55  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
08:32:55  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
08:32:55  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
08:32:55  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
08:32:55  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
08:32:55  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.14s]
08:32:55  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
08:32:55  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.11s]
08:32:55  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
08:32:55  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
08:32:55  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
08:32:55  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
08:32:55  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
08:32:55  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
08:32:55  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
08:32:55  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
08:32:55  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
08:32:55  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
08:32:55  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
08:32:55  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
08:32:55  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
08:32:55  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
08:32:55  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
08:32:55  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
08:32:55  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
08:32:55  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
08:32:55  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
08:32:55  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
08:32:55  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
08:32:55  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
08:32:55  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.07s]
08:32:55  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
08:32:55  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
08:32:55  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
08:32:55  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
08:32:55  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
08:32:55  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
08:32:56  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
08:32:56  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.33s]
08:32:56  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
08:32:56  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
08:32:56  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
08:32:56  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
08:32:56  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
08:32:56  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
08:32:56  
08:32:56  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.43 seconds (1.43s).
08:32:56  
08:32:56  Completed successfully
08:32:56  
08:32:56  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree