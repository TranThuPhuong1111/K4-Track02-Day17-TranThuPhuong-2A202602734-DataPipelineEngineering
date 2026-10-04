# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Tran Thu Phuong / 2A202602734
**Repo:** https://github.com/TranThuPhuong1111/K4-Track02-Day17-TranThuPhuong-2A202602734-DataPipelineEngineering
**Commit bài nộp:** `b2d315a` (commit code cuối; commit kế tiếp chỉ thêm REPORT và checksums.txt)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc code, chạy verify/test, đề xuất ba bản sửa, code bonus B1, prototype và nháp DESIGN.md cho B2, soạn nháp REPORT; tôi đã review từng dòng sửa và chạy lại toàn bộ kiểm tra.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17; tài liệu Debezium (định dạng envelope `before`/`after`/`op`).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets`: 24 hàng cho 12 ticket; T-91 có 3 hàng. | Feature lệch full recompute (`c50b…` ≠ `8630…`); u05 ngày 08-12: (2, 0) thay vì (5, 1). | T-97 vẫn `is_deleted = false`, còn body; 1 hàng trong snapshot mới nhất, 2 chunk RAG. |
| **Nguyên nhân gốc** | `QUALIFY` chỉ dedup *trong* batch; giữa các batch là `INSERT` thuần, không khoá. | `LOOKBACK_DAYS = 0`: run 08-15 không tính lại ngày 08-12. | `ticket_id` lấy từ `after`, mà delete có `after = null` ⇒ bị lọc `ticket_id IS NOT NULL`. |
| **Cách sửa** | `silver.py`: `MERGE … ON ticket_id`, chỉ `UPDATE` khi `s._lsn > t._lsn`. | `config.py`: `LOOKBACK_DAYS = 3`. | `staging.py`: `coalesce(after, before, key).ticket_id`; cột khác vẫn từ `after` ⇒ PII null. |
| **Khái niệm** | Silver có khoá; MERGE + LSN guard. | Late data; event time; overwrite-partition. | CDC log-based; tombstone; xoá phải lan. |

## 2. Các con số

- Lateness đo từ Bronze (43 bản ghi): P50 `0.00`, P95 `2.90`, P99 `3.00` ngày → `LOOKBACK_DAYS = 3`. Baseline verify `8/18`.
- `submission/checksums.txt`: PASS — Gold checksum `39e115c510ecdf526800eac227158a4f`.
- `make parity`: PARITY.

## 3. Lựa chọn kỹ thuật

- **MERGE vs overwrite-partition:** ticket là thực thể đổi trạng thái nên cần upsert theo khoá, LSN quyết định bản nào mới; feature là tổng hợp theo ngày nên xoá và tính lại cả cửa sổ là idempotent đơn giản nhất.
- **Lookback = 3:** bằng ceil(P99) *đo* được. Nhỏ hơn thì mất event muộn như của u05; lớn hơn thì mỗi run tính lại thừa nhiều ngày.
- **Tombstone vs xoá hẳn:** giữ khoá và LSN để replay batch cũ không hồi sinh ticket, PII vẫn bị xoá. Đánh đổi: hàng tombstone nằm lại mãi, cần dọn định kỳ khi chắc không còn replay.
- **Snapshot "as of", không sửa bản cũ:** tái lập được thí nghiệm và không rò rỉ tương lai.
- **DuckDB/dbt thay Spark:** vài chục hàng/ngày vừa một máy; Spark chỉ thêm chi phí vận hành.

## 4. Hai câu hỏi suy ngẫm

1. Bất biến không được thắng quyền xoá. Ghi yêu cầu vào `erasure_requests`; dựng version mới không có ticket (vd. `v2026-08-12-r1`), retire và xoá vật lý bản cũ, kể cả ở Bronze (crypto-shredding: mã hoá PII theo khoá từng user, huỷ khoá là xoá); giữ audit log để truy vết; model đã train trên dữ liệu đó được xét lại ở lần train kế.
2. Thêm chốt NER tiếng Việt cạnh `mask_pii` ở bước Bronze → Silver, và một kiểm tra chặn trước khi Gold xuất sang classifier/RAG. Đo bằng tập vàng gán nhãn tay (recall theo loại PII), theo dõi tỉ lệ che mỗi batch, thêm contract vào `verify` như check email/phone.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell bằng các lệnh tương đương trong [SUBMISSION.md](../docs/SUBMISSION.md).

### Baseline (CP1): bản chưa sửa

```text
PS> python -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.'), (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')])
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build  (see submission/checksums.txt)

RESULT: 8/18 checks — FAILURES ABOVE

PS> python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 0
```

### Sau khi sửa ba lỗi

```text
PS> python -m scripts.verify
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

PS> python -m pytest
..................................                                       [100%]
34 passed in 4.32s

PS> python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> python main.py --land-only; cd dbt_project; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
08:01:31  Running with dbt=1.12.5
08:01:32  Registered adapter: duckdb=1.11.0
08:01:32  Unable to do partial parsing because saved manifest not found. Starting full parse.
08:01:35  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
08:01:35  
08:01:35  Concurrency: 1 threads (target='dev')
08:01:35  
08:01:35  1 of 19 START sql view model main.stg_events ................................... [RUN]
08:01:35  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
08:01:35  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
08:01:35  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
08:01:35  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
08:01:35  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.08s]
08:01:35  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
08:01:35  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.12s]
08:01:35  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
08:01:35  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.08s]
08:01:35  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
08:01:35  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
08:01:35  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
08:01:35  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
08:01:35  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
08:01:35  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
08:01:35  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
08:01:35  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
08:01:35  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
08:01:35  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
08:01:35  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
08:01:35  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
08:01:35  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
08:01:35  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
08:01:35  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
08:01:35  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
08:01:35  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
08:01:35  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
08:01:35  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
08:01:35  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
08:01:35  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
08:01:35  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
08:01:35  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:35  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
08:01:35  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.04s]
08:01:35  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
08:01:35  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:35  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
08:01:35  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:35  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
08:01:35  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:35  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
08:01:36  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:36  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
08:01:36  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
08:01:36  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.20s]
08:01:36  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
08:01:36  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
08:01:36  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
08:01:36  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
08:01:36  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
08:01:36  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
08:01:36  
08:01:36  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.01 seconds (1.01s).
08:01:36  
08:01:36  Completed successfully
08:01:36  
08:01:36  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

## Bonus

- **B1 — Bước LLM có cache:** `pipeline/llm_label.py`. Khoá cache `(sha256(input), model, prompt_version)`; mọi câu trả lời (kể cả sai schema) đều được cache nên chạy lại 0 lần gọi; `gold_ticket_labels` chỉ nhận nhãn hợp lệ, phần còn lại vào `llm_label_quarantine`; ước lượng token/chi phí trước khi gọi.
- **B2 — Brainstorm:** [`bonus/DESIGN.md`](../bonus/DESIGN.md) (flywheel chatbot CSKH) + prototype [`bonus/fuzzy_decontam.py`](../bonus/fuzzy_decontam.py).

```text
PS> python -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
