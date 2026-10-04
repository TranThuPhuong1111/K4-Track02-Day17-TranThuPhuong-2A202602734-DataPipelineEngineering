# B2 — Brainstorm: Flywheel dữ liệu cho chatbot CSKH tiếng Việt

## 1. Bài toán và ràng buộc thực

Nền tảng SaaS trong lab có một **chatbot CSKH** (agent RAG và tool call: tra ticket,
tra hoá đơn, đặt lại mật khẩu). Mỗi ngày có khoảng 20.000 hội thoại, khoảng 120.000 span
OpenTelemetry `gen_ai.*`. Mục tiêu là **mỗi tuần** tạo được từ traffic thật:

- một **eval set** đóng băng để đo mọi phiên bản model/prompt mới, và
- dữ liệu **SFT + cặp DPO** `(prompt, chosen, rejected)` để fine-tune (Ngày 22).

Vì sao khó:

- **Tín hiệu thưa và nhiễu.** Chỉ khoảng 3% hội thoại có 👍/👎, và 👎 thường là bực vì
  chính sách chứ không phải vì câu trả lời sai.
- **Model học từ output của chính nó.** Không cẩn thận thì flywheel thành vòng lặp tự
  đầu độc (model collapse).
- **Tiếng Việt lộn xộn.** Có dấu, không dấu, teencode ("ko dc"), trộn tiếng Anh ("SSO", "workspace").
- **Hội thoại chứa PII.** Tên, số điện thoại, email, số hợp đồng cần được xử lý theo
  Nghị định 13/2023 về bảo vệ dữ liệu cá nhân, kể cả **quyền yêu cầu xoá**.
- **Ngân sách nhỏ.** Không có đội annotator toàn thời gian.

## 2. Sơ đồ kiến trúc

```
 Chatbot (prod) ── OTel gen_ai.* spans ──▶ Kafka agent.traces ─┬─▶ Alert stream (Flink-lite / consumer)
 Ticket system ─── escalate / CSAT (CDC) ─┐                     │     tỉ lệ lỗi tool, 👎/giờ → PagerDuty
                                           ▼                     ▼
                              BRONZE (Parquet, bất biến, 1 file / nguồn / ngày)
                                           │  daily batch (cùng code path khi backfill)
                                           ▼
                SILVER: spans (MERGE theo span_id) · conversations (1 hàng / conv_id)
                        PII gate: regex + NER tiếng Việt → <NAME>/<PHONE>/…   ──▶ quarantine
                        erasure_requests (tombstone theo user_id)
                                           │
                ┌──────────────────────────┼──────────────────────────────┐
                ▼                          ▼                              ▼
     LLM judge (cache theo         label_candidates                eval_set vYYYY-Www
     hash + model + prompt_ver)    (👎, escalate, judge)           đóng băng, tách theo user + thời gian
                └────────────┬─────────────┘                              │
                             ▼                                            │
               GOLD: sft_set / dpo_pairs vYYYY-Www  ◀── fuzzy decontam ───┘
                             │   (n-gram trên text đã chuẩn hoá tiếng Việt)
                             ▼
            cổng duyệt thủ công ──▶ fine-tune job (side-effect không đảo ngược)
```

## 3. Năm câu hỏi then chốt

### Q2 — Batch hay streaming?

**Quyết định:** batch hằng ngày cho dữ liệu train/eval, cộng một luồng streaming rất
mỏng **chỉ để cảnh báo**.

**Đánh đổi:** độ tươi so với độ phức tạp. Fine-tune chạy hằng tuần, nên dữ liệu tươi
hơn một ngày cũng không làm model tốt hơn. Ngược lại, batch cho phép dựng lại mọi thứ
từ Bronze, backfill cùng code path và kiểm bằng checksum như lab. Riêng sự cố ("tool
tra hoá đơn lỗi 40% trong 15 phút") cần phát hiện tính bằng phút, nên chỉ phần đó
chạy streaming, và nó không ghi vào dataset.

**Phương án bị loại: Kappa toàn phần** (Kafka → Flink → feature store → dataset streaming).
- Phải xử lý state, exactly-once và late data trong Flink chỉ để đổi lấy độ tươi không ai dùng.
- Khó tái lập một version dataset, nên mất khả năng "chạy lại ba lần cùng checksum".
- Tốn người vận hành gấp nhiều lần so với một DAG daily.

### Q4 — Hợp đồng và chất lượng trước khi vào model

**Quyết định:** chặn ở hai chốt.
- **Bronze → Silver:** validate span bằng Pydantic (bắt buộc có `trace_id`, `span_id`,
  `gen_ai.request.model`, timestamp hợp lệ). Span sai vào quarantine, run không dừng
  (giống `quality.py`).
- **Silver → Gold:** chặn PII. Regex xử lý email/điện thoại/số hợp đồng; model NER tiếng
  Việt xử lý tên người và địa chỉ.

Cảnh báo khi tỉ lệ quarantine của một ngày vượt **3× trung vị 14 ngày**. Ngưỡng tương
đối tốt hơn ngưỡng tuyệt đối vì traffic dao động theo mùa.

**Đánh đổi:** NER chặt thì recall PII cao nhưng che nhầm tên sản phẩm ("Basic", "Pro"),
làm câu train kém tự nhiên. Tôi chấp nhận che thừa, vì **rò PII vào trọng số model là
không thể gỡ**, còn che thừa chỉ làm giảm nhẹ chất lượng dữ liệu. Đo bằng một tập vàng
300 hội thoại gán nhãn tay: recall tên phải ≥ 0,95, precision được phép thấp hơn.

### Q7 — Flywheel mà không tự đầu độc

**Quyết định:**
1. **Nguồn nhãn.**
   - `rejected` lấy từ hội thoại bị escalate sang người, hoặc 👎 mà LLM judge cũng chấm thấp.
   - `chosen` ưu tiên **câu trả lời của nhân viên CSKH** sau escalate (do người viết).
     Câu trả lời của bot chỉ được làm `chosen` khi có 👍 *và* judge đồng ý.
2. **Eval tách theo user và thời gian.** Eval set lấy từ user không bao giờ xuất hiện
   trong train và từ tuần *sau* cutoff train. Tách ngẫu nhiên theo lượt sẽ rò, vì cùng
   một user hỏi lại cùng câu.
3. **Decontamination mờ.** Chuẩn hoá tiếng Việt (bỏ dấu, `đ→d`, mở rộng teencode), rồi
   loại mọi prompt train có containment word 3-gram ≥ 0,5 với bất kỳ prompt eval nào.
   Prototype: `bonus/fuzzy_decontam.py`. Exact-match như `extensions/dataset.py` bỏ lọt
   2/3 bản viết lại.

**Đánh đổi:**
- Ngưỡng 0,5 loại nhầm một ít câu hợp lệ ngắn, nhưng giữ eval trung thực. Eval nói dối
  thì mọi quyết định ship sau đó đều sai.
- Embedding similarity bắt paraphrase tốt hơn nhưng tốn và khó giải thích. Tôi để nó
  làm bước thứ hai, chỉ chạy trên các cặp có containment 0,3–0,5.

### Q8 — Failure semantics: chạy lại, backfill, side-effect

**Quyết định:**
- Bronze bất biến theo (nguồn, ngày).
- Silver `MERGE` theo `span_id`/`conv_id`.
- Dataset là **snapshot có version** (`v2026-W40`) dựng lại từ Bronze "as of" cutoff,
  giống `gold_training_set`.
- LLM judge cache theo `hash(input) + model + prompt_version` như bonus B1, nên backfill
  một tháng không gọi lại judge cho trace đã chấm. Đổi prompt judge thì chấm lại có chủ đích.
- **Side-effect không đảo ngược** là gửi dataset cho job fine-tune hoặc vendor gán nhãn.
  Bước này nằm sau một cổng duyệt thủ công, kèm checksum dataset trong log.

**Đánh đổi:** cổng thủ công chậm thêm khoảng nửa ngày mỗi tuần. Đổi lại, một lỗi PII
hay leakage không thể tự động chảy vào model.

### Q10 — Bối cảnh Việt Nam: quyền xoá dữ liệu

**Quyết định:** bảng `erasure_requests(user_id, requested_at)` được join ở mọi lần dựng
dataset.
- Snapshot cũ chứa user đó bị **retire**: dựng version `-r1` không có user, rồi xoá vật
  lý bản cũ khỏi storage.
- Bronze dùng **crypto-shredding**: PII mã hoá bằng khoá riêng từng user, huỷ khoá là xoá.
- Model đã train trên dữ liệu bị xoá được ghi vào audit log và loại ở lần fine-tune kế tiếp.

**Đánh đổi:** mất tính bất biến tuyệt đối của snapshot. Tôi giữ *bất biến có kiểm soát*:
mọi thay đổi đều sinh version mới kèm lý do, không bao giờ sửa âm thầm. Ngoài ra, dữ
liệu được lưu tại region Việt Nam/Singapore, vì traffic quốc tế từ Việt Nam đắt và chập chờn.

## 4. Chi phí (ngắn)

Khoảng 80% chi phí là **LLM judge**. Ba cách cắt:
- Chỉ chấm hội thoại có tín hiệu (👍/👎, escalate), cộng **mẫu ngẫu nhiên 5%** để ước
  lượng chất lượng chung không thiên lệch.
- Cache theo hash (B1).
- Dùng model nhỏ cho judge, kiểm định lại bằng 300 mẫu gán nhãn tay mỗi quý.

Storage Parquet cho 120.000 span/ngày chỉ vài GB/tháng, không đáng kể.
