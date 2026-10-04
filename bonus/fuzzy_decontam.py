"""B2 prototype — fuzzy decontamination for the support-chatbot flywheel.

    python bonus/fuzzy_decontam.py

extensions/dataset.py::decontaminate only drops a training prompt when it is an
EXACT copy (after lowercase + whitespace) of an eval prompt. Real users reword:
"không đăng nhập được sso" vs "Ko dang nhap duoc bang SSO cong ty". Those
paraphrases leak the eval set into training and the eval score lies.

This prototype adds the one decision DESIGN.md argues for:
  1. normalise Vietnamese text: Unicode NFC -> strip diacritics -> đ->d ->
     lowercase -> keep letters/digits only -> expand common teencode
  2. word 3-gram containment: |grams(train) ∩ grams(eval)| / |grams(train)|
  3. drop the training prompt if containment >= THRESHOLD against ANY eval prompt

Zero-key, standard library only.
"""
from __future__ import annotations

import re
import unicodedata

N = 3
THRESHOLD = 0.5
TEENCODE = {"ko": "khong", "k": "khong", "dc": "duoc", "j": "gi", "mk": "minh", "e": "em"}


def normalize(text: str) -> list[str]:
    text = unicodedata.normalize("NFD", text.replace("đ", "d").replace("Đ", "D"))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    words = re.findall(r"[a-z0-9]+", text.lower())
    return [TEENCODE.get(w, w) for w in words]


def grams(text: str, n: int = N) -> set[tuple[str, ...]]:
    w = normalize(text)
    if len(w) < n:
        return {tuple(w)} if w else set()
    return {tuple(w[i:i + n]) for i in range(len(w) - n + 1)}


def containment(train: str, eval_: str) -> float:
    g = grams(train)
    return len(g & grams(eval_)) / len(g) if g else 0.0


def exact_decontaminate(train: list[str], evals: list[str]) -> list[str]:
    held = {" ".join(t.lower().split()) for t in evals}
    return [t for t in train if " ".join(t.lower().split()) not in held]


def fuzzy_decontaminate(train: list[str], evals: list[str]) -> tuple[list[str], list[tuple]]:
    kept, dropped = [], []
    for t in train:
        score, match = max(((containment(t, e), e) for e in evals), default=(0.0, None))
        if score >= THRESHOLD:
            dropped.append((t, score, match))
        else:
            kept.append(t)
    return kept, dropped


EVAL_PROMPTS = [
    "Tôi không đăng nhập được bằng SSO công ty",
    "Làm sao để xuất hoá đơn VAT cho tháng trước?",
    "Chatbot trả lời sai giờ làm việc của chi nhánh",
]

TRAIN_PROMPTS = [
    "Tôi không đăng nhập được bằng SSO công ty",            # exact copy
    "ko dang nhap dc bang SSO cong ty, giup voi",            # no diacritics + teencode
    "Làm sao xuất hoá đơn VAT cho tháng trước vậy ạ",        # light rewording
    "Đổi gói từ Basic lên Pro thì tính tiền thế nào?",       # genuinely new
    "Mời thêm thành viên vào workspace ở đâu?",              # genuinely new
    "Tôi muốn huỷ đăng ký nhận email quảng cáo",             # new, shares few words
]


def main() -> int:
    exact_kept = exact_decontaminate(TRAIN_PROMPTS, EVAL_PROMPTS)
    fuzzy_kept, dropped = fuzzy_decontaminate(TRAIN_PROMPTS, EVAL_PROMPTS)

    print(f"=== fuzzy decontamination (word {N}-gram containment >= {THRESHOLD}) ===")
    print(f"  training prompts           : {len(TRAIN_PROMPTS)}")
    print(f"  kept by exact-match        : {len(exact_kept)}  "
          f"(dropped {len(TRAIN_PROMPTS) - len(exact_kept)})")
    print(f"  kept by fuzzy (this proto) : {len(fuzzy_kept)}  (dropped {len(dropped)})")
    print("\n  dropped as eval leakage:")
    for t, score, e in dropped:
        print(f"    {score:.2f}  {t!r}\n          ~ eval {e!r}")
    print("\n  kept for training:")
    for t in fuzzy_kept:
        print(f"    {t!r}")

    leaked_by_exact = [t for t in exact_kept if t not in fuzzy_kept]
    ok = len(leaked_by_exact) == 2 and len(fuzzy_kept) == 3
    print(f"\n  paraphrases exact-match would have leaked: {len(leaked_by_exact)}")
    print("RESULT: " + ("OK" if ok else "UNEXPECTED"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
