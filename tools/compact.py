#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import shutil
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    if n_src == 0:
        raise SystemExit("không có dữ liệu nguồn; chạy seed/generate.py --extra trước")

    # DST là dataset sinh lại hoàn toàn. Xoá phiên bản cũ để chạy compact nhiều
    # lần vẫn cho đúng một layout, không giữ sót file từ lần chạy trước.
    data_root = DATA.resolve()
    dst_resolved = DST.resolve()
    if dst_resolved.parent != data_root:
        raise RuntimeError(f"đường dẫn đích không an toàn: {dst_resolved}")
    if DST.exists():
        shutil.rmtree(DST)

    src_glob = (SRC / "*.parquet").as_posix()
    dst_path = DST.as_posix()
    con.execute(f"""
        copy (
            select *
            from read_parquet('{src_glob}')
            order by event_date, customer_name, event_time, event_id
        ) to '{dst_path}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore,
            row_group_size 2048
        )
    """)

    dst_glob = (DST / "**" / "*.parquet").as_posix()
    n_old = con.execute(
        f"select count(*) from read_parquet('{src_glob}')"
    ).fetchone()[0]
    n_new = con.execute(
        f"select count(*) from read_parquet('{dst_glob}', hive_partitioning=true)"
    ).fetchone()[0]
    assert n_old == n_new, f"mất dữ liệu khi compact: {n_old:,} != {n_new:,}"

    n_dst = len(list(DST.rglob("*.parquet")))
    print(f"  đích  : {DST}  ({n_dst:,} file)")
    print(f"  số hàng: {n_old:,} -> {n_new:,} (không đổi)\n")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
