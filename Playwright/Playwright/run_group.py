"""
File: run_group.py (ĐÃ SỬA LỖI)

Chạy theo group:
- group1: Web Development + Software Engineering
- group2: Programming Languages + Database Design & Development + Software Testing + No-Code Development
- group3: Data Science + Mobile Development + Game Development + Software Development Tools

Cách dùng (cmd):
    python run_group.py --group group1 --job dashboard
    python run_group.py --group group2 --job dashboard
    python run_group.py --group group3 --job dashboard
    python run_group.py --group group2 --job tracker
"""

import os
import sys
import subprocess
import argparse

GROUP_DEFINITION = {
    "group1": [
        "Web Development"
        ,"Software Engineering",
],
    "group2": [
        "Programming Languages",
        "Database Design & Development",
        "Software Testing",
        "No-Code Development",
    ],
    "group3": [
        "Data Science",
        "Mobile Development",
        "Game Development",
        "Software Development Tools",
    ],
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--group",
        type=str,
        choices=list(GROUP_DEFINITION.keys()),
        required=True,
        help="group1, group2 hoặc group3",
    )
    parser.add_argument(
        "--job",
        type=str,
        choices=["dashboard", "tracker"],
        default="dashboard",
        help="dashboard = full crawl, tracker = price-only",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Chạy test (giới hạn ít URL)",
    )
    args = parser.parse_args()

    categories = GROUP_DEFINITION[args.group]
    print(f"[INFO] Run {args.job} cho {args.group}: {categories}")

    # Biến môi trường này không cần thiết vì script chính
    # đọc tham số --group
    env = os.environ.copy()
    # env["GROUP_CATEGORIES"] = ",".join(categories) # <-- (KHÔNG CẦN NỮA)
    env.setdefault("IS_HEADLESS", "true")

    cmd = [sys.executable, "udemy_scraper.py", "--job", args.job]
    
    # --- [SỬA LỖI] ---
    # Thêm 3 dòng này để truyền tham số --group
    if args.group:
        cmd.append("--group")
        cmd.append(args.group)
    # --- [HẾT SỬA LỖI] ---

    if args.test:
        cmd.append("--test")

    print(f"[INFO] Đang chạy lệnh: {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)

    if result.returncode != 0:
        print(f"[ERROR] udemy_scraper.py exit code {result.returncode}")
        sys.exit(result.returncode)

    print("[OK] Hoàn thành.")


if __name__ == "__main__":
    main()