import datetime
import os
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# --- CẤU HÌNH ---
PROJECT_DIR = "/opt/airflow/project_scraper"
PYTHON_EXE = "python3"
SCRIPT_FILE = f"{PROJECT_DIR}/udemy_scraper.py"
LOGIN_SCRIPT = f"{PROJECT_DIR}/udemy_login_auto.py"
XVFB_WRAPPER = "xvfb-run --auto-servernum"

# Biến môi trường
BASE_ENV = os.environ.copy()
BASE_ENV["IS_HEADLESS"] = "false"
BASE_ENV["PYTHONUNBUFFERED"] = "1"

# [CẤU HÌNH TEST]
# Thay vì chạy cả Group 3, ta chỉ chạy đúng 1 Category này
TARGET_CATEGORY = "Data Science"
START_PAGE = 287

with DAG(
    dag_id="TEST_DataScience_Page287",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "debug", "custom"],
) as dag:

    # 1. Task Login (Vẫn giữ để đảm bảo có cookie)
    # task_login = BashOperator(
    #     task_id="prepare_login_custom",
    #     bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {LOGIN_SCRIPT}",
    #     env=BASE_ENV
    # )

    # 2. Task Scrape (Đã sửa lệnh bash)
    task_scrape = BashOperator(
        task_id="run_scraper_custom_page",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} "
            f"--job dashboard "
            # Thay --group bằng --category (Cần code v36 để hiểu)
            f"--category '{TARGET_CATEGORY}' "
            f"--start-page {START_PAGE} "
            f"--test"
        ),
        env=BASE_ENV
    )

    # Luồng
    # task_login >> task_scrape
    task_scrape