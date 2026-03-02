import datetime
import os
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# --- CẤU HÌNH CHUNG ---
PROJECT_DIR = "/opt/airflow/project_scraper"
PYTHON_EXE = "python3"
SCRIPT_FILE = f"{PROJECT_DIR}/udemy_scraper.py"
LOGIN_SCRIPT = f"{PROJECT_DIR}/udemy_login_auto.py"

# Wrapper tạo màn hình ảo (Bắt buộc cho Playwright trong Docker)
XVFB_WRAPPER = "xvfb-run --auto-servernum"

# Lấy biến môi trường từ Container (Chứa UDEMY_EMAIL, PASS, AZURE_KEY...)
# Đặt IS_HEADLESS = false để chạy được với Xvfb
BASE_ENV = os.environ.copy()
BASE_ENV["IS_HEADLESS"] = "false"
BASE_ENV["PYTHONUNBUFFERED"] = "1"

# ==================================================
# DAG 1: DASHBOARD FULL RUN (PRODUCTION)
# ==================================================
with DAG(
    dag_id="udemy_dashboard_FULL_RUN",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["udemy", "dashboard", "production", "full"],
) as dag_full:

    # 1. Task Login (Chạy trước để lấy Cookie)
    login_full = BashOperator(
        task_id="prepare_login_full_run",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {LOGIN_SCRIPT}",
        env=BASE_ENV
    )

    # 2. Các Task Cào (Chạy song song sau khi login xong)
    # Group 1: Web Dev, Software Eng
    task_g1 = BashOperator(
        task_id="run_dashboard_group1_PROD",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group1",
        env=BASE_ENV
    )

    # Group 2: Prog Lang, DB, Testing, No-Code
    task_g2 = BashOperator(
        task_id="run_dashboard_group2_PROD",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group2",
        env=BASE_ENV
    )

    # Group 3: Data Science, Mobile, Game, Tools
    task_g3 = BashOperator(
        task_id="run_dashboard_group3_PROD",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group3",
        env=BASE_ENV
    )

    # Luồng: Login xong -> Chạy 3 nhóm cùng lúc
    login_full >> [task_g1, task_g2, task_g3]


# ==================================================
# DAG 2: DASHBOARD TEST (TEST LIMIT 2 PAGES)
# ==================================================
with DAG(
    dag_id="udemy_dashboard_TEST",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["udemy", "dashboard", "test"],
) as dag_test:

    # 1. Task Login
    # login_test = BashOperator(
    #     task_id="prepare_login_dashboard_test",
    #     bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {LOGIN_SCRIPT}",
    #     env=BASE_ENV
    # )

    # 2. Test Group 1
    task_g1_test = BashOperator(
        task_id="run_dashboard_group1_TEST",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group1 --test",
        env=BASE_ENV
    )
    
    # 3. Test Group 2 (Bỏ comment nếu muốn chạy)
    task_g2_test = BashOperator(
        task_id="run_dashboard_group2_TEST",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group2 --test",
        env=BASE_ENV
    )

    # 4. Test Group 3 (Bỏ comment nếu muốn chạy)
    task_g3_test = BashOperator(
        task_id="run_dashboard_group3_TEST",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job dashboard --group group3 --test",
        env=BASE_ENV
    )
    
    # Luồng: Login -> Chạy test các group
    # login_test >> [task_g1_test, task_g2_test, task_g3_test]
    [task_g1_test, task_g2_test, task_g3_test]


# ==================================================
# DAG 3: PRICE TRACKER DAILY (CHẠY HÀNG NGÀY)
# ==================================================
with DAG(
    dag_id="udemy_price_tracker_daily",
    start_date=datetime.datetime(2025, 1, 1),
    # Chạy lúc 23:00 mỗi ngày (Giờ Server Local UTC+7)
    schedule_interval="0 23 * * *", 
    catchup=False,
    tags=["udemy", "tracker", "daily"],
) as dag_tracker:

    # 1. Task Login
    # login_tracker = BashOperator(
    #     task_id="prepare_login_daily_tracker",
    #     bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {LOGIN_SCRIPT}",
    #     env=BASE_ENV
    # )

    # 2. Task Tracker (Chạy No-Code theo cấu hình trong udemy_scraper.py)
    run_tracker = BashOperator(
        task_id="run_price_tracker",
        bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} --job tracker",
        env=BASE_ENV
    )

    # Luồng: Login -> Tracker
    # login_tracker >> run_tracker
    run_tracker