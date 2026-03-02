import datetime
import os
from airflow.models import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project_scraper"
PYTHON_EXE = "python3"
SCRIPT_FILE = f"{PROJECT_DIR}/udemy_scraper.py"
LOGIN_SCRIPT = f"{PROJECT_DIR}/udemy_login_auto.py"
XVFB_WRAPPER = "xvfb-run --auto-servernum"

BASE_ENV = os.environ.copy()
BASE_ENV["IS_HEADLESS"] = "false"

with DAG(
    dag_id="TEST_Price_Tracker_NoCode",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["test", "tracker", "debug"],
) as dag:

    # 1. Login
    # task_login = BashOperator(
    #     task_id="prepare_login_for_tracker_test",
    #     bash_command=f"cd {PROJECT_DIR} && {XVFB_WRAPPER} {PYTHON_EXE} {LOGIN_SCRIPT}",
    #     env=BASE_ENV
    # )

    # 2. Test Tracker (Chạy thử 2 trang đầu của No-Code)
    task_test_tracker = BashOperator(
        task_id="run_tracker_test",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{XVFB_WRAPPER} {PYTHON_EXE} {SCRIPT_FILE} "
            f"--job tracker --test"
        ),
        env=BASE_ENV
    )

    # task_login >> task_test_tracker
    task_test_tracker