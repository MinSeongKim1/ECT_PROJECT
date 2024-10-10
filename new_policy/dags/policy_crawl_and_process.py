from __future__ import annotations

import datetime
import pendulum
import requests
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import time

# CSV 파일 다운로드 함수
def download_csv():
    url = "https://climateactiontracker.org/documents/1219/08032024_CountryAssessmentData_no_capita-fixed.csv"
    download_dir = "/home/airflow/airflow/downloads"  # 실제 Airflow 서버나 시스템에서 접근 가능한 경로로 변경
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # 파일명을 날짜 기반으로 설정
    file_name = f"CountryAssessmentData_{time.strftime('%Y%m%d')}.csv"
    file_path = os.path.join(download_dir, file_name)

    # CSV 파일 다운로드
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"CSV 파일 {file_name}이(가) 다운로드되었습니다.")

# 크롤링 함수 (new_pol_03.py 내용 통합)
def run_crawling():
    # 크롤링 스크립트를 여기에서 실행 (new_pol_03.py 경로 수정)
    exec(open('/home/airflow/airflow/scripts/new_pol_03.py').read())  # 경로를 실제 크롤링 스크립트 경로로 변경
    print("크롤링 완료")

# DAG 정의
with DAG(
    dag_id="policy_crawl_and_process",
    schedule="0 0 1 * *",  # 매달 1일 0시에 실행
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=100),
    tags=["policy", "crawl", "monthly"],
) as dag:
    
    # 마지막에 실행할 빈 작업
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # 크롤링 작업
    crawl_task = PythonOperator(
        task_id="run_crawling_task",
        python_callable=run_crawling,
    )

    # CSV 파일 다운로드 작업
    download_csv_task = PythonOperator(
        task_id="download_csv_task",
        python_callable=download_csv,
    )

    # 작업 순서 정의
    crawl_task >> download_csv_task >> run_this_last

if __name__ == "__main__":
    dag.test()
