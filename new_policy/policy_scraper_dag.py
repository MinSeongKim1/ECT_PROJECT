"""
파일명: policy_scraper_dag.py
설명: Airflow DAG을 사용하여 new_pol_03.py 스크립트를 주기적으로 실행하는 워크플로우 정의.
작성자: 김민성
작성일: 2024-12-06
사용 방법: Airflow 환경에 이 DAG 파일을 추가하고, 스크립트의 절대 경로를 확인하여 실행 준비.
          이 DAG은 매일 자정(한국 표준시)에 실행되도록 스케줄링됩니다.
특이사항:
 - new_pol_03.py 스크립트는 /opt/airflow/dags/scripts 디렉토리에 위치해야 합니다.
 - Airflow는 PythonOperator를 사용하여 스크립트를 실행합니다.
 - 실패 시 작업은 최대 2회 재시도되며, 재시도 간격은 5분입니다.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import pendulum  # 타임존 설정을 위한 라이브러리

# 한국 표준시 타임존 설정
kst = pendulum.timezone("Asia/Seoul")

# 기본 설정 (DAG에 적용될 기본 인자 정의)
default_args = {
    'owner': 'airflow',  # 소유자 정보
    'depends_on_past': False,  # 이전 실행 상태와 관계없이 실행
    'email_on_failure': False,  # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,  # 재시도 시 이메일 알림 비활성화
    'retries': 2,  # 작업 실패 시 최대 2회 재시도
    'retry_delay': timedelta(minutes=5),  # 재시도 간격: 5분
}

# DAG 정의
with DAG(
    'policy_scraper_dag',  # DAG 이름
    default_args=default_args,  # 기본 설정 적용
    description='A DAG to run the new_pol_03.py scraper script',  # DAG 설명
    schedule_interval='00 00 * * *',  # 매일 자정(KST)에 실행
    start_date=datetime(2024, 12, 1, tzinfo=kst),  # 시작 날짜 및 타임존 설정
    catchup=False,  # 과거 실행 방지
) as dag:

    # Python 스크립트를 실행하는 함수 정의
    def run_policy_scraper():
        # subprocess를 사용하여 외부 Python 스크립트 실행
        result = subprocess.run(
            ['python3', '/opt/airflow/dags/scripts/new_pol_03.py'],  # 실행할 스크립트의 경로
            capture_output=True, text=True  # 출력 캡처 및 텍스트 모드 활성화
        )
        if result.returncode != 0:  # 실행 실패 시
            # 에러 메시지를 포함한 예외 발생
            raise Exception(f"Script failed with error: {result.stderr}")
        # 실행 결과 출력
        print(result.stdout)

    # PythonOperator로 작업 정의
    run_scraper = PythonOperator(
        task_id='run_policy_scraper',  # 작업 ID (DAG 내 고유)
        python_callable=run_policy_scraper,  # 호출할 함수
    )

    # DAG 실행 순서 정의 (여기서는 단일 작업만 실행)
    run_scraper
