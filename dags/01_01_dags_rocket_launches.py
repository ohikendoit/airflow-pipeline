import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exception
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#dag 객체 인스턴스 생성
dag = DAG(
    dag_id = "download_rocket_launches",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval = "@daily", #매일 자정에 실행
    start_date=dt.datetime(2023, 1, 1),
    end_date=dt.datetime(2023,12,31),
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag, #dag 변수에 대한 참조
)

def _get_pictures():
    #경로가 존재하는지 확인, 없으면 디렉터리 생성
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    #launches.json 파일에 있는 모든 그림 파일을 다운로드
    with open("/tmp/launches.json") as f: #이전 단계의 태스크 결과 확인, JSON파일 열기
        launches=json.load(f) #데이터를 섞을 수 있도록 딕셔너리로 읽기
        image_urls=[launch["image"] for launch in launches["results"]] #모든 발사에 대한 'image'의 URL값 읽기
        for image_url in image_urls:
            try:
                response=requests.get(image_url) #각각의 이미지 다운로드
                image_filename=image_url.split("/")[-1]
                target_file=f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content) #각각의 이미지 저장
                print(f"Download {image_url} to {target_file}") #로그에 저장하기 위해 stdout으로 출력
            except requests_exception.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exception.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures=PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures, #실행할 파이썬 함수를 저장
    dag=dag,
)

notify=BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

#task 실행 순서 정의
download_launches >> get_pictures >> notify
