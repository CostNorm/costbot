import boto3
from datetime import datetime, timedelta
import pandas as pd
import json
import urllib3

# Cost Explorer 클라이언트 생성
ce = boto3.client("ce", region_name="us-east-1")

# S3 클라이언트 생성
s3_client = boto3.client("s3")

# Slack Webhook URL (너의 Webhook URL로 교체)
SLACK_WEBHOOK_URL = (
    "https://hooks.slack.com/services/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"
)


def send_slack_message(text):
    http = urllib3.PoolManager()

    slack_payload = {"text": text}

    encoded_payload = json.dumps(slack_payload).encode("utf-8")

    response = http.request(
        "POST",
        SLACK_WEBHOOK_URL,
        body=encoded_payload,
        headers={"Content-Type": "application/json"},
    )

    print(f"Slack 응답 코드: {response.status}")


def save_df_to_s3(df, bucket_name, file_name):
    # DataFrame을 CSV로 변환
    csv_buffer = df.to_csv(index=False)

    # S3에 업로드
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer)


def get_service_operation_cost():
    # 어제 날짜 계산
    yesterday = datetime.today() - timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%d")
    end_date = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 조회 기간: {start_date} ~ {end_date}")

    # Cost Explorer API 호출
    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start_date, "End": end_date},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
        GroupBy=[
            {"Type": "DIMENSION", "Key": "SERVICE"},
            {"Type": "DIMENSION", "Key": "OPERATION"},
        ],
    )

    # 결과 데이터 정리
    cost_data = []
    for day in response["ResultsByTime"]:
        for group in day["Groups"]:
            service_name = group["Keys"][0]
            operation = group["Keys"][1]
            cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
            cost_data.append(
                {
                    "Date": day["TimePeriod"]["Start"],
                    "Service": service_name,
                    "Operation": operation,
                    "Cost": cost,
                }
            )

    # Pandas로 데이터프레임 생성
    df = pd.DataFrame(cost_data)

    if df.empty:
        print("비용 데이터가 없습니다.")
        send_slack_message("🚨 어제 비용 데이터가 없습니다.")
        return

    # 비용 기준으로 정렬
    df_sorted = df.sort_values(by="Cost", ascending=False)

    # 파일 이름에 날짜 포맷 추가 (DDMMYY 형식)
    file_date = yesterday.strftime("%y%m%d")
    file_name = f"{file_date}_sorted_costs.csv"

    # S3에 저장
    save_df_to_s3(df_sorted, "day-by-day", file_name)

    # 비용 기준으로 정렬 후 상위 3개 추출
    top3 = df_sorted.head(3)

    # 메시지 포맷팅
    message = f"*💰 어제({start_date}) AWS 비용 리포트*\n\n"
    message += "💸 비용이 많이 발생한 상위 3개 리소스:\n"

    for index, row in top3.iterrows():
        message += f"- 서비스: {row['Service']}, 오퍼레이션: {row['Operation']}, 비용: ${row['Cost']:.2f}\n"

    # 출력 및 Slack 전송
    print(message)
    send_slack_message(message)


def lambda_handler(event, context):
    print("Lambda 실행 시작")
    get_service_operation_cost()
