import boto3
from datetime import datetime, timedelta, timezone
import pandas as pd
import json
import urllib3
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# Cost Explorer 클라이언트 생성
ce = boto3.client("ce", region_name="us-east-1")

# S3 클라이언트 생성
s3_client = boto3.client("s3")

# Slack API 토큰과 채널 ID를 환경 변수에서 가져오기
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")
SLACK_API_URL = "https://slack.com/api/chat.postMessage"

# S3 버킷 이름을 환경 변수에서 가져오기
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")


def send_slack_message(text, thread_ts=None):
    http = urllib3.PoolManager()

    slack_payload = {
        "token": SLACK_API_TOKEN,
        "channel": SLACK_CHANNEL_ID,
        "text": text,
    }

    if thread_ts:
        slack_payload["thread_ts"] = thread_ts

    encoded_payload = json.dumps(slack_payload).encode("utf-8")

    response = http.request(
        "POST",
        SLACK_API_URL,
        body=encoded_payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SLACK_API_TOKEN}",
        },
    )

    print(f"Slack response code: {response.status}")

    # JSON 응답 파싱하여 ts 값 반환
    response_data = json.loads(response.data.decode("utf-8"))
    if response_data.get("ok"):
        return response_data.get("ts")
    else:
        print(f"Slack API 오류: {response_data.get('error')}")
        return None


def save_df_to_s3(df, bucket_name, file_name):
    # DataFrame을 CSV로 변환
    csv_buffer = df.to_csv(index=False)

    # S3에 업로드
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer)


def get_service_operation_cost():
    # 어제 날짜 계산 (UTC 기준)
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%d")
    end_date = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 Query period (UTC): {start_date} ~ {end_date}")

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
        print("No cost data available.")
        send_slack_message("🚨 No cost data available for yesterday.")
        return

    # 총 비용 계산
    total_cost = df["Cost"].sum()

    # 비용 기준으로 정렬
    df_sorted = df.sort_values(by="Cost", ascending=False)

    # 파일 이름에 날짜 포맷 추가 (DDMMYY 형식)
    file_date = yesterday.strftime("%y%m%d")
    file_name = f"{file_date}_sorted_costs.csv"

    # S3에 저장
    save_df_to_s3(df_sorted, S3_BUCKET_NAME, file_name)

    # 비용 기준으로 정렬 후 상위 3개 추출
    top3 = df_sorted.head(3)

    # 메시지 포맷팅
    message = f"*💰 Yesterday's({start_date}) AWS Cost Report (UTC)*\n\n"
    message += f"💵 Total Cost: ${total_cost:.2f}\n\n"
    message += "💸 Top 3 resources with highest costs:\n"

    for index, row in top3.iterrows():
        message += f"- Service: {row['Service']}, Operation: {row['Operation']}, Cost: ${row['Cost']:.2f}\n"

    # 출력 및 Slack 전송
    print(message)
    # 메인 메시지 전송 및 thread_ts 받기
    thread_ts = send_slack_message(message)

    if thread_ts:
        # 상위 30개 리소스를 스레드로 전송
        top30 = df_sorted.head(30)
        thread_message = "💡 Top 30 resources:\n"
        for index, row in top30.iterrows():
            thread_message += f"- Service: {row['Service']}, Operation: {row['Operation']}, Cost: ${row['Cost']:.2f}\n"

        send_slack_message(thread_message, thread_ts=thread_ts)


def lambda_handler(event, context):
    print("Lambda execution started")
    get_service_operation_cost()
