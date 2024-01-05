import os
import random
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta

def create_folder(path):
    """Tạo thư mục nếu nó chưa tồn tại"""
    if not os.path.exists(path):
        os.makedirs(path)

def random_date(start, end):
    """Tạo ngày giờ ngẫu nhiên trong một khoảng thời gian"""
    start_time = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    time_diff = end_time - start_time
    random_time = start_time + timedelta(seconds=random.randint(0, int(time_diff.total_seconds())))
    return random_time.isoformat()

# Tạo danh sách các bản ghi giả
data = []
start_date = datetime(2023, 6, 1)
end_date = datetime.now()

while start_date <= end_date:
    for i in range(24):
        current_datetime = start_date + timedelta(hours=i)
        record = {
            "_id": str(random.randint(0, 1000000)),
            "transaction_ID": random_date("2023-06-01 00:00:00", current_datetime.strftime("%Y-%m-%d %H:%M:%S")),
            "qualityClick": random.choice([True, False]),
            "clickDate": random_date("2023-06-01 00:00:00", current_datetime.strftime("%Y-%m-%d %H:%M:%S")),
            "postbackDate": random_date("2023-06-01 00:00:00", current_datetime.strftime("%Y-%m-%d %H:%M:%S")),
            "device": random.choice(["IOS", "ANDROID", "UNKNOWN"]),
            "city": random.choice(["Hà Nội", "HCM", "HP", "ĐN"]),
            "country": random.choice(["VN", "KR", "CN", "JP"]),
            "phoneNumber": str(random.randint(1000000000, 9999999999)),
            "trafficSource": random.choice(["facebook", "tiktok", "youtube", "twitter"]),
            "pubCode": str(random.randint(0, 1000)),
            "pubName": random.choice(["dongbq", "datnt", "truongnt", "anhah"]),
            "contractCode": str(random.randint(0, 1000)),
            "campaignName": str(random.randint(0, 1000)),
            "campaignCode": str(random.randint(0, 1000)),
            "utmSource": random.choice(["mvt", "mbf"]),
            "utmMedium": random.choice(["vay", "mo_the"]),
            "utmCampaign": str(random.randint(0, 1000)),
            "utmTerm": "dg",
            "sub": "Infor1",
            "sub1": "Infor1",
            "sub2": "Infor1",
            "sub3": "Infor1",
            "sub4": "Infor1",
            "status": random.choice(["Pending", "Approved", "Reject", "Pre-approved"]),
            "quantity": random.randint(1, 100),
            "commission": random.choice([20000, 30000, 40000, 50000]),
            "amount": random.choice([200000, 300000, 400000, 500000]),
            "currencyUnit": random.choice(["VND", "USD"]),
            "advCode": random.choice(["123", "456", "789"])
        }
        data.append(record)

    start_date += timedelta(days=1)

# Chuyển đổi danh sách thành DataFrame
df = pd.DataFrame(data)

# Chuyển đổi kiểu dữ liệu của cột 'transaction_ID' sang kiểu datetime
df['transaction_ID'] = pd.to_datetime(df['transaction_ID'])

# Tạo thư mục theo cấu trúc đã chỉ định và lưu trữ dữ liệu dưới định dạng Parquet
start_date = datetime(2023, 6, 1).date()
end_date = datetime.now().date()

while start_date <= end_date:
    year_folder = start_date.strftime("%Y")
    month_folder = start_date.strftime("%m")
    day_folder = start_date.strftime("%d")
    folder_path = os.path.join(year_folder, month_folder, day_folder)
    # Lọcvà lưu trữ dữ liệu trong tệp Parquet
    filtered_df = df[(df['transaction_ID'].dt.date == start_date)]
    file_name = "data_affiliate.parquet"
    file_path = os.path.join(folder_path, file_name)

    table = pa.Table.from_pandas(filtered_df)
    pq.write_table(table, file_path)

    start_date += timedelta(days=1)