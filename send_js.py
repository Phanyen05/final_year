import time
import random
import uuid
from datetime import datetime
import json
from confluent_kafka import Producer
# topic = 'final_year'
# conf = {'bootstrap.servers': 'localhost:9092'}
# producer = Producer(conf)


def fake_data():

    t = random.randint(0, 100000000000)
    phone = "0" + str(random.randint(900000000, 999999999))
    cmp = random.choice(["mcredit", "fecredit", "drdong"])
    z = {
        "_id": str(uuid.uuid4().hex),
        "qualityClick": random.choice([True, False]),
        "updateAt": random.randint(1686989768, 1705479557),
        "clickDate": random.randint(1686989768, 1705479557),
        "postbackDate": random.randint(1686989768, 1705479557),
        "transactionDate": list(range(1686989768, 1705479557)),
        "device": random.choice(["IOS", "ANDROID", "UNKNOWN"]),
        "city": random.choice(["Hà Nội", "HCM", "HP", "ĐN"]),
        "country": random.choice(["VN", "KR", "CN", "JP"]),
        "phoneNumber": phone,
        "trafficSource": random.choice(["facebook", "tiktok", "youtube", "twitter"]),
        "pubCode": str(random.randint(0, 1000)),
        "pubName": random.choice(["dongbq", "datnt", "truongnt", "anhah"]),
        "contractCode": str(random.randint(0, 1000)),
        "campaignName": cmp,
        "campaignCode": cmp,
        "utmSource": random.choice(["mvt", "mbf"]),
        "utmMedium": random.choice(["vay", "mo_the"]),
        "utmCampaign": cmp,
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
        "advCode": random.choice(["123", "456", "789"]),
    }
    return z



# Send JSON data through Kafka
for _ in range(2):
    data = fake_data()
    json_data = json.dumps(data)
    print(json_data)
    # producer.produce(topic, json_data.encode('utf-8'))
# producer.flush()
# Close the producer connection
# producer.close()

print("Data sent successfully!")