import json
import boto3
from datetime import datetime

s3 = boto3.client("s3")

BUCKET = "cloud-analytics-project"
PREFIX = "raw/orders_api/"

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    try:
        record = {
            "product_id": str(body["product_id"]),
            "product_name": str(body["product_name"]),
            "category": str(body["category"]),
            "price": float(body["price"]),
            "review_score": float(body["review_score"]),
            "review_count": int(body["review_count"]),
        }

        # sales_month_1 to sales_month_12
        for i in range(1, 13):
            key = f"sales_month_{i}"
            record[key] = int(body.get(key, 0))

        record["ingestion_timestamp"] = datetime.utcnow().isoformat()

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    filename = f"order_api_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=PREFIX + filename,
        Body=json.dumps(record),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Order ingested successfully",
            "s3_path": f"s3://{BUCKET}/{PREFIX}{filename}"
        })
    }
