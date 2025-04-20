from kafka import KafkaConsumer
import json
import pandas as pd
import joblib
import xgboost as xgb
import warnings

warnings.filterwarnings("ignore")

# Load model and features
print("📦 Loading model and feature dataset...")
model = joblib.load("models/xgb_model.pkl")
df_features = pd.read_csv("data/features_merged.csv")
df_features.set_index(["user_id", "product_id"], inplace=True)

# Kafka consumer
consumer = KafkaConsumer(
    "reorder-stream",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Tracking stats
count = 0
reorder_preds = 0

print("🧠 Consumer running... Streaming predictions...\n")

for msg in consumer:
    user_id = msg.value.get("user_id")
    product_id = msg.value.get("product_id")
    count += 1

    try:
        row = df_features.loc[(user_id, product_id)]
        X = row[["last_order_number", "avg_cart_position", "order_count", "reorder_times", "reorder_ratio"]].values.reshape(1, -1)
        prob = model.predict_proba(X)[0][1]

        if prob > 0.8:
            reorder_preds += 1
            print(f"✅ HIGH reorder → user_id={user_id:<6} | product_id={product_id:<6} | prob={float(prob):.4f}")

        # Show progress every 100,000 rows
        if count % 100000 == 0:
            print(f"📊 Processed {count:,} rows | High Reorders: {reorder_preds:,}")

    except KeyError:
        continue  # silently skip missing feature rows
