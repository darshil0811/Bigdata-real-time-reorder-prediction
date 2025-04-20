# Real-Time Reorder Prediction System 🛒⚡

This project demonstrates a real-time streaming ML pipeline using Kafka + XGBoost to predict product reorders for grocery shoppers (Instacart 32M+ dataset).

### 🔥 Features
- Kafka-based event stream (32M+ rows)
- Real-time inference with XGBoost
- Consumer with prediction logging + high-confidence alerts
- Big Data ready with S3 integration (optional)

### 🚀 How it works
1. Kafka Producer streams `user_id + product_id` from 32M-row CSV
2. Kafka Consumer loads `xgb_model.pkl` + features
3. Live prediction printed and logged for reorders

### 🛠 Tech Stack
- Python
- Kafka
- pandas / XGBoost / joblib
- VS Code + GitHub

### 📁 Folder Structure
