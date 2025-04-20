# scripts/train_model.py

import os
import joblib
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

def train_and_save_model(df, save_dir="models"):
    os.makedirs(save_dir, exist_ok=True)

    # Prepare features and label
    X = df.drop(columns=["reordered", "user_id", "product_id", "product_name"])
    y = df["reordered"]

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    print("🚀 Training XGBoost model...")
    model = xgb.XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        use_label_encoder=False,
        n_jobs=-1,
        verbosity=1
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    print(f"\n✅ Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print("📊 Classification Report:\n", classification_report(y_test, y_pred))

    # Save model
    model_path = os.path.join(save_dir, "xgb_model.pkl")
    joblib.dump(model, model_path)
    print(f"💾 Model saved to {model_path}")
