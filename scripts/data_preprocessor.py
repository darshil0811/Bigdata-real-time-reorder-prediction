# scripts/data_preprocessor.py

import pandas as pd

def prepare_features(orders, prior, train, products):
    print("🔗 Merging prior orders with order info...")
    prior_merged = pd.merge(prior, orders, on='order_id', how='left')

    print("🧠 Creating user-product features from prior orders...")
    features = prior_merged.groupby(['user_id', 'product_id']).agg({
        'order_number': 'max',
        'add_to_cart_order': ['mean', 'count'],
        'reordered': 'sum'
    }).reset_index()

    features.columns = [
        'user_id', 'product_id',
        'last_order_number',
        'avg_cart_position',
        'order_count',
        'reorder_times'
    ]

    features['reorder_ratio'] = features['reorder_times'] / features['order_count']
    features = features.merge(products[['product_id', 'product_name']], on='product_id', how='left')

    print("🏷️ Generating 0/1 labels from train users...")

    # ✅ Get train users (people whose next order we want to predict)
    train_user_orders = pd.merge(train, orders[['order_id', 'user_id']], on='order_id', how='left')
    train_user_ids = train_user_orders['user_id'].unique()

    # ✅ All user-product pairs for these users from prior orders
    train_features = features[features['user_id'].isin(train_user_ids)]

    # ✅ Get actual reordered items in their next order
    reordered_items = train_user_orders[['user_id', 'product_id']]
    reordered_items['reordered'] = 1

    # ✅ Merge to assign 0 or 1
    final_df = pd.merge(train_features, reordered_items, on=['user_id', 'product_id'], how='left')
    final_df['reordered'] = final_df['reordered'].fillna(0).astype(int)

    print(f"✅ Final ML dataset: {final_df.shape[0]:,} rows, {final_df.shape[1]} columns")
    print("🔢 Label distribution:")
    print(final_df['reordered'].value_counts())

    return final_df
