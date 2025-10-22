import os
import json
import pickle
import random
import pandas as pd
from datetime import datetime

# =======================================================================
# Configuration
# =======================================================================
BASE_DATA_DIR = r"C:\Users\pc\Optiguide\data"
PROCESSED_DIR = os.path.join(BASE_DATA_DIR, "processed")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "fine_tuning_data.jsonl")

# Random seed for reproducibility
random.seed(42)

# =======================================================================
# Load processed data
# =======================================================================
def load_pickle(filename):
    with open(os.path.join(PROCESSED_DIR, filename), "rb") as f:
        return pickle.load(f)

print("🔹 Loading processed optimization data...")
SETS = load_pickle("optimization_sets.pkl")
PARAMS = load_pickle("parameter_dicts.pkl")

P_PRODUCTS = list(SETS["P_PRODUCTS"])
M_MARKETS = list(SETS["M_MARKETS"])
T_SHIP_MODES = list(SETS["T_SHIP_MODES"])
W_WAREHOUSES = list(SETS["W_WAREHOUSES"])

D_PROFIT = PARAMS["D_PROFIT_PER_PRODUCT"]
D_INV_COST = PARAMS["D_INVENTORY_COST"]
D_FULFILL = PARAMS["D_FULFILLMENT_TIME"]
D_DEMAND = PARAMS["D_HISTORICAL_DEMAND"]

print(f"✅ Loaded {len(P_PRODUCTS)} products, {len(M_MARKETS)} markets.")

# =======================================================================
# Helper functions for generating synthetic Q–A pairs
# =======================================================================

def get_fulfillment_question(product):
    return f"What is the average fulfillment time for {product}?"

def get_profit_question(product):
    return f"What is the profit per unit for {product}?"

def get_demand_question(product, market):
    return f"What is the historical demand for {product} in the {market} market?"

def get_inventory_cost_question(product):
    return f"What is the average inventory cost per unit for {product}?"

def generate_response(question):
    """
    Smart lookup for answers using parameter dictionaries.
    """
    product = None
    market = None
    for p in P_PRODUCTS:
        if p in question:
            product = p
            break
    for m in M_MARKETS:
        if m in question:
            market = m
            break

    if "fulfillment" in question.lower() and product in D_FULFILL:
        return f"The average fulfillment time for {product} is {D_FULFILL[product]:.1f} days."

    if "profit" in question.lower() and product in D_PROFIT:
        return f"The profit per unit for {product} is ${D_PROFIT[product]:.2f}."

    if "inventory" in question.lower() and product in D_INV_COST:
        return f"The average inventory cost per unit for {product} is ${D_INV_COST[product]:.2f}."

    if "demand" in question.lower() and (product, market) in D_DEMAND:
        return f"The historical demand for {product} in {market} is {D_DEMAND[(product, market)]} units."

    return "I'm sorry, I don't have enough data to answer that question."

# =======================================================================
# Generate fine-tuning dataset
# =======================================================================

def generate_training_data(n_samples_per_type=25):
    data = []

    # Fulfillment questions
    for product in random.sample(P_PRODUCTS, min(n_samples_per_type, len(P_PRODUCTS))):
        q = get_fulfillment_question(product)
        a = generate_response(q)
        data.append({"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}]})

    # Profit questions
    for product in random.sample(P_PRODUCTS, min(n_samples_per_type, len(P_PRODUCTS))):
        q = get_profit_question(product)
        a = generate_response(q)
        data.append({"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}]})

    # Inventory cost questions
    for product in random.sample(P_PRODUCTS, min(n_samples_per_type, len(P_PRODUCTS))):
        q = get_inventory_cost_question(product)
        a = generate_response(q)
        data.append({"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}]})

    # Demand questions
    for (product, market) in random.sample(list(D_DEMAND.keys()), min(n_samples_per_type, len(D_DEMAND))):
        q = get_demand_question(product, market)
        a = generate_response(q)
        data.append({"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}]})

    return data

print("🧠 Generating fine-tuning dataset...")
training_data = generate_training_data(n_samples_per_type=40)

# =======================================================================
# Save dataset
# =======================================================================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for example in training_data:
        f.write(json.dumps(example) + "\n")

print(f"✅ Fine-tuning dataset created at: {OUTPUT_FILE}")
print(f"📊 Total samples: {len(training_data)}")

# =======================================================================
# (Optional) Kick off OpenAI fine-tuning (if API key is set)
# =======================================================================
# Uncomment below if you have an OpenAI fine-tuning account:
"""
from openai import OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

print('🚀 Uploading training file...')
response = client.files.create(
    file=open(OUTPUT_FILE, "rb"),
    purpose="fine-tune"
)
file_id = response.id

print('✅ Starting fine-tuning job...')
fine_tune = client.fine_tuning.jobs.create(
    training_file=file_id,
    model="gpt-4o-mini",  # or your base model
)
print(f"Fine-tune job started: {fine_tune.id}")
"""
# =======================================================================
# Convert to LLaMA-style JSONL (prompt/completion format)
# =======================================================================
LLAMA_FILE = os.path.join(PROCESSED_DIR, "fine_tuning_data_llama.jsonl")

print("🔄 Converting dataset to LLaMA prompt-completion format...")
with open(LLAMA_FILE, "w", encoding="utf-8") as f_out:
    for ex in training_data:
        user_msg = ex["messages"][0]["content"]
        assistant_msg = ex["messages"][1]["content"]
        llama_format = {"prompt": user_msg, "completion": assistant_msg}
        f_out.write(json.dumps(llama_format) + "\n")

print(f"✅ LLaMA fine-tuning file created: {LLAMA_FILE}")
