import pandas as pd
import pickle
import os

# =======================================================================
# !!! CRITICAL CHANGE: SET YOUR ABSOLUTE PATH HERE !!!
# Replace the placeholder with your actual path, e.g., r"C:\Users\msrid\OneDrive\Desktop\optiGuide\data"
BASE_DATA_DIR = r"C:\Users\pc\Optiguide\data" 
# =======================================================================

# --- Helper Function for Cleaning Columns ---
def clean_column_names(df):
    """Strips whitespace from column names and replaces special characters."""
    df.columns = df.columns.str.strip()
    return df

# --- 1. Define File Paths ---
# Use os.path.join for robust, OS-independent path construction
ORDERS_FILE = os.path.join(BASE_DATA_DIR, 'orders_and_shipments.csv')
INVENTORY_FILE = os.path.join(BASE_DATA_DIR, 'inventory.csv')
FULFILLMENT_FILE = os.path.join(BASE_DATA_DIR, 'fulfillment.csv')

PROCESSED_PATH = os.path.join(BASE_DATA_DIR, 'processed')

# Check if the processed folder exists, create if not
os.makedirs(PROCESSED_PATH, exist_ok=True)


# --- 2. Load Data and Clean Columns ---
print("Loading source data...")
try:
    orders_df = pd.read_csv(ORDERS_FILE)
    inventory_df = pd.read_csv(INVENTORY_FILE)
    fulfillment_df = pd.read_csv(FULFILLMENT_FILE)

    # CRITICAL FIX: Clean column names immediately after loading
    orders_df = clean_column_names(orders_df)
    inventory_df = clean_column_names(inventory_df)
    fulfillment_df = clean_column_names(fulfillment_df)

except FileNotFoundError as e:
    print(f"Error loading file: {e}")
    print(f"Please confirm your CSV files are in this exact location: {BASE_DATA_DIR}")
    exit()

# --- 3. Define Core Optimization Sets ---
print("Defining optimization sets...")

# P: Set of Products
P_PRODUCTS = set(orders_df['Product Name'].unique())

# W: Set of Warehouse Countries (Supply Locations)
W_WAREHOUSES = set(orders_df['Warehouse Country'].unique())

# M: Set of Customer Markets/Regions (Demand Locations)
M_MARKETS = set(orders_df['Customer Market'].unique())

# T: Set of Shipment Modes (Logistics Options)
T_SHIP_MODES = set(orders_df['Shipment Mode'].unique())


# --- 4. Create Parameter Dictionaries ---

# P1: Average Profit per Product (This line now relies on the cleaned column name 'Profit')
print("Calculating profit and demand parameters...")
product_profit_df = orders_df.groupby('Product Name')['Profit'].mean().reset_index()
D_PROFIT_PER_PRODUCT = product_profit_df.set_index('Product Name')['Profit'].to_dict()

# P2: Inventory Cost Per Unit
inventory_cost_df = inventory_df.groupby('Product Name')['Inventory Cost Per Unit'].mean().reset_index()
D_INVENTORY_COST = inventory_cost_df.set_index('Product Name')['Inventory Cost Per Unit'].to_dict()

# P3: Average Fulfillment Time
print("Calculating fulfillment time parameter...")
fulfillment_time_df = fulfillment_df.groupby('Product Name')['Warehouse Order Fulfillment (days)'].mean().reset_index()
D_FULFILLMENT_TIME = fulfillment_time_df.set_index('Product Name')['Warehouse Order Fulfillment (days)'].to_dict()

# P4: Historical Demand
demand_df = orders_df.groupby(['Product Name', 'Customer Market'])['Order Quantity'].sum().reset_index()
D_HISTORICAL_DEMAND = demand_df.set_index(['Product Name', 'Customer Market'])['Order Quantity'].to_dict()


# --- 5. Save Sets and Parameters to Processed Folder ---
print("Saving sets and parameters...")

# Save Sets
SETS = {
    'P_PRODUCTS': P_PRODUCTS,
    'W_WAREHOUSES': W_WAREHOUSES,
    'M_MARKETS': M_MARKETS,
    'T_SHIP_MODES': T_SHIP_MODES
}
output_sets_file = os.path.join(PROCESSED_PATH, 'optimization_sets.pkl')
with open(output_sets_file, 'wb') as f:
    pickle.dump(SETS, f)
print(f"Saved sets to {output_sets_file}")

# Save Parameters
PARAMETERS = {
    'D_PROFIT_PER_PRODUCT': D_PROFIT_PER_PRODUCT,
    'D_INVENTORY_COST': D_INVENTORY_COST,
    'D_FULFILLMENT_TIME': D_FULFILLMENT_TIME,
    'D_HISTORICAL_DEMAND': D_HISTORICAL_DEMAND
}
output_params_file = os.path.join(PROCESSED_PATH, 'parameter_dicts.pkl')
with open(output_params_file, 'wb') as f:
    pickle.dump(PARAMETERS, f)
print(f"Saved parameters to {output_params_file}")

print("\nData preparation complete. The 'data/processed' folder is now created and populated.")
