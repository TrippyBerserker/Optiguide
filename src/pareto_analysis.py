# src/pareto_analysis.py
import matplotlib.pyplot as plt
import pandas as pd

def plot_pareto(results_df):
    plt.scatter(results_df['Cost'], results_df['DeliveryTime'], c=results_df['StockoutRisk'])
    plt.xlabel("Cost")
    plt.ylabel("Delivery Time")
    plt.title("Pareto Frontier: Cost vs Delivery Time")
    plt.colorbar(label="Stockout Risk")
    plt.show()
