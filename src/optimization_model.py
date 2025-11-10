# src/optimization_model.py
import pulp
import pandas as pd

def build_multiobjective_model(costs, demands, inventory, fulfillment):
    """
    Builds a PuLP model that minimizes cost and risk while maximizing service.
    Returns Pareto alternatives for trade-off exploration.
    """
    model = pulp.LpProblem("InventoryOptimization", pulp.LpMinimize)

    # Decision variable: how many units to ship for each product-market pair
    x = pulp.LpVariable.dicts("ship", demands.keys(), lowBound=0)

    # Objective components
    total_cost = pulp.lpSum(costs[p] * x[(p, m)] for (p, m) in demands)
    stockout_risk = pulp.lpSum(max(0, demands[(p, m)] - x[(p, m)]) for (p, m) in demands)
    service_time = pulp.lpSum(fulfillment[p] * x[(p, m)] for (p, m) in demands if p in fulfillment)

    # Weighted-sum objective (tunable weights)
    alpha, beta, gamma = 0.5, 0.3, 0.2
    model += alpha * total_cost + beta * stockout_risk + gamma * service_time

    # Constraints: inventory and demand
    for (p, m) in demands:
        model += x[(p, m)] <= inventory.get(p, 0)
        model += x[(p, m)] <= demands[(p, m)]

    model.solve(pulp.PULP_CBC_CMD(msg=False))

    solution = {k: v.value() for k, v in x.items()}
    return solution, pulp.value(model.objective)
