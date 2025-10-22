# src/llm_explainer.py
def explain_tradeoffs(llm, results_df):
    prompt = (
        "You are an AI supply chain analyst.\n"
        "Given the following optimization results, explain trade-offs:\n"
        f"{results_df.to_string(index=False)}\n"
        "Summarize how cost, stockout risk, and delivery time interact."
    )
    explanation = llm.generate(prompt)
    return explanation
