# src/feedback_loop.py
def update_weights_from_feedback(weights, feedback_text):
    if "reduce cost" in feedback_text.lower():
        weights['cost'] *= 1.1
    if "improve delivery" in feedback_text.lower():
        weights['time'] *= 0.9
    return weights
