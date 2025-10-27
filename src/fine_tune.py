# OptiGuide/src/fine_tune.py
"""
Fine-tuning Phi-3-Mini using LoRA (Windows + RTX 4050 optimized)
"""

import os
from datasets import load_dataset
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    TrainingArguments,
    Trainer,
    BitsAndBytesConfig,
)
from peft import (
    LoraConfig,
    get_peft_model,
    prepare_model_for_kbit_training,
)

# --- Configuration ---
MODEL_NAME = "microsoft/Phi-3-mini-4k-instruct"
DATA_PATH = r"C:\Users\RITHWIK DIDIGAM\OneDrive\Desktop\supplyChain\Optiguide\data\processed\fine_tuning_data.jsonl"
OUT_DIR = os.path.join("models", "phi3-lora-optiguide")


def main():
    # === 1️⃣ Tokenizer ===
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # === 2️⃣ Model in 8-bit ===
    bnb_cfg = BitsAndBytesConfig(load_in_8bit=True, llm_int8_threshold=6.0)

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        quantization_config=bnb_cfg,
        device_map="auto",
    )

    model = prepare_model_for_kbit_training(model)

    # ✅ Correct LoRA target modules for Phi-3
    lora_cfg = LoraConfig(
        r=8,
        lora_alpha=16,
        target_modules=[
            "self_attn.qkv_proj",
            "self_attn.o_proj",
            "mlp.gate_up_proj",
            "mlp.down_proj",
        ],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora_cfg)

    # === 3️⃣ Load dataset ===
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"❌ Dataset not found at {DATA_PATH}")

    ds = load_dataset("json", data_files=DATA_PATH, split="train")

    # === 4️⃣ Prepare text ===
    def to_text(example):
        text = ""
        for m in example["messages"]:
            text += f"{m['role']}: {m['content']}\n"
        example["text"] = text + "assistant:"
        return example

    ds = ds.map(to_text)

    # === 5️⃣ Tokenize with labels ===
    def tokenize_fn(batch):
        tokens = tokenizer(
            batch["text"],
            truncation=True,
            padding="max_length",
            max_length=512,
        )
        tokens["labels"] = tokens["input_ids"].copy()
        return tokens

    ds_tok = ds.map(tokenize_fn, batched=True).shuffle(seed=42)

    # === 6️⃣ Training arguments ===
    args = TrainingArguments(
        output_dir=OUT_DIR,
        per_device_train_batch_size=2,  # lower for 8GB GPU
        gradient_accumulation_steps=8,
        learning_rate=2e-4,
        num_train_epochs=2,
        fp16=True,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=1,
        report_to="none",
    )

    # === 7️⃣ Trainer ===
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=ds_tok,
        tokenizer=tokenizer,
    )

    print("🚀 Starting fine-tuning...")
    trainer.train()

    # === 8️⃣ Save ===
    os.makedirs(OUT_DIR, exist_ok=True)
    model.save_pretrained(OUT_DIR)
    tokenizer.save_pretrained(OUT_DIR)
    print(f"✅ Fine-tuning complete! Model saved to {OUT_DIR}")


if __name__ == "__main__":
    main()
