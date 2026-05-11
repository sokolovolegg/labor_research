import pandas as pd
import torch
import os
import numpy as np
from sklearn.model_selection import train_test_split
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments
)
import evaluate

# --- Paths Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PROC_DIR = os.path.join(BASE_DIR, "data", "processed")
TRAIN_FILE = os.path.join(PROC_DIR, "training_set.csv")
MODEL_DIR = os.path.join(BASE_DIR, "models", "domain_classifier")

print("1. Loading dataset...")
df = pd.read_csv(TRAIN_FILE, low_memory=False).dropna(subset=['description', 'label'])

# Class Mapping
labels = df['label'].unique().tolist()
label2id = {label: i for i, label in enumerate(labels)}
id2label = {i: label for label, i in label2id.items()}
df['label_id'] = df['label'].map(label2id)

print(f"Classes found: {labels}")

# Split into train/val (80/20)
train_texts, val_texts, train_labels, val_labels = train_test_split(
    df['description'].tolist(),
    df['label_id'].tolist(),
    test_size=0.2,
    random_state=42,
    stratify=df['label_id'].tolist()
)

print("2. Loading DistilBERT tokenizer...")
MODEL_NAME = "distilbert-base-multilingual-cased"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

print("3. Tokenizing (truncating to 256 tokens for speed)...")
train_encodings = tokenizer(train_texts, truncation=True, padding=True, max_length=256)
val_encodings = tokenizer(val_texts, truncation=True, padding=True, max_length=256)

class JobDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

train_dataset = JobDataset(train_encodings, train_labels)
val_dataset = JobDataset(val_encodings, val_labels)

print("4. Initializing model...")
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME,
    num_labels=len(labels),
    id2label=id2label,
    label2id=label2id
)

# Loading Accuracy metric
accuracy = evaluate.load("accuracy")

def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    predictions = np.argmax(predictions, axis=1)
    return accuracy.compute(predictions=predictions, references=labels)

# Training Configuration
training_args = TrainingArguments(
    output_dir=MODEL_DIR,
    learning_rate=2e-5,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    num_train_epochs=3,
    weight_decay=0.01,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    logging_steps=100, 
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    processing_class=tokenizer, 
    compute_metrics=compute_metrics,
)

print("5. Starting training! (This process may take some time)")
trainer.train()

print(f"6. Saving the best model to {MODEL_DIR}...")
trainer.save_model(MODEL_DIR)
tokenizer.save_pretrained(MODEL_DIR)
print("Done! The model has been trained and saved successfully.")