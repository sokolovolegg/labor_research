import pandas as pd
import torch
import os
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# --- Path Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
MODEL_DIR = os.path.join(BASE_DIR, "models", "domain_classifier")

INPUT_FILE = os.path.join(DATA_DIR, "MASTER_DATASET_CLEAN.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "MASTER_DATASET_LABELED.csv")

def run_classification():
    # 1. Check if model exists
    if not os.path.exists(MODEL_DIR):
        print(f"Error: Model not found at {MODEL_DIR}. Please extract your model first.")
        return

    # 2. Load model and tokenizer from local folder
    print("Loading the trained model...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_DIR)
    
    # Move model to GPU if available
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()

    # 3. Load dataset
    print(f"Loading dataset: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    
    # Fill empty descriptions to avoid errors
    df['description'] = df['description'].fillna("No description provided")
    descriptions = df['description'].tolist()
    
    # 4. Processing in batches
    batch_size = 32
    all_predictions = []
    
    print(f"Starting classification on {len(df)} rows using {device}...")
    
    with torch.no_grad():
        for i in tqdm(range(0, len(descriptions), batch_size)):
            batch_texts = descriptions[i : i + batch_size]
            
            # Tokenization
            inputs = tokenizer(
                batch_texts, 
                padding=True, 
                truncation=True, 
                max_length=128, 
                return_tensors="pt"
            ).to(device)
            
            
            if "token_type_ids" in inputs:
                del inputs["token_type_ids"]
            
            
            # Forward pass
            outputs = model(**inputs)
            logits = outputs.logits
            
            # Get predicted class ID
            predictions = torch.argmax(logits, dim=-1).cpu().numpy()
            
            # Convert IDs back to labels
            batch_labels = [model.config.id2label[p] for p in predictions]
            all_predictions.extend(batch_labels)

    # 5. Save results
    df['domain_label'] = all_predictions
    print(f"Saving results to: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Classification completed successfully!")

if __name__ == "__main__":
    run_classification()