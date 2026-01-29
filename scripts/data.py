import json
import gzip
import os
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth as mlxtend_fpgrowth
from mlxtend.frequent_patterns import association_rules
import sys
import gc
from fim import fpgrowth
import langid


MAX_SHELVES = 5000

def is_english(text):
    #Returns true if lanaguage detected is English 
    
    if not text or len(text.strip()) < 10:
        return False
    lang, _ = langid.classify(text)
    return lang == 'en'

def filter_english(input_path, output_path):
    print(f"Filtering English for datasets")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    count = 0 
    saved = 0 

    open_func = gzip.open if input_path.endswith('.gz') else open 

    with open_func(input_path, 'rt', encoding='utf-8') as fin, \
            open(output_path, 'w', encoding='utf-8') as fout:

                for line in fin:
                    try:
                        record = json.loads(line)
                        count += 1 

                        texts_to_check = record.get('review_text', record.get('title', ''))

                        if is_english(texts_to_check):
                            fout.write(json.dumps(record) + '\n')
                            saved += 1 
                        if count % 50000 == 0:
                            print(f"processed {count}..Saved {saved} English records")
                    except(json.JSONDecodeError, ValueError):
                        continue
    print(f"Done! Processed {count} total records. Saved {saved} English-only records.")

def extract_shelf_transactions(input_path, output_path, min_count=10):
    """
    Extracts 'popular_shelves' from the metadata to create a transaction dataset.
    Streams line-by-line to keep memory usage low.
    """
    print(f"Extracting transactions from {input_path}...")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    book_count = 0
    # Generic tags that add noise to discovery patterns
    generic_tags = {
        'to-read', 'currently-reading', 'owned', 'favorites', 
        'all-time-favorites', 'books-i-own', 'read-in-2017', 
        'read-in-2016', 'default', 'ebook', 'kindle', 'audiobook',
        'my-books', 'library', 'wish-list', 'maybe', 'finish', 'read'
    }
    
    open_func = gzip.open if input_path.endswith('.gz') else open
    
    with open_func(input_path, 'rt', encoding='utf-8') as fin, \
         open(output_path, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            try:
                book = json.loads(line)
                
                # Filter tags based on community frequency
                shelves = [
                    s['name'].replace(',', ' ').strip() 
                    for s in book.get('popular_shelves', []) 
                    if int(s['count']) >= min_count
                ]
                
                # Remove administrative/utility tags
                filtered_shelves = [s for s in shelves if s.lower() not in generic_tags]
                
                # Only save transactions with at least 2 relevant tags
                if len(filtered_shelves) > 1:
                    fout.write(",".join(filtered_shelves) + '\n')
                    book_count += 1
                    
                if book_count % 100000 == 0 and book_count > 0:
                    print(f"Streamed {book_count} books...")
            except (json.JSONDecodeError, ValueError):
                continue

    print(f"Done! Saved {book_count} transactions to {output_path}.")
def run_association_mining(transaction_file, min_support=0.01, min_threshold=1.2):
    """
    Loads transactions and runs FP-Growth using Sparse DataFrames to save memory.
    """
    print(f"Loading transactions from {transaction_file} into memory...")
    
    with open(transaction_file, 'r', encoding='utf-8') as f:
        dataset = [line.strip().split(',') for line in f if line.strip()]
    
    if not dataset:
        print("No transactions found.")
        return None, None

    print(f"Encoding {len(dataset)} transactions into a Sparse Matrix...")
    te = TransactionEncoder()
    # te_ary becomes a sparse scipy matrix instead of a dense numpy array
    te_ary = te.fit(dataset).transform(dataset, sparse=True)
      # Create a Sparse DataFrame - this uses significantly less RAM
    df = pd.DataFrame.sparse.from_spmatrix(te_ary, columns=te.columns_)
    
    # Delete the original dataset and trigger garbage collection immediately
    del dataset
    gc.collect()

    print(f"Running FP-Growth (min_support={min_support})...")
    # FP-Growth is more memory efficient than Apriori for large datasets
   
    frequent_itemsets = mlxtend_fpgrowth(
    df,
    min_support=min_support,
    use_colnames=True,
    max_len=3
    )
    
    if frequent_itemsets.empty:
        print("No frequent itemsets found. Try lowering min_support.")
        return None, None

    print("Generating association rules...")
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=min_threshold)
    rules = rules.sort_values(by="lift", ascending=False)
    
    return frequent_itemsets, rules


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python extract_shelves.py <input_json> <temp_csv> <output_csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    transaction_file = sys.argv[2]
    output_path = sys.argv[3]

    if os.path.exists(input_file):

        filter_english(sys.argv[1], sys.argv[2])
        # 1. Extraction (Streams through file, uses very little RAM)
        extract_shelf_transactions(input_file, transaction_file, min_count=15)
        
        # 2. Mining (Now uses Sparse DataFrames)
        # Note: If it still crashes, increase min_support (e.g., 0.05)
        itemsets, rules = run_association_mining(transaction_file, min_support=0.08, min_threshold=1.5)
        
        if rules is not None:
            print("\n--- Top 10 Discovered Patterns (by Lift) ---")
            print(rules[['antecedents', 'consequents', 'lift', 'support']].head(10))
            rules.to_csv(output_path, index=False)
            print(f"Results saved to {output_path}")
    else:
        print(f"Error: {input_file} not found.") 
