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
import pycld2 as cld2

def is_english(text):
  # Keep your existing length check to save processing time
    if not text or len(text.strip()) < 10:
        return False

    try:
        # pycld2.detect returns a tuple: (is_reliable, text_bytes_found, details)
        # 'details' contains the top 3 detected languages, e.g., (('ENGLISH', 'en', 99, 100.0), ...)
        is_reliable, _, details = cld2.detect(text)

        # Check if the most likely language (index 0) is English ('en')
        return details[0][1] == 'en'

    except Exception:
        # pycld2 occasionally throws an error if it encounters pure garbage characters
        # or invalid UTF-8 bytes. If it can't parse it, we safely assume it's not English.
        return False

def filter_english(input_path, output_path):
    print("Filtering English for datasets")

    # exist_ok=True handles the directory creation, but fallback to '.' if dirname is empty
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

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
                    # Write the raw line directly instead of re-serializing!
                    fout.write(line)
                    saved += 1

                if count % 50000 == 0:
                    print(f"Processed {count}.. Saved {saved} English records")

            except (json.JSONDecodeError, ValueError):
                continue

    print(f"Done! Processed {count} total records. Saved {saved} English-only records.")
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 00_makeEnglish.py <input_json> <output_json>")
        sys.exit(1)

    in_file = sys.argv[1]
    out_file = sys.argv[2]

    with open(out_file, 'w') as fp:
        pass

    if (os.path.exists(in_file) & os.path.exists(out_file)):
        print(f"Removing non-English characters from {in_file} and saving to {out_file}.")
        filter_english(in_file, out_file)

        print(f"Removed non-English characters. Saved to {out_file}")
    else:
        print(f"Files not found.")
