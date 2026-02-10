import json
import gzip
import os
import sys
from langdetect import detect, LangDetectException
import nltk
from nltk.corpus import words
from numpy import rec 
nltk.download('punkt_tab')
nltk.download('words')

ENGLISH_VOCAB = set(w.lower() for w in words.words())
def remove_nonenglish(review_dataset_gz, review_dataset_english, min_len=2):
    cleaned_dataset = []
    open_func = gzip.open if review_dataset_gz.endswith('.gz') else open
    
    with open_func(review_dataset_gz, 'rt', encoding   ='utf-8') as fin, \
         open(review_dataset_english, 'w', encoding='utf-8') as fout:
          
        fout.write("[\n]")
        is_first = True

        for line in fin:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            original_text = record.get('review_text', "")
            tokens = nltk.word_tokenize(original_text)
           # Keep token if it's in the English dictionary or is punctuation/number
            cleaned_tokens = [
                t for t in tokens 
                if (len(t) >= min_len and t.lower() in ENGLISH_VOCAB) or not t.isalpha()
            ]
            record['review_text'] = ' '.join(cleaned_tokens)

            # 4. Stream directly to the new file
            if not is_first:
                fout.write(",\n")
            
            json.dump(record, fout)
            is_first = False

        # Close JSON array format
        fout.write("\n]")
    


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python filter_reviews.py <reviews_dataset.json> <output_dataset.json>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]


    if os.path.exists(input_file):
        print(f"Removing non-English from reviews and storing in a temp file {output_file}")    
        remove_nonenglish(input_file, output_file, 2) # removes nonenglish and stores in a temp file
        print(f"Removed Non-English from reviews and filtering reviews now")
    else:
        print("Error: Missing input files.")

