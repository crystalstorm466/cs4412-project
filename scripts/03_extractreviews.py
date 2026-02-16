from contextlib import ExitStack
import json
import gzip
import os
import sys
import pandas as pd


def filter_reviews(subset_path, review_dataset, output):
    """
    1. Loads book_ids from already filtered metadata (created in main.py)
    2. Stremas the 11GB review file and saves only reviews for those books
    """

    print(f"1. Loading book ids from {subset_path}")

    book_ids = set()

    with open(subset_path, 'r', encoding='utf-8') as f:
        for line in f:
            book = json.loads(line)
            book_ids.add(book['book_id'])

    print(f"Loaded {len(book_ids)} unique book IDs")
    print(f"2. Filtering reviews from {review_dataset}")

    count = 0
    saved = 0

    with gzip.open(review_dataset, mode="rt", encoding='utf-8') as fin, \
            open(output, 'w', encoding='utf-8') as fout:


                 for line in fin:
                    count += 1
                    review = json.loads(line)

                    if review['book_id'] in book_ids:
                        fout.write(json.dumps(review) + '\n')
                        saved += 1
                    if count % 50000 == 0:
                        print(f"Processed {count} review... Saved {saved} reviews")

    print(f"Done! saved {saved} reviews to {output}")

def flatten_shelves(shelves_list):
    """Helper to turn list of dicts into a clean string: 'tag1, tag2, tag3'"""
    if not isinstance(shelves_list, list):
        return ""
    # Extract just the 'name' and join them with commas
    return ", ".join([s['name'] for s in shelves_list[:10]]) # Limit to top 10 for readability

def make_table(filtered_reviews, output):
    print(f"Loading filtered reviews from {filtered_reviews}")
    try:

        df = pd.read_json(filtered_reviews, lines=True)
        print(f"Successfully loaded {len(df)} reviews ")
        if 'review_text' in df.columns:
            # Replace newlines with spaces and truncate to 100 chars
            df['review_snippet'] = df['review_text'].str.replace('\n', ' ', regex=False).str[:100] + "..."

        cols_to_show = ['book_id', 'user_id', 'rating', 'n_votes', 'n_comments', 'review_snippet']
        existing_cols = [c for c in cols_to_show if c in df.columns]


        working_df = df[existing_cols]
        print(working_df.head().to_string(index=False))


        working_df.to_csv(output, index=False)
        print(f"\nSuccess! Readable results saved to: {output}")
        return working_df


    except Exception as e:
        print(f"Failed to load JSON: {e}")
        return None


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python reviews.py <subset_path.json> <reviews_dataset.json> <output.json> <output.csv>")
        sys.exit(1)

    subset = sys.argv[1]
    reviews = sys.argv[2]
    output = sys.argv[3]
    csv = sys.argv[4]

    if os.path.exists(subset) and os.path.exists(reviews):
        #        with open('reviews_english.json', 'w') as fp:
         #   pass
        #print(f"Removing non-English from reviews and storing in a temp file reviews_english.json")    
        #remove_nonenglish(reviews, 'reviews_english.json', 2) # removes nonenglish and stores in a temp file
        #print(f"Removed Non-English from reviews and filtering reviews now")
       filter_reviews(subset, reviews, output)
       print(f"Formatting {output} to CSV and printing first 5 lines")
       df = make_table(output, csv)

       if df is not None:
           pass
    else:
        print("Error: Missing input files.")

