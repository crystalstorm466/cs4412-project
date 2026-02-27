import json
import gzip
import os
import sys
import pandas as pd

# 02_extractbooks first removes all administrative tags like "to-read", "read" basically tags that wont tell use much data and just add noise and file size
# This script normalizes shelve names from the first script. It removes the administrative tags
def track_frequency(input_path, output_path, min_count=10):

    print(f"Extracting tags from {input_path}...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    book_count = 0
    # Generic tags that add noise to discovery patterns
    generic_tags = {
        'to-read', 'currently-reading', 'owned', 'favorites',
        'all-time-favorites', 'books-i-own', 'read-in-2017',
        'read-in-2016', 'default', 'ebook', 'kindle', 'audiobook',
        'my-books', 'library', 'wish-list', 'maybe', 'finish', 'read',
        'to-own', 'currently-reading', 'i-own', 'on-my-shelf', 'dnf', 'my-personal-library',
        're-read', 'tbr', 'tbr-pile', 'to read', 'currently reading', 'owned', 'favorites', 'all time favorites',
        'books i own', 'default', 'ebook', 'kindle', 'audiobook', 'audiobooks',
        'my books', 'library', 'wish list', 'maybe', 'finish', 'read', 'e book',
        'hardcover', 'paperback', 'hardback', 'dnf', 'dnf d', 'shelfari favorites',
        'owned books', 'favorite', 'paper', 'hardcopy', 'unfinished', 'duplicates',
        'i own it', 'not read', 'read some day', 'own hard copy', 'in my home library',
        'to-buy', 'audio', 'i-own', 'my library'
    }

    open_func = gzip.open if input_path.endswith('.gz') else open

    with open_func(input_path, 'rt', encoding='utf-8') as fin, \
         open(output_path, 'w', encoding='utf-8') as fout:

        for line in fin:
            try:
                book = json.loads(line)
                raw_shelves = book.get('popular_shelves', [])

                
            
                filtered_shelves = [] 
                   
                for s in raw_shelves:
                    try:
                        count = int((s.get('count', 0)))
                    except (ValueError, TypeError):
                        continue

                    if count < min_count:
                        continue

                    # 2. extract and normalize
                    name = s.get('name', '').strip()
                    if not name:
                        continue

                    #normalize for comparison
                    norm = name.lower().replace('-', ' ').replace('_', ' ').strip()

                    if norm.startswith('read in') or norm.startswith('read in-'):
                        continue
                    #3 filtering logic 
                    if norm in generic_tags:
                        continue
                    if len(norm) <= 2 or norm.isdigit():
                        continue

                    filtered_shelves.append(name.replace(',', ' ').strip())
                # Only save transactions with at  least 2 relevant tags
                if len(filtered_shelves) > 1:
                    book['popular_shelves'] = filtered_shelves
                    fout.write(json.dumps(book) + '\n')
                    book_count += 1

                if book_count % 100000 == 0 and book_count > 0:
                    print(f"Streamed {book_count} books...")
            except (json.JSONDecodeError, ValueError):
                continue

    print(f"Done! Saved {book_count} transactions to {output_path}.")
def filter_reviews(subset_path, nonadmin ,review_dataset, output):
    """
    1. Loads book_ids from already filtered metadata (created in main.py)
    2. Stremas the 11GB review file and saves only reviews for those books
    """
    print(f"0. Removing administrative tags from {subset_path}.")
    track_frequency(subset_path, nonadmin)
    print(f"1. Loading book ids from {nonadmin}")

    book_ids = set()

    with open(nonadmin, 'r', encoding='utf-8') as f:
        for line in f:
            book = json.loads(line)
            book_ids.add(book['book_id'])

    print(f"Loaded {len(book_ids)} unique book IDs")
    print(f"2. Filtering books from {review_dataset}")

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
                        print(f"Processed {count} books... Saved {saved} books")

    print(f"Done! saved {saved} books to {output}")

def flatten_shelves(shelves_list):
    """Helper to turn list of dicts into a clean string: 'tag1, tag2, tag3'"""
    if not isinstance(shelves_list, list):
        return ""
    # Extract just the 'name' and join them with commas
    return ", ".join([s['name'] for s in shelves_list[:10]]) # Limit to top 10 for readability

def make_table(filtered_reviews, output):
    print(f"Loading filtered books from {filtered_reviews}")
    try:

        df = pd.read_json(filtered_reviews, lines=True)
        print(f"Successfully loaded {len(df)} books ")
        if 'review_text' in df.columns:
            df['review_length'] = df['review_text'].str.len()
            # Create a snippet for the console preview only
            df['review_snippet'] = df['review_text'].str.replace('\n', ' ', regex=False).str[:75] + "..."

        if 'popular_shelves' in df.columns:
            df['shelves_list'] = df['popular_shelves'].apply(flatten_shelves)
        if 'authors' in df.columns:
            df['primary_author'] = df['authors'].apply(lambda x: x[0]['author_id'] if x else "")

        cols_to_show = ['book_id', 'user_id', 'title', 'rating', 'average_rating', 'n_votes', 'n_comments', 'review_length', 'shelves_list','review_snippet']
        existing_cols = [c for c in cols_to_show if c in df.columns]

        working_df = df[existing_cols]


        if 'review_length' in working_df.columns:
            working_df = working_df[working_df['review_length'] > 5]

        print(working_df.head().to_string(index=False))


        working_df.to_csv(output, index=False)
        print(f"\nSuccess! Readable results saved to: {output}")
        return working_df


    except Exception as e:
        print(f"Failed to load JSON: {e}")
        return None


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python 02_extractbooks.py <subset_path.json> <subset_path_noadmin.json> <reviews_dataset.json> <output.json> <output.csv>")
        sys.exit(1)

    subset = sys.argv[1]
    nonadmin = sys.argv[2]
    reviews = sys.argv[3]
    output = sys.argv[4]
    csv = sys.argv[5]

    if os.path.exists(subset) and os.path.exists(reviews):
        with open(nonadmin, 'w') as fp:
            pass
        with open(output, 'w') as fp:
            pass
        with open(csv, 'w') as fp:
            pass
        #print(f"Removing non-English from reviews and storing in a temp file reviews_english.json")    
        #remove_nonenglish(reviews, 'reviews_english.json', 2) # removes nonenglish and stores in a temp file
        #print(f"Removed Non-English from reviews and filtering reviews now")
        filter_reviews(subset, nonadmin, reviews, output)
        print(f"Formatting {output} to CSV and printing first 5 lines")
        df = make_table(output, csv)

        if df is not None:
           pass
    else:
        print("Error: Missing input files.")

