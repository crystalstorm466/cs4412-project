from contextlib import ExitStack
import json
import gzip
import os
import sys
import pandas as pd

#Uses goodreads_reviews_dedup.json.gz dataset to stream throught the massive 11GB fiile (millions of reviews)
# isolates only the matching bookids from the subset created in 01_filterbooks.py
# output: produces a dataset of individua user reviews spefically for the books in the subset

def clean_admin_tags(input_path, output_path, min_count=10):
    print(f"Extracting and cleaning tags from {input_path}...")
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
        'to-buy', 'audio', 'i-own', 'my library', 'to-buy', 'to buy', 'ebooks', 'kindle',
        'currently reading',
    }

    with open(input_path, 'rt', encoding='utf-8') as fin, \
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

                    name = s.get('name', '').strip()
                    if not name:
                        continue

                    # normalize for comparison
                    norm = name.lower().replace('-', ' ').replace('_', ' ').strip()

                    if norm.startswith('read in'):
                        continue
                    if norm in generic_tags or len(norm) <= 2 or norm.isdigit():
                        continue

                    filtered_shelves.append(name.replace(',', ' ').strip())

                # Only save books that still have at least 2 relevant tags left
                if len(filtered_shelves) > 1:
                    book['popular_shelves'] = filtered_shelves
                    fout.write(json.dumps(book) + '\n')
                    book_count += 1

                if book_count % 50000 == 0 and book_count > 0:
                    print(f"Cleaned {book_count} books...")
            except (json.JSONDecodeError, ValueError):
                continue

    print(f"Done! Saved {book_count} clean books to {output_path}.")

def flatten_shelves(shelves_list):
    """Helper to turn list of strings into a clean string: 'tag1, tag2, tag3'"""
    if not isinstance(shelves_list, list):
        return ""
    return ", ".join(shelves_list[:10])


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
    open_func = gzip.open if review_dataset.endswith('.gz') else open

    with open_func(review_dataset, mode="rt", encoding='utf-8') as fin, \
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


def make_table(filtered_reviews, output):
    print(f"Loading filtered reviews from {filtered_reviews} in chunks...")
    try:
        chunk_size = 100000
        first_chunk = True
        total_processed = 0

        # Read the file in chunks of 100,000 rows
        for chunk in pd.read_json(filtered_reviews, lines=True, chunksize=chunk_size):
            if 'review_text' in chunk.columns:
                # Replace newlines with spaces and truncate to 100 chars
                chunk['review_snippet'] = chunk['review_text'].str.replace('\n', ' ', regex=False).str[:100] + "..."

            cols_to_show = ['book_id', 'user_id', 'rating', 'n_votes', 'n_comments', 'review_snippet']
            existing_cols = [c for c in cols_to_show if c in chunk.columns]

            working_df = chunk[existing_cols]

            # If it's the first chunk, write with headers. Otherwise, append without headers.
            if first_chunk:
                working_df.to_csv(output, index=False, mode='w')
                print(working_df.head().to_string(index=False))
                first_chunk = False
            else:
                working_df.to_csv(output, index=False, mode='a', header=False)

            total_processed += len(working_df)
            print(f"Processed and appended {total_processed} reviews to CSV...")

        print(f"\nSuccess! Readable results saved to: {output}")
        return True

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

