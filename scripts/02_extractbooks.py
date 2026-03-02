import json
import os
import sys
import pandas as pd

# 02_extractbooks removes all administrative tags like "to-read" to reduce noise
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

def make_books_table(clean_json, output_csv):
    print(f"Formatting {clean_json} to CSV...")
    try:
        # Load the clean JSON
        df = pd.read_json(clean_json, lines=True)
        print(f"Successfully loaded {len(df)} books")

        # Extract primary author ID safely
        if 'authors' in df.columns:
            df['primary_author'] = df['authors'].apply(lambda x: x[0]['author_id'] if isinstance(x, list) and len(x) > 0 else "")

        # Flatten the shelves list for the CSV
        if 'popular_shelves' in df.columns:
            df['shelves_list'] = df['popular_shelves'].apply(flatten_shelves)

        # Select ONLY book-related columns
        cols_to_show = ['book_id', 'title', 'average_rating', 'ratings_count', 'primary_author', 'shelves_list']
        existing_cols = [c for c in cols_to_show if c in df.columns]

        working_df = df[existing_cols]

        # Save to CSV
        working_df.to_csv(output_csv, index=False)
        print(working_df.head().to_string(index=False))
        print(f"\nSuccess! Readable book results saved to: {output_csv}")

    except Exception as e:
        print(f"Failed to load JSON to CSV: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python 02_extractbooks.py <input_subset.json> <output_clean.json> <output.csv>")
        sys.exit(1)

    input_subset = sys.argv[1]   # data/01_filteredbooks.json
    output_clean = sys.argv[2]   # data/02_filteredbooks_noadmin.json
    output_csv = sys.argv[3]     # data/02_extractedbooks.csv

    if os.path.exists(input_subset):
        # 1. Clean the tags
        clean_admin_tags(input_subset, output_clean)
        # 2. Make the CSV
        make_books_table(output_clean, output_csv)
    else:
        print(f"Error: {input_subset} not found.")
