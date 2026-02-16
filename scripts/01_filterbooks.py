import json
import gzip
import os
import sys
def filter_goodreads_data(input_path, output_path, target_keywords):
    """
    Streams a large .json.gz file and filters records based on keywords
    in the 'popular_shelves' attribute.
    """
    print(f"Starting filtration of {input_path}...")
    count = 0
    saved = 0

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with gzip.open(input_path, 'rt', encoding='utf-8') as fin, \
         open(output_path, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            count += 1
            book = json.loads(line)
            
            # Extract tags from the popular_shelves list of dictionaries
            # Structure: [{'count': '123', 'name': 'fantasy'}, ...]
            shelves = [s['name'].lower() for s in book.get('popular_shelves', [])]
            
            # Check if any target keywords (like 'romantasy') are in the shelves
            if any(key in " ".join(shelves) for key in target_keywords):
                fout.write(json.dumps(book) + '\n')
                saved += 1
            
            if count % 100000 == 0:
                print(f"Processed {count} books... Saved {saved} matches.")

    print(f"Done! Processed {count} total books. Saved {saved} to {output_path}.")

if __name__ == "__main__":
    # Define your "Romantasy" anchor keywords
    keywords = ['romantasy', 'romantic-fantasy', 'fantasy-romance']
    
    # Update these paths to your actual local file locations
    input_file = "data/goodreads_books.json.gz"
    output_file = "data/romantasy_books_subset.json"
    

    input_file = sys.argv[1];
    output_file = sys.argv[2];
    if os.path.exists(input_file):
        filter_goodreads_data(input_file, output_file, keywords)
    else:
        print(f"Error: {input_file} not found. Please download it first.")
