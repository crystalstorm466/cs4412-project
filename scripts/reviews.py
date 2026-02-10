import json
import gzip
import os
import sys



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

    with open(review_dataset, 'r', encoding='utf-8') as fin, \
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



if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python reviews.py <subset_path.json> <reviews_dataset.json> <output.json>")
        sys.exit(1)

    subset = sys.argv[1]
    reviews = sys.argv[2]
    output = sys.argv[3]

    if os.path.exists(subset) and os.path.exists(reviews):
        #        with open('reviews_english.json', 'w') as fp:
         #   pass
        #print(f"Removing non-English from reviews and storing in a temp file reviews_english.json")    
        #remove_nonenglish(reviews, 'reviews_english.json', 2) # removes nonenglish and stores in a temp file
        #print(f"Removed Non-English from reviews and filtering reviews now")
        filter_reviews(subset, reviews, output)
    else:
        print("Error: Missing input files.")

