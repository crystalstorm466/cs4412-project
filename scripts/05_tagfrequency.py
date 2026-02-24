import sys
import pandas as pd 
import json 
import os
from collections import Counter

def tag(inputjson, outputcsv):
    tags_counts = Counter()

    print(f"reading {inputjson} to measure tag")

    with open(inputjson, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                book = json.loads(line)
                shelves = book.get('popular_shelves', [])

                tags = []

                for s in shelves:
                    # If it's the original dict format: {'count': 'X', 'name': 'tag'}
                    if isinstance(s, dict):
                        tags.append(s['name'].lower().strip())
                    # If it's already been flattened to a string: 'tag'
                    elif isinstance(s, str):
                        tags.append(s.lower().strip())

                tags_counts.update(tags)

            except json.JSONDecodeError:
                continue

    df = pd.DataFrame(tags_counts.most_common(50), columns=['Tag', 'Book Count'])

    df.to_csv(outputcsv, index=False)

    print("\n " + "="*40)
    print("TOP 15 DISCOVERED ATTRIBUTES (SHELF TAGS)")
    print("="*40)
    print(df.head(15).to_string(index=False))
    print("="*40)
    print(f"Results saved to {outputcsv}")


if __name__ == "__main__":
    input_file = sys.argv[1]
    output = sys.argv[2]


    if os.path.exists(input_file):
        tag(input_file, output)
    else:
         print(f"Error: {input_file} not found. Please run your extraction script first.")
