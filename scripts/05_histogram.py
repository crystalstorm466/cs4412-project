import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import json
import gzip
def histogram(path):
    if not os.path.exists(path):
        print(f"error: File {path} does not exist. Please try again")
        return

    df = pd.read_csv(path)

    column_name = 'average_rating' if 'average_rating' in df.columns else 'rating'

    if column_name not in df.columns:
        print(f"Error: could not find")
        return


    plt.figure(figsize=(10,6))

    df[column_name].hist(
        bins=20,
        color="#008080",
        edgecolor='black',
        grid=False,
        alpha=0.7
    )
    title = column_name.replace('_', ' ').title()
    plt.title(f'Distribution of {title} in 01_goodreadsbooks.csv', fontsize=14, fontweight='bold')
    plt.xlabel('Rating (1-5)', fontsize=12)
    plt.ylabel('Frequency (Count of Books)', fontsize=12)

    mean_val = df[column_name].mean()
    plt.axvline(mean_val, color='red', linestyle='dashed', linewidth=2, label=f'Mean: {mean_val:.2f}')
    plt.legend()

    # 5. Save and Show
    output_img = 'docs/rating_histogram.png'
    plt.savefig(output_img, dpi=300, bbox_inches='tight')
    plt.show()

    print(f"Done created {path}")

def histogram_gz(path):

    data_points = []
    count = 0
    with gzip.open(path, 'rt', encoding='utf-8') as fin:
        try:
            for line in fin:
                count += 1
                record = json.loads(line)
                value = record.get('avarage_rating')

                if value is not None:
                    data_points.append(float(value))

                count += 1
                if count % 100000 == 0:
                    print(f"Processed {count} books..")
        except:
            print("an error occured")

        print(f"Processed {len(count)}")


    plt.figure(figsize=(10,6))
    plt.hist(data_points, bins=40, color="#008080", edgecolor='black', alpha=0.7)

    clean_title= value.replace('_', ' ').title()
    plt.title(f'Average Ratings of goodreads_books.json.gz', fontsize=14, fontweight='bold')
    plt.xlabel(clean_title, fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.grid(axis='y', alpha=0.3)

    output = "docs/globalaverageratings.png"
    plt.savefig(output, dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    if (len(sys.argv) > 1):

        if (sys.argv[1].endswith('.gz')):
            histogram_gz(sys.argv[1])
            exit()
        histogram(sys.argv[1])
    else:
        print("python 05_histogram.py <csv path>")


