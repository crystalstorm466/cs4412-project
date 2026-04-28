# CS 4412 - Data Mining Project 

Goodreads Data mining

Author: David Holland
E-Mail: dholla36@students.kennesaw.edu

## Datasets 

Download the following datasets from the UCSD Goodreads

https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html#datasets

- goodreads_books.json.gz
- goodreads_interactions.csv
- goodreads_reviews_dedup.json.gz

## Setup

1. Download the datasets from the URL in the Datasets category
   - Download `goodreads_books.json.gz`
   - Download `goodreads_interactions.csv`
   - Download `goodreads_reviews_dedup.json.gz`
   
2. Setup a virtual environment

Windows: 

```bat 
  python -m venv venv
  venv\Scripts\activate
```

Mac/Linux

```sh
  python3 -m venv venv
  source venv/bin/activate
```

3. Required Libraries

```sh
  pip install -r requirements.txt
```

To access the Jupyter Notebook. All code can be ran in the jupyter notebook.

For all platforms:

```sh
   jupyter notebook
```


# M3: Full Implementation

Milestone 3 is very comprehensive and was my favorite part of this project. I refactored much of the notebook and included the scripts as part of it and not using %run anymore. I also overhauled and changed a lot of the scripts as such any python scripts in ` scripts/ ` may not be accurate anymore. All code needed for the analysis and this project is inside the Jupyter notebook located inside `notebooks/Data Mining Project.ipynb`. The `scripts/` folder will be kept for archival purposes and to show the development process. 

Before running the notebook make sure to run `pip install -r requirements.txt` as many more requirements were added to complete the required analysis. In addition I have also switched from using `jupyter lab` to `jupyter notebook` as it provides a simpler website UI. 

