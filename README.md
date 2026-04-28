# CS 4412 - Data Mining Project 

Goodreads Data mining

Author: David Holland
E-Mail: dholla36@students.kennesaw.edu

## Project Overview
This data mining project analyzes the 2017 UCSD Goodreads dataset to uncover behavioral patterns within the reading community. By utilizing Association Rule Mining (FP-Growth), K-Means Clustering, and Latent Dirichlet Allocation (LDA) Topic Modeling, this project moves beyond standard rating predictions to categorize distinct "Reader Personas" and identify the contextual drivers of high community engagement.

## Datasets 

Download the following datasets from the UCSD Goodreads

https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html#datasets

- goodreads_books.json.gz
- goodreads_interactions.csv
- goodreads_reviews_dedup.json.gz

**Important:**
 Once downloaded, create a folder named `data/` in the root directory of this repository and place all three files inside it. The Jupyter Notebook relies on this folder structure to execute properly.

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

** Execution Instructions **: 
All data ingestion, preprocessing, and machine learning models are consolidated within a single Jupyter Notebook for ease of use.
1. Navigate to the notebooks/ directory and open Data Mining Project.ipynb.

2. Ensure your datasets are correctly placed in the data/ folder as mentioned above.

3. Run the notebook cells sequentially from top to bottom. The notebook will automatically handle the chunked reading of the multi-gigabyte JSON files, filter the targeted genres, and generate the intermediate CSV files required for the final clustering and LDA models.

* Note: Our team initally used python scripts are kept in the scripts/ folder for archival and developmental context and imported them into the notebook using %run but the Jupyter Notebook is the definitive, fully-integrated version of this project. *


Before running the notebook make sure to run `pip install -r requirements.txt` as many more requirements were added to complete the required analysis. In addition I have also switched from using `jupyter lab` to `jupyter notebook` as it provides a simpler website UI. 

