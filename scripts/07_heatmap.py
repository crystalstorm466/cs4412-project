import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Load the dataset
df_reviews = pd.read_csv('../data/03_extractedreviews.csv')

# 2. Ensure we have a review_length column (calculate it if missing)
if 'review_length' not in df_reviews.columns:
    # Assuming the text column is named 'review_snippet' based on your previous message
    df_reviews['review_length'] = df_reviews['review_snippet'].astype(str).apply(len)

# 3. Select ONLY the numeric columns for correlation
# (Correlation only works on numbers, not text or IDs)
numeric_cols = ['rating', 'n_votes', 'n_comments', 'review_length']
df_numeric = df_reviews[numeric_cols]

# 4. Calculate the correlation matrix
corr_matrix = df_numeric.corr()

# 5. Set up the figure size
plt.figure(figsize=(8, 6))

# 6. Create the heatmap
# annot=True puts the actual numbers inside the squares
# cmap='coolwarm' provides a good blue-to-red color scale (-1 to 1)
# vmin=-1, vmax=1 sets the scale bounds
sns.heatmap(
    corr_matrix,
    annot=True,
    cmap='coolwarm',
    vmin=-1,
    vmax=1,
    fmt='.2f',
    linewidths=0.5
)

# 7. Add title and format layout
plt.title('Correlation Heatmap of User Engagement Metrics', fontsize=14)
plt.tight_layout()

# 8. Save and show the plot
plt.savefig('../docs/correlation_heatmap.png')
plt.show()
