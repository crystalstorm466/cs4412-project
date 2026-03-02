import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Load the association rules
df_rules = pd.read_csv('data/03_extractedreviews.csv')

# 2. Set up the figure size
plt.figure(figsize=(12, 6))

# 3. Create the scatter plot
# x is support, y is confidence, and the color (hue) represents the lift value
scatter = sns.scatterplot(
    data=df_rules,
    x='n_comments',
    y='n_votes',
    palette='viridis', # A good color map for showing scale
    sizes=(50, 200),
    alpha=0.8
)

# 4. Add titles and labels
plt.title('Comments vs Votes of User Reviews', fontsize=14)
plt.xlabel('n_comments', fontsize=12)
plt.ylabel('n_votes', fontsize=12)

# Move the legend outside the plot

# 5. Show and save the plot
plt.tight_layout()
plt.savefig('scatterplot_extractedreviews.png')
plt.show()
