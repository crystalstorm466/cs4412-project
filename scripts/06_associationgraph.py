import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Load the association rules
df_rules = pd.read_csv('data/04_association.csv')

# 2. Set up the figure size
plt.figure(figsize=(10, 6))

# 3. Create the scatter plot
# x is support, y is confidence, and the color (hue) represents the lift value
scatter = sns.scatterplot(
    data=df_rules,
    x='support',
    y='confidence',
    hue='lift',
    palette='viridis', # A good color map for showing scale
    size='lift',       # Makes higher lift points slightly larger
    sizes=(50, 200),
    alpha=0.8
)

# 4. Add titles and labels
plt.title('Association Rules: Support vs. Confidence', fontsize=14)
plt.xlabel('Support', fontsize=12)
plt.ylabel('Confidence', fontsize=12)

# Move the legend outside the plot
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title='Lift')

# 5. Show and save the plot
plt.tight_layout()
plt.savefig('scatter_association_rules.png')
plt.show()
