# --- scripts/06_associationgraph.py ---
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

input_fil = '../data/04_association.csv'
df_rules = pd.read_csv(input_fil)

plt.figure(figsize=(10, 6))

scatter = sns.scatterplot(
    data=df_rules,
    x='support',
    y='confidence',
    hue='lift',
    palette='viridis', 
    size='lift',       
    sizes=(50, 200),
    alpha=0.8
)

plt.title('Association Rules: Support vs. Confidence', fontsize=14)
plt.xlabel('Support', fontsize=12)
plt.ylabel('Confidence', fontsize=12)

plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title='Lift')
plt.tight_layout()
plt.savefig('scatter_association_rules.png')
plt.show()
