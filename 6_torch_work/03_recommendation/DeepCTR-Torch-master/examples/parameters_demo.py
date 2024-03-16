import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import LabelEncoder
import os, sys
import numpy as np

import pandas as pd
import numpy as np

# Create a DataFrame with NaN values
data = {'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emily'],
        'Age': [25, 30, 25, 30, 30],
        'Salary': [50000, 60000, 70000, 55000, 60000]}
df = pd.DataFrame(data)

# Drop rows with NaN values in the grouping column ('Age')
df.dropna(subset=['Age'], inplace=True)

# Sort the DataFrame within each group by 'Salary' in ascending order and add reverse group-wise numbering column
df['Reverse_Group_Number'] = df.groupby('Age')['Salary'].rank(ascending=False, method='first')

print(df)