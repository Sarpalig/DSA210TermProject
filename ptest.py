import pandas as pd
from scipy.stats import ttest_ind

# Load the data
file_path = "data/watch_history_summary.csv"
df = pd.read_csv(file_path)

# Ensure the 'month' column is treated as string
df['month'] = df['month'].astype(str)

# Extract winter months (December, January, February)
winter_months = ['12', '01', '02']
df['is_winter'] = df['month'].str[-2:].isin(winter_months)

# Group by 'month' and sum watch time
monthly_watch_time = df.groupby(['month', 'is_winter'])['duration_minutes'].sum().reset_index()

# Separate watch times for winter and non-winter months
winter_watch_time = monthly_watch_time[monthly_watch_time['is_winter'] == True]['duration_minutes']
non_winter_watch_time = monthly_watch_time[monthly_watch_time['is_winter'] == False]['duration_minutes']

# Perform two-sample t-test
t_stat, p_value_two_tailed = ttest_ind(winter_watch_time, non_winter_watch_time, equal_var=False)

# Adjust for one-tailed test by dividing the p-value
p_value_one_tailed = p_value_two_tailed / 2 if t_stat > 0 else 1 - (p_value_two_tailed / 2)

# Print results
print("One-Tailed T-Test Results for Winter Months")
print(f"T-Statistic: {t_stat:.4f}")
print(f"P-Value: {p_value_one_tailed:.4f}")

if p_value_one_tailed < 0.05:
    print("Reject the null hypothesis: There is no significant increase in watch time during winter months.")
else:
    print("Fail to reject the null hypothesis: The data supports that there is a significant increase in watch time during winter months.")

