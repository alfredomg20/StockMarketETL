import pandas as pd

# Get tickers for top NASDAQ companies from csv file
csv_filepath = 'config/nasdaq_symbols.csv'
df = pd.read_csv(csv_filepath)

# Handle special characters
df['Symbol'] = df['Symbol'].replace({'BRK/A': 'BRK-A', 'BRK/B': 'BRK-B'})

# Sort by Market Cap and select top tickers
df = df.sort_values(by='Market Cap', ascending=False)
TICKERS = df['Symbol'].head(100).to_list()
