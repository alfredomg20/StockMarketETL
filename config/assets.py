# Get tickers for top 1000 NASDAQ companies from nasdaq_symbols.csv
import pandas as pd
csv_filepath = 'config/nasdaq_symbols.csv'
df = pd.read_csv(csv_filepath)
# rename 'BRK/A' and 'BRK/B' to 'BRK-A' and 'BRK-B'
df['Symbol'] = df['Symbol'].replace({'BRK/A': 'BRK-A', 'BRK/B': 'BRK-B'})
df = df.sort_values(by='Market Cap', ascending=False)
TICKERS = df['Symbol'].head(1000).to_list()
