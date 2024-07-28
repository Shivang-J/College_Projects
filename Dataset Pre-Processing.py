import requests
import pandas as pd
from datetime import datetime
import io
import os

# Function to fetch and clean data
def get_nse_historical_data(stock_symbol, start_date, end_date):
    url = f'https://www.nseindia.com/api/historical/cm/equity?symbol={stock_symbol}&series=["EQ"]&from={start_date}&to={end_date}&csv=true'
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/csv, text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={stock_symbol}'
    }

    session = requests.Session()
    session.headers.update(headers)
    session.get(f'https://www.nseindia.com/get-quotes/equity?symbol={stock_symbol}')
    response = session.get(url)
    response.raise_for_status()
    
    data = pd.read_csv(io.StringIO(response.text))
    data.columns = data.columns.str.strip().str.replace('\ufeff', '') #byte order mark(BOM) removal
    
    return data

# Function to download and combine data for given years
def download_nse_data(stock_symbol, start_year, end_year):
    combined_data = pd.DataFrame()
    
    for year in range(start_year, end_year + 1):
        start_date = f'01-01-{year}'
        end_date = f'31-12-{year}' if year < end_year else datetime.now().strftime('%d-%m-%Y')
        
        try:
            data = get_nse_historical_data(stock_symbol, start_date, end_date)
            
            # Get the name of the first column
            date_column = data.columns[0]
            
            # Convert 'Date' column to datetime
            data[date_column] = pd.to_datetime(data[date_column], format='%d-%b-%Y')
            
            # Combine data for all years
            combined_data = pd.concat([combined_data, data], ignore_index=True)
        except requests.exceptions.HTTPError as e:
            print(f"Failed to download data for {year}: {e}")

    if not combined_data.empty:
        # Ensure the date column is sorted
        combined_data[date_column] = pd.to_datetime(combined_data[date_column], format='%d-%b-%Y')
        combined_data.sort_values(by=date_column, inplace=True)
        combined_data.reset_index(drop=True, inplace=True)
        
        # Convert the 'Date' column back to 'dd-mm-yyyy' format
        combined_data[date_column] = combined_data[date_column].dt.strftime('%d-%m-%Y')
        combined_data.rename(columns={date_column: 'Date'}, inplace=True)
        
        # Drop the 'series' column if it exists
        if 'series' in combined_data.columns:
            combined_data = combined_data.drop('series', axis=1)
        
        # Remove commas from numeric columns
        for col in combined_data.columns[1:]:
            combined_data[col] = pd.to_numeric(combined_data[col].astype(str).str.replace(',', ''), errors='coerce')

    return combined_data

# Parameters
start_year = int(input("Enter the start year: "))
end_year = int(input("Enter the end year: "))
stock_symbol = input("Enter the stock symbol: ")

# Download data
combined_data = download_nse_data(stock_symbol, start_year, end_year)

# Display combined data for verification
print("Combined data column names:", combined_data.columns)
print("First few rows of combined data:")
print(combined_data.head())

# Save to CSV in the Downloads folder
downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
output_file = os.path.join(downloads_path, f'{stock_symbol}_historical_data_{start_year}_to_{end_year}.csv')
combined_data.to_csv(output_file, index=False)

print(f'Data saved to {output_file}')
