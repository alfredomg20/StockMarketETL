# Stock Market ETL
ETL pipeline that extracts historical stock price data from Yahoo Finance along with sector information, transforms it for analysis, and loads it into BigQuery for querying and visualization.

The system is designed to implement incremental updates, only extracting the most recent data when run periodically.

## 🔍 Features

- **Data Extraction**: Retrieves stock data from Yahoo Finance API
- **Data Transformation**: Cleans and prepares the data for analysis
- **Data Loading**: Stores data in Google BigQuery
- **Incremental Updates**: Only extracts new data since last update
- **Containerization**: Docker support for easy deployment
- **Cloud Ready**: Can be deployed to Google Cloud Run as a job

## 🚀 Project Structure

```
Stock Market ETL
├── config/
│   ├── assets.py          # Defines stock tickers to track
│   ├── nasdaq_symbols.csv # Nasdaq stocks dataset to get stocks symbols(tickers)  
│   └── settings.py        # Configuration parameters
├── etl/
│   ├── extract.py         # Data extraction logic
│   ├── transform.py       # Data transformation logic
│   └── load.py            # Data loading logic
├── utils/
│   ├── google_cloud.py    # BigQuery connection utilities
│   └── validations.py     # Data validation functions
├── main.py                # Main ETL execution script
├── Dockerfile             # Docker configuration for containerization
└── requirements.txt       # Project dependencies
```

## ⚙️ Installation

### Local Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/StockMarketETL.git
   cd StockMarketETL
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your BigQuery credentials in `.env` file:
```
# Set up your BigQuery credential variables in .env
TYPE = 'service_account'
PROJECT_ID = 'your-project-id'
PRIVATE_KEY_ID = 'your-private-key-id'
PRIVATE_KEY = 'your-private-key'
CLIENT_EMAIL = 'your-client-email@example.com'
CLIENT_ID = 'your-client-id'
AUTH_URI = 'https://accounts.google.com/o/oauth2/auth'
TOKEN_URI = 'https://oauth2.googleapis.com/token'
AUTH_PROVIDER_X509_CERT_URL = 'https://www.googleapis.com/oauth2/v1/certs'
CLIENT_X509_CERT_URL = 'your-client-cert-url'
UNIVERSE_DOMAIN = 'googleapis.com'
DATASET_ID = 'your_dataset_name'
STOCKS_TABLE_ID = 'your_stocks_table_name'
SECTORS_TABLE_ID = 'your_sectors_table_name's
```

### Docker Installation

1. Build the Docker image:
   ```bash
   docker build -t stock-market-etl .
   ```

2. Run the container with environment variables:
   ```bash
   docker run -it --env-file .env stock-market-etl
   ```

### Google Cloud Run Deployment

1. Build and push the Docker image to Google Container Registry:
   ```bash
   gcloud builds submit --tag gcr.io/[YOUR-PROJECT-ID]/stock-market-etl
   ```

2. Deploy as a Cloud Run job:
   ```bash
   gcloud run jobs create stock-market-etl-job \
     --image gcr.io/[YOUR-PROJECT-ID]/stock-market-etl \
     --region [REGION] \
     --set-env-vars="PROJECT_ID=your-project-id,DATASET_ID=your_dataset_name,..." \
     --service-account="your-service-account@your-project-id.iam.gserviceaccount.com"
   ```

3. Schedule the job (optional):
   ```bash
   gcloud scheduler jobs create http stock-market-etl-daily \
     --location [REGION] \
     --schedule="0 8 * * 1-5" \
     --uri="https://[REGION]-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/[YOUR-PROJECT-ID]/jobs/stock-market-etl-job:run" \
     --http-method=POST \
     --oauth-service-account-email="your-service-account@your-project-id.iam.gserviceaccount.com"
   ```

## 🔧 Usage

### Local Execution

Run the ETL process:

```bash
python main.py
```

### Docker Execution

```bash
docker run stock-market-etl
```

### Cloud Run Job Execution

```bash
gcloud run jobs execute stock-market-etl-job
```

## 📊 Data Flow

1. **Extract**: Retrieve stock price history and sector data.
2. **Transform**: Clean and format data.
3. **Load**: Store data in BigQuery for analysis

## 🛠️ Technologies Used

- **Python**: Programming language
- **Yahoo Finance API**: Data source for stock prices
- **Pandas**: Data manipulation and analysis
- **Google BigQuery**: Data warehouse for storage and analysis
- **Docker**: Containerization for consistent deployment
- **Google Cloud Run**: Serverless compute platform for job execution
- **Logging**: ETL progress and errors tracking

## 📝 License

[MIT License](LICENSE)

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

## 📧 Contact

For questions or support, please contact [alfredomg4000@gmail.com](mailto:alfredomg4000@gmail.com).