import os
import redis
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, send_file, jsonify
import io
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.from_url(REDIS_URL)

# Binance API configuration
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
YEARS_TO_FETCH = 8  # Fetch 8 years of data

def get_redis_key():
    return f"binance:{SYMBOL}:{INTERVAL}:historical_data"

async def fetch_binance_data(session, symbol, interval, start_time, end_time):
    """Fetch data from Binance API for a specific time range"""
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': int(start_time.timestamp() * 1000),
        'endTime': int(end_time.timestamp() * 1000),
        'limit': 1000
    }
    
    try:
        async with session.get(BINANCE_API_URL, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.error(f"Error fetching data: {response.status}")
                return []
    except Exception as e:
        logger.error(f"Error in fetch_binance_data: {e}")
        return []

async def fetch_historical_data():
    """Fetch 8 years of hourly BTC data from Binance"""
    end_time = datetime.now()
    start_time = end_time - timedelta(days=YEARS_TO_FETCH * 365)
    
    logger.info(f"Fetching data from {start_time} to {end_time}")
    
    all_data = []
    current_start = start_time
    
    async with aiohttp.ClientSession() as session:
        while current_start < end_time:
            current_end = min(current_start + timedelta(days=30), end_time)
            
            data = await fetch_binance_data(session, SYMBOL, INTERVAL, current_start, current_end)
            if data:
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} candles for period {current_start} to {current_end}")
            
            current_start = current_end
            await asyncio.sleep(0.1)  # Rate limiting
    
    return all_data

def process_data(raw_data):
    """Process raw Binance data into a structured format"""
    if not raw_data:
        return None
    
    df = pd.DataFrame(raw_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    
    # Convert timestamps
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    
    # Convert numeric columns
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume', 
                       'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Sort by time
    df = df.sort_values('open_time').reset_index(drop=True)
    
    return df

def save_to_redis(dataframe):
    """Save processed data to Redis"""
    if dataframe is None:
        return False
    
    try:
        # Convert DataFrame to JSON
        data_json = dataframe.to_json(orient='records', date_format='iso')
        
        # Save to Redis with expiration (7 days)
        redis_key = get_redis_key()
        redis_client.setex(redis_key, timedelta(days=7), data_json)
        
        # Also save metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'symbol': SYMBOL,
            'interval': INTERVAL,
            'data_points': len(dataframe),
            'start_date': dataframe['open_time'].min().isoformat(),
            'end_date': dataframe['open_time'].max().isoformat()
        }
        redis_client.setex(f"{redis_key}:metadata", timedelta(days=7), json.dumps(metadata))
        
        logger.info(f"Data saved to Redis. Total records: {len(dataframe)}")
        return True
    except Exception as e:
        logger.error(f"Error saving to Redis: {e}")
        return False

@app.route('/')
def home():
    """Home page with download links"""
    redis_key = get_redis_key()
    data_exists = redis_client.exists(redis_key)
    
    if data_exists:
        metadata_json = redis_client.get(f"{redis_key}:metadata")
        if metadata_json:
            metadata = json.loads(metadata_json)
        else:
            metadata = {'status': 'Data available but metadata missing'}
        
        return f"""
        <h1>Binance BTC/USDT Historical Data</h1>
        <p>Data available for download:</p>
        <pre>{json.dumps(metadata, indent=2)}</pre>
        <p><a href="/download/csv">Download CSV</a></p>
        <p><a href="/download/json">Download JSON</a></p>
        <p><a href="/download/parquet">Download Parquet</a></p>
        <p><a href="/refresh">Refresh Data (takes a few minutes)</a></p>
        """
    else:
        return """
        <h1>Binance BTC/USDT Historical Data</h1>
        <p>No data available. <a href="/refresh">Click here to fetch data</a></p>
        """

@app.route('/refresh')
def refresh_data():
    """Trigger data refresh"""
    try:
        # Run async function in background
        asyncio.run(fetch_and_process_data())
        return jsonify({'status': 'success', 'message': 'Data refresh started'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

async def fetch_and_process_data():
    """Main function to fetch and process data"""
    logger.info("Starting data fetch...")
    raw_data = await fetch_historical_data()
    logger.info(f"Fetched {len(raw_data)} total candles")
    
    processed_data = process_data(raw_data)
    if processed_data is not None:
        save_to_redis(processed_data)
        logger.info("Data processing completed successfully")
    else:
        logger.error("Failed to process data")

@app.route('/download/<format>')
def download_data(format):
    """Download data in specified format"""
    redis_key = get_redis_key()
    data_json = redis_client.get(redis_key)
    
    if not data_json:
        return "Data not available. Please refresh first.", 404
    
    data = json.loads(data_json)
    df = pd.DataFrame(data)
    
    # Convert date strings back to datetime
    df['open_time'] = pd.to_datetime(df['open_time'])
    df['close_time'] = pd.to_datetime(df['close_time'])
    
    filename = f"binance_{SYMBOL}_{INTERVAL}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if format == 'csv':
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{filename}.csv"
        )
    
    elif format == 'json':
        output = io.BytesIO()
        df.to_json(output, orient='records', date_format='iso')
        output.seek(0)
        return send_file(
            output,
            mimetype='application/json',
            as_attachment=True,
            download_name=f"{filename}.json"
        )
    
    elif format == 'parquet':
        output = io.BytesIO()
        df.to_parquet(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/octet-stream',
            as_attachment=True,
            download_name=f"{filename}.parquet"
        )
    
    return "Invalid format", 400

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    # Fetch data on startup if not available
    redis_key = get_redis_key()
    if not redis_client.exists(redis_key):
        logger.info("No data found in Redis, fetching initial data...")
        asyncio.run(fetch_and_process_data())
    
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
