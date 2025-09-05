import os
import redis
import asyncio
import aiohttp
import csv
import json
from datetime import datetime, timedelta
from flask import Flask, send_file, jsonify, render_template_string, request
import io
import logging
import gc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get port from environment variable or default to 5000
PORT = int(os.getenv('PORT', 5000))
logger.info(f"Starting server on port {PORT}")

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
logger.info(f"Redis URL: {REDIS_URL}")

try:
    redis_client = redis.from_url(REDIS_URL)
    # Test Redis connection
    redis_client.ping()
    logger.info("Redis connection successful")
except redis.ConnectionError as e:
    logger.error(f"Redis connection error: {e}")
    redis_client = None
except Exception as e:
    logger.error(f"Unexpected Redis error: {e}")
    redis_client = None

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
            gc.collect()  # Force garbage collection to manage memory
    
    return all_data

def process_data(raw_data):
    """Process raw Binance data into a structured format without pandas"""
    if not raw_data:
        return None
    
    processed_data = []
    for candle in raw_data:
        try:
            processed_candle = {
                'open_time': datetime.fromtimestamp(candle[0] / 1000).isoformat(),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': float(candle[5]),
                'close_time': datetime.fromtimestamp(candle[6] / 1000).isoformat(),
                'quote_asset_volume': float(candle[7]),
                'number_of_trades': int(candle[8]),
                'taker_buy_base_asset_volume': float(candle[9]),
                'taker_buy_quote_asset_volume': float(candle[10])
            }
            processed_data.append(processed_candle)
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Error processing candle data: {e}, candle: {candle}")
            continue
    
    # Sort by time
    processed_data.sort(key=lambda x: x['open_time'])
    
    return processed_data

def save_to_redis(data):
    """Save processed data to Redis"""
    if data is None:
        return False
    
    if redis_client is None:
        logger.error("Cannot save to Redis: Redis client not available")
        return False
    
    try:
        # Convert data to JSON
        data_json = json.dumps(data)
        
        # Save to Redis with expiration (7 days)
        redis_key = get_redis_key()
        redis_client.setex(redis_key, timedelta(days=7), data_json)
        
        # Also save metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'symbol': SYMBOL,
            'interval': INTERVAL,
            'data_points': len(data),
            'start_date': data[0]['open_time'],
            'end_date': data[-1]['open_time']
        }
        redis_client.setex(f"{redis_key}:metadata", timedelta(days=7), json.dumps(metadata))
        
        logger.info(f"Data saved to Redis. Total records: {len(data)}")
        return True
    except Exception as e:
        logger.error(f"Error saving to Redis: {e}")
        return False

# HTML template for the home page
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Binance BTC/USDT Historical Data</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        pre { background: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
        a { color: #007bff; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .alert { padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .alert-success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .alert-error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .alert-warning { background-color: #fff3cd; color: #856404; border: 1px solid #ffeeba; }
        .btn { display: inline-block; padding: 10px 20px; background: #007bff; color: white; border-radius: 5px; }
        .btn:hover { background: #0056b3; text-decoration: none; }
        .btn-warning { background: #ffc107; color: #000; }
        .btn-warning:hover { background: #e0a800; }
    </style>
</head>
<body>
    <h1>Binance BTC/USDT Historical Data</h1>
    
    {% if message %}
    <div class="alert alert-{{ message_type }}">{{ message }}</div>
    {% endif %}
    
    {% if redis_status != "connected" %}
    <div class="alert alert-warning">
        <strong>Warning:</strong> Redis is not connected. Data will not be persisted.
        {% if REDIS_URL %}Using Redis URL: {{ REDIS_URL }}{% endif %}
    </div>
    {% endif %}
    
    {% if data_exists %}
    <p>Data available for download:</p>
    <pre>{{ metadata | tojson(indent=2) }}</pre>
    <p><a href="/download/csv" class="btn">Download CSV</a></p>
    <p><a href="/download/json" class="btn">Download JSON</a></p>
    <p><a href="/refresh" class="btn">Refresh Data (takes a few minutes)</a></p>
    {% else %}
    <p>No data available. <a href="/refresh" class="btn">Click here to fetch data</a></p>
    {% endif %}
    
    <hr>
    <p><small>Server running on port: {{ port }}</small></p>
</body>
</html>
"""

@app.route('/')
def home():
    """Home page with download links"""
    redis_key = get_redis_key()
    data_exists = redis_client and redis_client.exists(redis_key)
    
    message = request.args.get('message')
    message_type = request.args.get('message_type', 'success')
    
    if data_exists:
        metadata_json = redis_client.get(f"{redis_key}:metadata")
        if metadata_json:
            try:
                metadata = json.loads(metadata_json)
            except json.JSONDecodeError:
                metadata = {'status': 'Error parsing metadata'}
        else:
            metadata = {'status': 'Data available but metadata missing'}
    else:
        metadata = {}
    
    return render_template_string(HTML_TEMPLATE, 
                                data_exists=data_exists, 
                                metadata=metadata,
                                message=message,
                                message_type=message_type,
                                redis_status="connected" if redis_client else "disconnected",
                                REDIS_URL=REDIS_URL,
                                port=PORT)

@app.route('/refresh')
def refresh_data():
    """Trigger data refresh"""
    try:
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(fetch_and_process_data())
        loop.close()
        
        return jsonify({'status': 'success', 'message': 'Data refresh completed'})
    except Exception as e:
        logger.error(f"Error in refresh_data: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

async def fetch_and_process_data():
    """Main function to fetch and process data"""
    logger.info("Starting data fetch...")
    try:
        raw_data = await fetch_historical_data()
        logger.info(f"Fetched {len(raw_data)} total candles")
        
        processed_data = process_data(raw_data)
        if processed_data is not None:
            save_to_redis(processed_data)
            logger.info("Data processing completed successfully")
        else:
            logger.error("Failed to process data")
    except Exception as e:
        logger.error(f"Error in fetch_and_process_data: {e}")

@app.route('/download/<format>')
def download_data(format):
    """Download data in specified format"""
    if not redis_client:
        return "Redis not available. Cannot download data.", 500
        
    redis_key = get_redis_key()
    data_json = redis_client.get(redis_key)
    
    if not data_json:
        return "Data not available. Please refresh first.", 404
    
    try:
        data = json.loads(data_json)
    except json.JSONDecodeError:
        return "Error parsing data. Please refresh.", 500
    
    filename = f"binance_{SYMBOL}_{INTERVAL}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if format == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['open_time', 'open', 'high', 'low', 'close', 'volume', 
                         'close_time', 'quote_asset_volume', 'number_of_trades',
                         'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'])
        
        # Write data
        for candle in data:
            writer.writerow([
                candle['open_time'],
                candle['open'],
                candle['high'],
                candle['low'],
                candle['close'],
                candle['volume'],
                candle['close_time'],
                candle['quote_asset_volume'],
                candle['number_of_trades'],
                candle['taker_buy_base_asset_volume'],
                candle['taker_buy_quote_asset_volume']
            ])
        
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{filename}.csv"
        )
    
    elif format == 'json':
        output = io.BytesIO()
        output.write(json.dumps(data, indent=2).encode())
        output.seek(0)
        return send_file(
            output,
            mimetype='application/json',
            as_attachment=True,
            download_name=f"{filename}.json"
        )
    
    return "Invalid format", 400

@app.route('/health')
def health():
    """Health check endpoint"""
    redis_status = "connected" if redis_client else "disconnected"
    return jsonify({
        'status': 'healthy', 
        'timestamp': datetime.now().isoformat(),
        'redis': redis_status,
        'port': PORT
    })

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Server error: {error}")
    return render_template_string("""
        <h1>500 - Internal Server Error</h1>
        <p>Something went wrong. Please try again later.</p>
        <p><a href="/">Return to home</a></p>
    """), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return render_template_string("""
        <h1>404 - Page Not Found</h1>
        <p>The page you're looking for doesn't exist.</p>
        <p><a href="/">Return to home</a></p>
    """), 404

if __name__ == '__main__':
    # Force garbage collection before starting
    gc.collect()
    
    # Get port from environment variable or default to 5000
    port = int(os.getenv('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    
    app.run(host='0.0.0.0', port=port, debug=False)
