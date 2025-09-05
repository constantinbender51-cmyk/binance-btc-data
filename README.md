# Binance BTC/USDT Historical Data Fetcher

A Flask application that fetches 8 years of hourly BTC/USDT data from Binance and serves it via Redis with download links.

## Features

- Fetches 8 years of hourly candle data from Binance
- Stores data in Redis with automatic expiration (7 days)
- Provides download links for CSV, JSON, and Parquet formats
- Automatic data refresh on startup if no data exists
- Health check endpoint

## Deployment on Railway

1. Fork this repository
2. Connect your GitHub account to Railway
3. Create a new project on Railway and connect your repository
4. Add Redis plugin to your Railway project
5. Deploy!

## Environment Variables

- `REDIS_URL`: Redis connection URL (automatically provided by Railway Redis plugin)
- `PORT`: Port to run the application (default: 5000)

## Endpoints

- `/`: Home page with download links
- `/refresh`: Trigger manual data refresh
- `/download/<format>`: Download data (csv, json, parquet)
- `/health`: Health check endpoint

## Data Format

Each candle includes:
- Open time, Close time
- Open, High, Low, Close prices
- Volume, Quote asset volume
- Number of trades
- Taker buy volumes
