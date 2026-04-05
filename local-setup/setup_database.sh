#!/bin/bash
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="admin"
DB_PASSWORD="Admin@2001"
DB_NAME="stock_forecast"
CONTAINER_NAME="postgres-db"

export PGPASSWORD=$DB_PASSWORD

echo "Creating tickers table..."
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < create_tickers_table.sql

if [ $? -eq 0 ]; then
    echo "tickers table created successfully!"
else
    echo "Error creating tickers table!"
    exit 1
fi

echo "Creating raw_daily table..."
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < create_raw_daily_table.sql

if [ $? -eq 0 ]; then
    echo "raw_daily table created successfully!"
else
    echo "Error creating raw_daily table!"
    exit 1
fi

echo "Creating enriched_daily table..."
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < create_enriched_daily_table.sql

if [ $? -eq 0 ]; then
    echo "enriched_daily table created successfully!"
else
    echo "Error creating enriched_daily table!"
    exit 1
fi

echo "Creating predictions table..."
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < create_predictions_table.sql

if [ $? -eq 0 ]; then
    echo "predictions table created successfully!"
else
    echo "Error creating predictions table!"
    exit 1
fi

echo "All tables created successfully!"
unset PGPASSWORD
