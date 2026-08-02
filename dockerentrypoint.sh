#!/bin/sh

# Exit immediately if a command exits with a non-zero status
set -e

echo "📦 Checking database migrations..."
python manage.py migrate

echo "📁 Collecting static files..."
python manage.py collectstatic --noinput

echo "🌐 Starting Daphne production server..."
exec python -m daphne -b 0.0.0.0 -p 8000 config.asgi:application
