#!/bin/bash

echo "Installing required dependencies..."
pip install flask==2.0.1 trino==0.319.0 python-dateutil==2.8.2 requests==2.28.1 werkzeug==2.0.3

echo "Attempting to install Kafka dependencies (optional)..."
# Try multiple approaches to install confluent-kafka
if command -v brew >/dev/null 2>&1; then
    # macOS with Homebrew
    brew list librdkafka >/dev/null 2>&1 || brew install librdkafka
    export C_INCLUDE_PATH=$(brew --prefix)/include
    export LIBRARY_PATH=$(brew --prefix)/lib
fi

# Try to install confluent-kafka with pip
pip install confluent-kafka || {
    echo "Trying to install confluent-kafka with --no-binary option..."
    pip install --no-binary confluent-kafka confluent-kafka || {
        echo "Trying older version of confluent-kafka..."
        pip install 'confluent-kafka<2.0.0' || {
            echo "confluent-kafka failed to install. The app will still work but will use mock Kafka."
        }
    }
}

echo "Starting the Flask application..."
export FLASK_APP=app.py
export FLASK_ENV=development
flask run --host=0.0.0.0