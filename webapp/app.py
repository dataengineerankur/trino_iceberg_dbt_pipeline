from flask import Flask, render_template, request, jsonify
import trino
import json
import traceback
import uuid
import datetime
import socket

# Try to import confluent_kafka, but provide fallback if not available
try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
    print("Confluent Kafka is available, using real Kafka integration")
except ImportError:
    print("confluent_kafka package not available. Kafka event production will be simulated.")
    KAFKA_AVAILABLE = False

# Stub for Producer to use when Kafka is not available
class MockProducer:
    def produce(self, topic, key=None, value=None):
        print(f"MOCK KAFKA: Would send to topic {topic}, key={key}, value={value}")
        return True
        
    def flush(self):
        return True

app = Flask(__name__)

# Trino connection details from environment variables
import os

# Update these values to match your Trino server settings
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")  # Change to your Trino server hostname/IP
TRINO_PORT = int(os.environ.get("TRINO_PORT", 8080))
TRINO_USER = os.environ.get("TRINO_USER", "trino")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "default")

# Kafka connection details from environment variables
# Default to localhost for when running on the host machine
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "events_topic")

def get_trino_connection():
    """Create and return a Trino connection"""
    try:
        print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT} as user '{TRINO_USER}'")
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            # Add a reasonable connection timeout
            http_scheme="http",
            request_timeout=30,
        )
        # Test the connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Successfully connected to Trino and executed test query: {result}")
        return conn
    except Exception as e:
        print(f"Error connecting to Trino: {str(e)}")
        traceback.print_exc()
        return None

def get_kafka_producer():
    """Create and return a Kafka producer, or a mock producer if Kafka is not available"""
    if not KAFKA_AVAILABLE:
        # Return a mock producer that logs messages instead of sending them
        return MockProducer()
        
    try:
        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname()
        }
        return Producer(producer_config)
    except Exception as e:
        print(f"Error creating Kafka producer: {str(e)}")
        print("Falling back to mock Kafka producer")
        return MockProducer()

@app.route('/')
def index():
    """Render the main page"""
    connection_settings = {
        'trino_host': TRINO_HOST,
        'trino_port': TRINO_PORT,
        'trino_user': TRINO_USER,
        'trino_catalog': TRINO_CATALOG,
        'trino_schema': TRINO_SCHEMA,
        'kafka_bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'kafka_topic': KAFKA_TOPIC,
        'kafka_available': KAFKA_AVAILABLE
    }
    return render_template('index.html', **connection_settings)

@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Update connection settings"""
    global TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG, TRINO_SCHEMA
    global KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    
    data = request.json
    
    # Update Trino settings
    if 'trino_host' in data:
        TRINO_HOST = data['trino_host']
    if 'trino_port' in data:
        TRINO_PORT = int(data['trino_port'])
    if 'trino_user' in data:
        TRINO_USER = data['trino_user']
    if 'trino_catalog' in data:
        TRINO_CATALOG = data['trino_catalog']
    if 'trino_schema' in data:
        TRINO_SCHEMA = data['trino_schema']
        
    # Update Kafka settings
    if 'kafka_bootstrap_servers' in data:
        KAFKA_BOOTSTRAP_SERVERS = data['kafka_bootstrap_servers']
    if 'kafka_topic' in data:
        KAFKA_TOPIC = data['kafka_topic']
        
    # Test Kafka connection if bootstrap servers were updated
    kafka_status = {'available': False, 'message': 'Not tested'}
    if 'kafka_bootstrap_servers' in data and KAFKA_AVAILABLE:
        try:
            import socket
            for server in KAFKA_BOOTSTRAP_SERVERS.split(','):
                host, port = server.split(':')
                print(f"Testing connection to Kafka at {host}:{port}")
                
                try:
                    # Try to connect to the host:port to verify it's reachable
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2)
                    s.connect((host, int(port)))
                    s.close()
                    print(f"Successfully connected to Kafka at {host}:{port}")
                    kafka_status = {'available': True, 'message': f'Successfully connected to {host}:{port}'}
                except Exception as e:
                    print(f"Failed to connect to Kafka at {host}:{port}: {str(e)}")
                    kafka_status = {'available': False, 'message': f'Failed to connect to {host}:{port}: {str(e)}'}
        except Exception as e:
            kafka_status = {'available': False, 'message': f'Error testing Kafka connection: {str(e)}'}
        
    return jsonify({
        'success': True,
        'message': 'Settings updated successfully',
        'settings': {
            'trino_host': TRINO_HOST,
            'trino_port': TRINO_PORT,
            'trino_user': TRINO_USER,
            'trino_catalog': TRINO_CATALOG,
            'trino_schema': TRINO_SCHEMA,
            'kafka_bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
            'kafka_topic': KAFKA_TOPIC
        },
        'kafka_status': kafka_status
    })

@app.route('/test_connection', methods=['POST'])
def test_connection():
    """Test the Trino connection"""
    try:
        conn = get_trino_connection()
        if not conn:
            return jsonify({'success': False, 'message': 'Failed to connect to Trino'}), 500
            
        cursor = conn.cursor()
        cursor.execute("SELECT 'Connected successfully!'")
        result = cursor.fetchone()[0]
        
        return jsonify({
            'success': True,
            'message': result,
            'connection_details': {
                'host': TRINO_HOST,
                'port': TRINO_PORT,
                'user': TRINO_USER,
                'catalog': TRINO_CATALOG,
                'schema': TRINO_SCHEMA
            }
        })
    except Exception as e:
        error_details = {
            'success': False,
            'message': f"Error connecting to Trino: {str(e)}",
            'traceback': traceback.format_exc(),
            'connection_details': {
                'host': TRINO_HOST,
                'port': TRINO_PORT,
                'user': TRINO_USER,
                'catalog': TRINO_CATALOG,
                'schema': TRINO_SCHEMA
            }
        }
        return jsonify(error_details), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/execute_query', methods=['POST'])
def execute_query():
    """Execute a Trino query and return the results"""
    query = request.json.get('query', '')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    
    print(f"Executing query: {query}")
        
    try:
        conn = get_trino_connection()
        if not conn:
            connection_details = {
                'host': TRINO_HOST,
                'port': TRINO_PORT,
                'user': TRINO_USER,
                'catalog': TRINO_CATALOG,
                'schema': TRINO_SCHEMA
            }
            error_message = f"Could not connect to Trino server. Please check your connection settings: {connection_details}"
            print(error_message)
            return jsonify({
                'error': error_message,
                'connection_details': connection_details
            }), 500
            
        cursor = conn.cursor()
        print(f"Executing query with cursor: {query}")
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        print(f"Query returned {len(columns)} columns")
        
        # Get data rows
        rows = []
        row_count = 0
        for row in cursor:
            rows.append(row)
            row_count += 1
        
        print(f"Query returned {row_count} rows")
            
        return jsonify({
            'success': True,
            'columns': columns,
            'rows': rows,
            'rowCount': row_count
        })
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        traceback.print_exc()
        error_details = {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'query': query,
            'connection_details': {
                'host': TRINO_HOST,
                'port': TRINO_PORT,
                'user': TRINO_USER,
                'catalog': TRINO_CATALOG,
                'schema': TRINO_SCHEMA
            }
        }
        return jsonify(error_details), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/send_event', methods=['POST'])
def send_event():
    """Send an event to Kafka"""
    try:
        event_data = request.json.get('event_data', {})
        use_docker_method = request.json.get('use_docker_method', True)  # Default to Docker method
        
        # Add default fields if not provided
        if 'id' not in event_data:
            event_data['id'] = str(uuid.uuid4())
        if 'timestamp' not in event_data:
            event_data['timestamp'] = datetime.datetime.now().isoformat()
            
        # Convert dict to JSON string
        event_json = json.dumps(event_data)
        
        if use_docker_method:
            # Use Docker exec to send the event directly to Kafka within the container
            import subprocess
            
            # Escape double quotes in the JSON string
            escaped_json = event_json.replace('"', '\\"')
            
            # Create the command to execute
            command = f'docker exec -i kafka bash -c \'echo "{escaped_json}" | kafka-console-producer --broker-list kafka:9092 --topic {KAFKA_TOPIC}\''
            
            print(f"Executing command: {command}")
            
            # Execute the command
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                error_message = f"Docker command failed: {result.stderr}"
                print(error_message)
                return jsonify({
                    'error': error_message,
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }), 500
            
            print(f"Event sent successfully via Docker command: {event_json}")
            print(f"Command output: {result.stdout}")
            
            return jsonify({
                'success': True,
                'message': 'Event sent successfully via Docker',
                'event': event_data,
                'command_output': result.stdout
            })
        else:
            # Try the original method using Kafka producer
            # Create Kafka producer
            producer = get_kafka_producer()
            if not producer:
                return jsonify({
                    'error': 'Could not create Kafka producer',
                    'details': f'Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}',
                    'simulation_mode': not KAFKA_AVAILABLE
                }), 500
                
            # Send message
            print(f"Sending event to Kafka topic {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}")
            try:
                # Create a dummy check to see if Kafka is truly reachable
                import socket
                for server in KAFKA_BOOTSTRAP_SERVERS.split(','):
                    host, port = server.split(':')
                    print(f"Testing connection to {host}:{port}")
                    
                    try:
                        # Try to connect to the host:port to verify it's reachable
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.settimeout(2)
                        s.connect((host, int(port)))
                        s.close()
                        print(f"Successfully connected to {host}:{port}")
                    except Exception as e:
                        print(f"Failed to connect to {host}:{port}: {str(e)}")
                        if not isinstance(producer, MockProducer):
                            raise Exception(f"Cannot connect to Kafka at {host}:{port}: {str(e)}")
                
                # Now try to produce the message
                producer.produce(KAFKA_TOPIC, key=str(event_data.get('id')), value=event_json)
                producer.flush(timeout=5.0)  # Add timeout to prevent hanging
                print(f"Event sent successfully: {event_json}")
            except Exception as produce_error:
                # Check if we're using a mock producer
                if isinstance(producer, MockProducer):
                    # With mock producer, just log and continue
                    print(f"Mock producer simulated sending event: {event_json}")
                else:
                    # Real producer failed, raise the error
                    raise produce_error
            
            success_message = 'Event sent successfully'
            if not KAFKA_AVAILABLE:
                success_message += ' (in simulation mode)'
                
            return jsonify({
                'success': True,
                'message': success_message,
                'event': event_data,
                'kafka_available': KAFKA_AVAILABLE,
                'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS
            })
    except Exception as e:
        print(f"Error sending event to Kafka: {str(e)}")
        traceback.print_exc()
        
        error_details = {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'kafka_available': KAFKA_AVAILABLE,
            'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS
        }
        return jsonify(error_details), 500

@app.route('/catalogs', methods=['GET'])
def get_catalogs():
    """Get list of available catalogs"""
    try:
        conn = get_trino_connection()
        if not conn:
            return jsonify({'error': 'Could not connect to Trino'}), 500
            
        cursor = conn.cursor()
        cursor.execute("SHOW CATALOGS")
        
        catalogs = [row[0] for row in cursor]
        return jsonify({'catalogs': catalogs})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/schemas', methods=['GET'])
def get_schemas():
    """Get list of schemas for a catalog"""
    catalog = request.args.get('catalog', TRINO_CATALOG)
    
    try:
        conn = get_trino_connection()
        if not conn:
            return jsonify({'error': 'Could not connect to Trino'}), 500
            
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
        
        schemas = [row[0] for row in cursor]
        return jsonify({'schemas': schemas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/tables', methods=['GET'])
def get_tables():
    """Get list of tables for a schema"""
    catalog = request.args.get('catalog', TRINO_CATALOG)
    schema = request.args.get('schema', TRINO_SCHEMA)
    
    try:
        conn = get_trino_connection()
        if not conn:
            return jsonify({'error': 'Could not connect to Trino'}), 500
            
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}")
        
        tables = [row[0] for row in cursor]
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)