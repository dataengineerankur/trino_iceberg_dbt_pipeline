// Initialize when document is ready
$(document).ready(function() {
    // Load connection settings from localStorage if available
    loadConnectionSettings();
    // Initialize CodeMirror for SQL Editor
    const sqlEditor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
        mode: 'text/x-sql',
        theme: 'dracula',
        lineNumbers: true,
        indentWithTabs: false,
        indentUnit: 4,
        smartIndent: true,
        matchBrackets: true,
        autofocus: true,
        extraKeys: {
            'Ctrl-Enter': executeQuery,
            'Cmd-Enter': executeQuery
        }
    });

    // Initialize CodeMirror for Event Editor
    const eventEditor = CodeMirror.fromTextArea(document.getElementById('event-editor'), {
        mode: {name: 'javascript', json: true},
        theme: 'dracula',
        lineNumbers: true,
        indentWithTabs: false,
        indentUnit: 2,
        smartIndent: true,
        matchBrackets: true
    });

    // Load catalogs when the Schema tab is shown
    $('#schema-tab').on('shown.bs.tab', function() {
        loadCatalogs();
    });

    // Button click handlers
    $('#execute-query').click(executeQuery);
    $('#clear-editor').click(() => {
        sqlEditor.setValue('');
        sqlEditor.focus();
    });

    $('#send-event').click(() => sendEvent(true));  // Send via Docker method
    $('#send-event-direct').click(() => sendEvent(false));  // Send directly
    $('#clear-event').click(() => {
        eventEditor.setValue('{\n  "id": ' + Math.floor(Math.random() * 9000 + 1000) + ',\n  "name": "New Event",\n  "timestamp": "' + new Date().toISOString() + '"\n}');
        eventEditor.focus();
    });

    // Sample query click handlers
    $('.sample-query').click(function(e) {
        e.preventDefault();
        const query = $(this).data('query');
        sqlEditor.setValue(query);
        sqlEditor.focus();
    });

    // Template event click handlers
    $('.template-event').click(function(e) {
        e.preventDefault();
        const eventData = $(this).data('event');
        // Generate pretty JSON with 2 space indentation
        eventEditor.setValue(JSON.stringify(eventData, null, 2));
        eventEditor.focus();
    });

    // Function to execute SQL query
    function executeQuery() {
        const query = sqlEditor.getValue().trim();
        
        if (!query) {
            showQueryError('Please enter a SQL query.');
            return;
        }
        
        // Show loading indicator
        $('#results-area').html('<div class="text-center my-5"><div class="spinner-border text-primary" role="status"></div><p class="mt-2">Executing query...</p></div>');
        
        // Hide any previous error
        $('#query-error').addClass('d-none');
        
        // Execute query via AJAX
        $.ajax({
            url: '/execute_query',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({ query: query }),
            success: function(response) {
                displayQueryResults(response);
            },
            error: function(xhr) {
                let errorMessage = 'An error occurred while executing the query.';
                try {
                    const response = JSON.parse(xhr.responseText);
                    if (response.error) {
                        errorMessage = response.error;
                    }
                    if (response.traceback) {
                        errorMessage += '\n\n' + response.traceback;
                    }
                } catch (e) {
                    console.error('Error parsing error response:', e);
                }
                showQueryError(errorMessage);
                
                // Show empty results
                $('#results-area').html('<div class="alert alert-warning">Query failed. See error details above.</div>');
            }
        });
    }

    // Function to send event to Kafka
    function sendEvent(useDockerMethod) {
        // If useDockerMethod parameter wasn't passed, use the checkbox value
        if (useDockerMethod === undefined) {
            useDockerMethod = $('#use-docker-method').prop('checked');
        } else {
            // Update the checkbox to match the parameter
            $('#use-docker-method').prop('checked', useDockerMethod);
        }
        
        let eventData;
        try {
            eventData = JSON.parse(eventEditor.getValue());
        } catch (e) {
            showEventError('Invalid JSON: ' + e.message);
            return;
        }
        
        // Hide previous messages
        $('#event-error').addClass('d-none');
        $('#event-success').addClass('d-none');
        
        // Show loading indicator in the event history
        const loadingItem = $('<div class="list-group-item bg-light">');
        loadingItem.append('<div class="spinner-border spinner-border-sm text-primary me-2" role="status"></div>');
        loadingItem.append(`<span>Sending event${useDockerMethod ? ' via Docker' : ' directly'}...</span>`);
        
        if ($('#event-history .text-muted').length) {
            $('#event-history').empty();
        }
        $('#event-history').prepend(loadingItem);
        
        // Send event via AJAX
        $.ajax({
            url: '/send_event',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({ 
                event_data: eventData,
                use_docker_method: useDockerMethod
            }),
            success: function(response) {
                // Remove loading indicator
                loadingItem.remove();
                
                // Show success message
                const methodText = useDockerMethod ? 'via Docker' : 'directly';
                $('#event-success').removeClass('d-none')
                    .html(`Event sent successfully ${methodText}!${response.command_output ? '<pre class="mt-2 small">' + response.command_output + '</pre>' : ''}`);
                
                // Add to history
                addEventToHistory(response.event, useDockerMethod);
                
                // Suggest checking Trino
                setTimeout(() => {
                    const query = "SELECT * FROM kafka.default.events_topic WHERE _message LIKE '%" + eventData.id + "%' LIMIT 10";
                    const checkTrinoHtml = `
                        <div class="mt-2">
                            <button class="btn btn-sm btn-outline-primary check-event-in-trino" 
                                data-query="${query}">
                                Check this event in Trino
                            </button>
                        </div>`;
                    $('#event-success').append(checkTrinoHtml);
                    
                    // Attach click handler to the new button
                    $('.check-event-in-trino').click(function() {
                        const queryToRun = $(this).data('query');
                        sqlEditor.setValue(queryToRun);
                        $('#sql-tab').tab('show');
                        setTimeout(executeQuery, 100);
                    });
                }, 500);
            },
            error: function(xhr) {
                // Remove loading indicator
                loadingItem.remove();
                
                let errorMessage = 'An error occurred while sending the event.';
                try {
                    const response = JSON.parse(xhr.responseText);
                    if (response.error) {
                        errorMessage = response.error;
                    }
                    if (response.traceback) {
                        errorMessage += '\n\n' + response.traceback;
                    }
                    if (response.stderr) {
                        errorMessage += '\n\nStderr: ' + response.stderr;
                    }
                } catch (e) {
                    console.error('Error parsing error response:', e);
                }
                showEventError(errorMessage);
                
                // Add a retry button with the alternative method
                const alternativeMethod = !useDockerMethod;
                const retryHtml = `
                    <div class="mt-3">
                        <button id="retry-alternative" class="btn btn-warning">
                            Retry using ${alternativeMethod ? 'Docker method' : 'direct method'}
                        </button>
                    </div>`;
                $('#event-error').append(retryHtml);
                
                $('#retry-alternative').click(function() {
                    sendEvent(alternativeMethod);
                });
            }
        });
    }

    // Function to add event to history
    function addEventToHistory(event, useDockerMethod) {
        // Clear "no events" message if present
        if ($('#event-history .text-muted').length) {
            $('#event-history').empty();
        }
        
        // Format timestamp
        const timestamp = new Date(event.timestamp).toLocaleString();
        
        // Create event item
        const eventItem = $('<div class="list-group-item event-item">');
        
        // Add badge to indicate sending method
        const methodBadge = useDockerMethod ? 
            '<span class="badge bg-primary float-end">Docker</span>' : 
            '<span class="badge bg-secondary float-end">Direct</span>';
        eventItem.append(methodBadge);
        
        eventItem.append('<div class="event-timestamp">' + timestamp + '</div>');
        eventItem.append('<div class="fw-bold">' + (event.name || 'Unnamed Event') + ' (ID: ' + event.id + ')</div>');
        
        // Create event details
        const eventDetails = $('<pre class="mb-0 mt-2">');
        eventDetails.text(JSON.stringify(event, null, 2));
        eventItem.append(eventDetails);
        
        // Add query link
        const queryLink = $(`<a href="#" class="btn btn-sm btn-outline-primary mt-2">Query this event in Trino</a>`);
        queryLink.click(function(e) {
            e.preventDefault();
            const query = `SELECT * FROM kafka.default.events_topic WHERE _message LIKE '%${event.id}%' LIMIT 10`;
            sqlEditor.setValue(query);
            $('#sql-tab').tab('show');
            setTimeout(executeQuery, 100);
        });
        eventItem.append(queryLink);
        
        // Add to history (prepend to show newest first)
        $('#event-history').prepend(eventItem);
    }

    // Function to load catalogs
    function loadCatalogs() {
        $.ajax({
            url: '/catalogs',
            type: 'GET',
            success: function(response) {
                displayCatalogs(response.catalogs);
            },
            error: function() {
                $('#catalogs-list').html('<li class="list-group-item text-danger">Error loading catalogs</li>');
            }
        });
    }

    // Function to load schemas for a catalog
    function loadSchemas(catalog) {
        $.ajax({
            url: '/schemas',
            type: 'GET',
            data: { catalog: catalog },
            success: function(response) {
                displaySchemas(response.schemas, catalog);
            },
            error: function() {
                $('#schemas-list').html('<li class="list-group-item text-danger">Error loading schemas</li>');
            }
        });
    }

    // Function to load tables for a schema
    function loadTables(catalog, schema) {
        $.ajax({
            url: '/tables',
            type: 'GET',
            data: { catalog: catalog, schema: schema },
            success: function(response) {
                displayTables(response.tables, catalog, schema);
            },
            error: function() {
                $('#tables-list').html('<li class="list-group-item text-danger">Error loading tables</li>');
            }
        });
    }

    // Function to display catalogs
    function displayCatalogs(catalogs) {
        const catalogsList = $('#catalogs-list');
        catalogsList.empty();
        
        if (!catalogs || catalogs.length === 0) {
            catalogsList.html('<li class="list-group-item text-muted">No catalogs found</li>');
            return;
        }
        
        catalogs.forEach(catalog => {
            const item = $('<li class="list-group-item catalog-item">').text(catalog);
            item.click(function() {
                $('.catalog-item').removeClass('active');
                $(this).addClass('active');
                loadSchemas(catalog);
            });
            catalogsList.append(item);
        });
    }

    // Function to display schemas
    function displaySchemas(schemas, catalog) {
        const schemasList = $('#schemas-list');
        schemasList.empty();
        
        if (!schemas || schemas.length === 0) {
            schemasList.html('<li class="list-group-item text-muted">No schemas found</li>');
            return;
        }
        
        schemas.forEach(schema => {
            const item = $('<li class="list-group-item schema-item">').text(schema);
            item.click(function() {
                $('.schema-item').removeClass('active');
                $(this).addClass('active');
                loadTables(catalog, schema);
            });
            schemasList.append(item);
        });
    }

    // Function to display tables
    function displayTables(tables, catalog, schema) {
        const tablesList = $('#tables-list');
        tablesList.empty();
        
        if (!tables || tables.length === 0) {
            tablesList.html('<li class="list-group-item text-muted">No tables found</li>');
            return;
        }
        
        tables.forEach(table => {
            const item = $('<li class="list-group-item table-item">').text(table);
            item.click(function() {
                $('.table-item').removeClass('active');
                $(this).addClass('active');
                showTableDetails(catalog, schema, table);
            });
            tablesList.append(item);
        });
    }

    // Function to show table details
    function showTableDetails(catalog, schema, table) {
        const tableDetails = $('#table-details');
        tableDetails.html('<div class="text-center my-3"><div class="spinner-border text-primary" role="status"></div><p class="mt-2">Loading table details...</p></div>');
        
        // Execute describe query
        const query = `DESCRIBE ${catalog}.${schema}.${table}`;
        
        $.ajax({
            url: '/execute_query',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({ query: query }),
            success: function(response) {
                displayTableDetails(response, catalog, schema, table);
            },
            error: function() {
                tableDetails.html('<div class="alert alert-danger">Error loading table details</div>');
            }
        });
    }

    // Function to display table details
    function displayTableDetails(response, catalog, schema, table) {
        const tableDetails = $('#table-details');
        tableDetails.empty();
        
        // Create table name header
        tableDetails.append(`<h4>${catalog}.${schema}.${table}</h4>`);
        
        // Create actions buttons
        const actionsDiv = $('<div class="mb-3">');
        
        const viewDataBtn = $('<button class="btn btn-sm btn-primary me-2">View Data</button>');
        viewDataBtn.click(function() {
            sqlEditor.setValue(`SELECT * FROM ${catalog}.${schema}.${table} LIMIT 100;`);
            $('#sql-tab').tab('show');
            setTimeout(executeQuery, 100);
        });
        
        const copyQueryBtn = $('<button class="btn btn-sm btn-outline-secondary">Copy Query</button>');
        copyQueryBtn.click(function() {
            sqlEditor.setValue(`SELECT * FROM ${catalog}.${schema}.${table} LIMIT 100;`);
            $('#sql-tab').tab('show');
        });
        
        actionsDiv.append(viewDataBtn, copyQueryBtn);
        tableDetails.append(actionsDiv);
        
        // Create table schema
        if (response.columns && response.columns.length > 0 && response.rows && response.rows.length > 0) {
            const table = $('<table class="table table-sm table-bordered">');
            
            // Table header
            const thead = $('<thead>');
            const headerRow = $('<tr>');
            response.columns.forEach(column => {
                headerRow.append($('<th>').text(column));
            });
            thead.append(headerRow);
            table.append(thead);
            
            // Table body
            const tbody = $('<tbody>');
            response.rows.forEach(row => {
                const dataRow = $('<tr>');
                row.forEach(cell => {
                    dataRow.append($('<td>').text(cell === null ? 'NULL' : cell));
                });
                tbody.append(dataRow);
            });
            table.append(tbody);
            
            tableDetails.append(table);
        } else {
            tableDetails.append('<div class="alert alert-warning">No schema information available</div>');
        }
    }

    // Function to display query results
    function displayQueryResults(response) {
        const resultsArea = $('#results-area');
        resultsArea.empty();
        
        // Check if we have results
        if (!response.columns || !response.rows) {
            resultsArea.html('<div class="alert alert-info">Query executed successfully. No results returned.</div>');
            return;
        }
        
        // Create results table
        const table = $('<table class="table table-striped table-bordered" id="results-table">');
        
        // Create table header
        const thead = $('<thead>');
        const headerRow = $('<tr>');
        response.columns.forEach(column => {
            headerRow.append($('<th>').text(column));
        });
        thead.append(headerRow);
        table.append(thead);
        
        // Create table body
        const tbody = $('<tbody>');
        response.rows.forEach(row => {
            const dataRow = $('<tr>');
            row.forEach(cell => {
                dataRow.append($('<td>').text(cell === null ? 'NULL' : cell));
            });
            tbody.append(dataRow);
        });
        table.append(tbody);
        
        // Add results summary
        const summary = $('<div class="mb-3">');
        summary.append(`<span class="badge bg-success">${response.rowCount} rows returned</span>`);
        
        resultsArea.append(summary);
        resultsArea.append(table);
        
        // Initialize DataTables
        $('#results-table').DataTable({
            pageLength: 10,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
            scrollX: true,
            dom: '<"d-flex justify-content-between"<"d-flex"l<"ms-3"f>>t<"d-flex justify-content-between"<"d-flex"i<"ms-3"p>>>'
        });
    }

    // Function to show query error
    function showQueryError(message) {
        $('#query-error').removeClass('d-none').text(message);
    }

    // Function to show event error
    function showEventError(message) {
        $('#event-error').removeClass('d-none').text(message);
    }
    
    // Handle connection settings form submission
    $('#trino-settings-form').submit(function(e) {
        e.preventDefault();
        const settings = {
            trino_host: $('#trino-host').val(),
            trino_port: $('#trino-port').val(),
            trino_user: $('#trino-user').val(),
            trino_catalog: $('#trino-catalog').val(),
            trino_schema: $('#trino-schema').val()
        };
        
        // Save settings to localStorage
        localStorage.setItem('trino_settings', JSON.stringify(settings));
        
        // Update server-side settings
        $.ajax({
            url: '/update_settings',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(settings),
            success: function(response) {
                $('#connection-status').html(
                    '<div class="alert alert-success">Trino settings updated successfully!</div>'
                );
                setTimeout(() => {
                    $('#connection-status .alert').fadeOut();
                }, 3000);
            },
            error: function(xhr) {
                $('#connection-status').html(
                    '<div class="alert alert-danger">Failed to update settings: ' + xhr.responseText + '</div>'
                );
            }
        });
    });
    
    // Handle Kafka settings form submission
    $('#kafka-settings-form').submit(function(e) {
        e.preventDefault();
        const settings = {
            kafka_bootstrap_servers: $('#kafka-bootstrap-servers').val(),
            kafka_topic: $('#kafka-topic').val()
        };
        
        // Save settings to localStorage
        localStorage.setItem('kafka_settings', JSON.stringify(settings));
        
        // Show loading indicator
        $('#connection-status').html(
            '<div class="alert alert-info">Updating Kafka settings...</div>'
        );
        
        // Update server-side settings
        $.ajax({
            url: '/update_settings',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(settings),
            success: function(response) {
                // Show success message with Kafka connection status if available
                let statusMessage = 'Kafka settings updated successfully! ';
                
                if (response.kafka_status) {
                    if (response.kafka_status.available) {
                        statusMessage += '<div class="mt-2 alert alert-success">Successfully connected to Kafka at ' + 
                            response.settings.kafka_bootstrap_servers + '</div>';
                    } else {
                        statusMessage += '<div class="mt-2 alert alert-warning">Could not connect to Kafka: ' + 
                            response.kafka_status.message + '</div>';
                    }
                }
                
                statusMessage += 'Page will refresh in 2 seconds...';
                
                $('#connection-status').html(
                    '<div class="alert alert-info">' + statusMessage + '</div>'
                );
                
                // Confirm the settings were applied
                console.log('Updated Kafka settings:', response.settings);
                console.log('Kafka status:', response.kafka_status);
                
                // Force reload the page to ensure new settings are applied
                setTimeout(() => {
                    window.location.reload();
                }, 2000);
            },
            error: function(xhr) {
                $('#connection-status').html(
                    '<div class="alert alert-danger">Failed to update settings: ' + xhr.responseText + '</div>'
                );
            }
        });
    });
    
    // Test Trino connection
    $('#test-connection').click(function() {
        $('#connection-status').html(
            '<div class="alert alert-info">Testing connection to Trino...</div>'
        );
        
        $.ajax({
            url: '/test_connection',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({}),
            success: function(response) {
                $('#connection-status').html(
                    '<div class="alert alert-success">' + response.message + '</div>'
                );
            },
            error: function(xhr) {
                try {
                    const response = JSON.parse(xhr.responseText);
                    let errorMessage = response.message;
                    if (response.traceback) {
                        errorMessage += '<pre class="mt-2 small">' + response.traceback + '</pre>';
                    }
                    $('#connection-status').html(
                        '<div class="alert alert-danger">' + errorMessage + '</div>'
                    );
                } catch (e) {
                    $('#connection-status').html(
                        '<div class="alert alert-danger">Failed to test connection</div>'
                    );
                }
            }
        });
    });
    
    // Function to load connection settings from localStorage
    function loadConnectionSettings() {
        // Load Trino settings
        try {
            const savedTrinoSettings = localStorage.getItem('trino_settings');
            if (savedTrinoSettings) {
                const settings = JSON.parse(savedTrinoSettings);
                $('#trino-host').val(settings.trino_host || 'localhost');
                $('#trino-port').val(settings.trino_port || 8080);
                $('#trino-user').val(settings.trino_user || 'trino');
                $('#trino-catalog').val(settings.trino_catalog || 'iceberg');
                $('#trino-schema').val(settings.trino_schema || 'default');
                
                // Update server-side settings
                $.ajax({
                    url: '/update_settings',
                    type: 'POST',
                    contentType: 'application/json',
                    data: JSON.stringify(settings),
                    error: function(xhr) {
                        console.error('Failed to update Trino settings on server:', xhr.responseText);
                    }
                });
            }
        } catch (e) {
            console.error('Error loading Trino settings:', e);
        }
        
        // Load Kafka settings
        try {
            const savedKafkaSettings = localStorage.getItem('kafka_settings');
            if (savedKafkaSettings) {
                const settings = JSON.parse(savedKafkaSettings);
                $('#kafka-bootstrap-servers').val(settings.kafka_bootstrap_servers || 'localhost:9092');
                $('#kafka-topic').val(settings.kafka_topic || 'events_topic');
                
                // Update server-side settings
                $.ajax({
                    url: '/update_settings',
                    type: 'POST',
                    contentType: 'application/json',
                    data: JSON.stringify(settings),
                    error: function(xhr) {
                        console.error('Failed to update Kafka settings on server:', xhr.responseText);
                    }
                });
            }
        } catch (e) {
            console.error('Error loading Kafka settings:', e);
        }
    }
});