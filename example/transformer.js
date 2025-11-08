// Anonymous function example - the script should return a function
(function(event) {
    // if rows and old_rows contains a field called 'password', delete it
    if (event.rows && event.rows.length > 0) {
        event.rows = event.rows.map(row => {
            delete row.password;
            return row;
        });
    }
    if (event.old_rows && event.old_rows.length > 0) {
        event.old_rows = event.old_rows.map(row => {
            delete row.password;
            return row;
        });
    }
    
    // Example: Use NATS KV to store/retrieve data
    // Note: Make sure the KV bucket exists in NATS JetStream before using
    // You can create it with: nats kv add mybucket
    try {
        // Store event metadata in KV store
        const eventKey = `${event.database}.${event.table}.${event.timestamp}`;
        nats.kv.put('mybucket', eventKey, JSON.stringify({
            type: event.type,
            timestamp: event.timestamp
        }));
        
        // Retrieve a value from KV store (example)
        // const cached = nats.kv.get('mybucket', 'some-key');
        // if (cached) {
        //     event.cached_data = JSON.parse(cached);
        // }
    } catch (err) {
        // Handle errors gracefully - KV operations may fail if bucket doesn't exist
        // or JetStream is not enabled
        console.error('NATS KV error:', err);
    }
    
    // Example: Publish to another NATS subject
    // This allows you to send transformed events to different subjects
    try {
        if (event.type === 'INSERT') {
            // Publish INSERT events to a separate subject
            nats.publish('cdc.mysql.inserts', JSON.stringify(event));
        } else if (event.type === 'UPDATE') {
            // Publish UPDATE events to a separate subject
            nats.publish('cdc.mysql.updates', JSON.stringify(event));
        }
    } catch (err) {
        // Handle errors gracefully
        console.error('NATS publish error:', err);
    }
    
    return event;
})