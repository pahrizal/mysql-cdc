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
    return event;
})