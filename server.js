/**
@description
<pre>
Heroku REST API for data integration
<br/>
2020-01-23      LCS-BGO     Ver: 0.1.0  -   Created
</pre>
@author Hawkers
@date 2020/01/23
*/

// REST services
var app = require( "express" )();
var httpServer = require( "http" ).Server( app );


var bodyParser = require( "body-parser" );
app.use( bodyParser.text( { limit: '50mb', type: 'application/json' } ) ); // JSON bodies are treated as text to be injected into the DMLs
app.use( bodyParser.urlencoded( { limit: '50mb', extended: true } ) );

var multer = require( "multer" );
var upload = multer();

const ROOT_PATH = "/data-api";
const PORT = 3000;

// Security
var helmet = require( "helmet" );
app.use( helmet() );

var token = "KUypCxw+uNWkWNeeAL=@Yp5-QGMc&mSkxs9%t?gjBErytrms_wtEPMN^S8MKg!QwyD@W!=g9UUrq*p+fY*-eeEB@95TZ!rG%jp+C3F_^C%rnS88qPTu+QjBgCqPCCRtXS+DHQQ-5ecdts&pXjP-Hz+g=JJSxq$2RFe*HG#ku69V-HjQM?xy-QWw&byP%%$gvm?*?g-qBT^PLHVdxt*y+KNkH%PAgcM-EH8k9@J&5LPZdPHj$a6me+YZJ^J_wX#f";

// -------------------------------------------------------------------------------------------------------------------------
// --------------- CONNECTION TO DATABASE ----------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------------------------------

// Schemas
var BUFFER_SCHEMA = "global_buffer";
var BUFFER_SCHEMA_SANDBOX = "global_buffer_sandbox";
var SALESFORCE_SCHEMA = "sf_mirror";
var SALESFORCE_SCHEMA_SANDBOX = "sf_mirror_sandbox";

// https://devcenter.heroku.com/articles/heroku-postgresql#connecting-in-node-js
var pg = require( "pg" );
pg.defaults.ssl = true;
// var dbURL = "postgres://ub3sn6j7hnsapl:p6ac34ea3e5868be0f78e8b2341053f1b672694973ddfc4eaaed7966b56a82df4@ec2-34-246-254-183.eu-west-1.compute.amazonaws.com:5432/d9atqk42arg1jd?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
var dbURL = "postgres://sfsc_dbuser:NNbK8LjlB4c09btN@35.198.189.51:5432/sfsc_db";
var syncPeriod = "0";

httpServer.listen( PORT, function(){
        console.log(`Listening on ${ PORT }`);
});


// -------------------------------------------------------------------------------------------------------------------------
// --------------- API ENDPOINTS -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------------------------------
// TODO: http://stackoverflow.com/questions/8484404/what-is-the-proper-way-to-use-the-node-js-postgresql-module


// --------------- ECHO ----------------------------------------------------------------------------------------------------

/* Echo endpoint for testing purposes */
app.get(
        ROOT_PATH + "/echo",
        function( req, res ) {
          res.send( "esto es una prueba" );
        }
);


// --------------- SHOPIFY BUFFER ------------------------------------------------------------------------------------------

// ---------- CUSTOMERS ----------

/**
[GET] Customers data retrieval

Expected URIs:
 -      [/sandbox]/buffer/(ENTITY NAME)
                to get a list of records

URI parameters:
 -      fromtime:               minimum last modified date of data to be retrieved
 -      datalength:             maximum number of records to be retrieved
*/
app.get(
        ROOT_PATH + "(/sandbox)?/buffer/customers",
        function( req, res ) {

                logRequest( req );

                try {
                // Authentication check
                        if( checkAuth( req, res ) ) {

                                // TargetEntity (schema.table)
                                var targetEntity = (req.originalUrl.startsWith( ROOT_PATH + '/sandbox/buffer' )? BUFFER_SCHEMA_SANDBOX : BUFFER_SCHEMA)  + ".customer";

                                // Request processing
                                pg.connect(
                                        dbURL,
                                        function( err, client, done ) {

                                                // Database connection error
                                                if( err ) processResponse( res, 500, "DB connection error: " + err );

                                                // Query data
                                                else {

                                                        // Query composition
                                                        var query =
                                                         " SET LOCAL application_name  = 'Salesforce_Sync'; \n"
                                                         +        "WITH customersToSync AS (\n"
                                                         +              "SELECT *\n"
                                                         +              "FROM " + targetEntity + "\n"
                                                         +              "WHERE\n"
                                                         +      "(\n"
                                                         +                      "(( sync_status = 'syncing' AND last_modified_date <= CURRENT_TIMESTAMP - INTERVAL " + "'" + syncPeriod + " hours' )\n"
                                                         +                      " OR ( sync_status = 'not synced' OR sync_status IS NULL) \n"
                                                         +          " OR ( sync_status = 'synced' AND last_modified_date > last_synced_date) \n"
                                                         +          " OR ( sync_status = 'retry') )\n"
                                                         +              ")\n"
                                                         +          " ORDER BY sync_status ASC, last_modified_date ASC\n"
                                                         +              ( req.query.datalength? "LIMIT " + req.query.datalength + "\n" : "" )
                                                         +      "),\n"
                                                         +      "syncedCustomers AS (\n"
                                                         +              "UPDATE " + targetEntity + " SET sync_status  = CASE WHEN " + targetEntity + ".sync_status = 'retry' THEN 'retry' ELSE 'syncing' END\n"
                                                         +              "FROM customersToSync\n"
                                                         +              "WHERE " + targetEntity + ".shop = customersToSync.shop AND " + targetEntity + ".email = customersToSync.email\n"
                                                         +              "RETURNING " + targetEntity + ".*\n"
                                                         +      ") (\n"
                                                         +              "SELECT JSON_AGG( ROW_TO_JSON ( syncedCustomers ) ORDER BY last_modified_date ) AS jsondata FROM syncedCustomers\n"
                                                         +      ")";

                                                        console.log( "Executing query: '" + query + "'\n" );

                                                        // Query execution
                                                        client.query( query, function( err, result ) {

                                                                done();

                                                                // Query error
                                                                if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                // Query success
                                                                else {
                                                                        res.set( "Content-Type", "application/json;charset=UTF-8" );
                                                                        processResponse( res, 200, result.rows[0].jsondata? result.rows[0].jsondata : "[]" );
                                                                }

                                                        });

                                                }

                                        }
                                );

                        }
                } catch (error) {
                        processResponse( res, 502, "Request error getting customers" + error );
                }
        }
);


/**
[POST] Customers creation and update from Shopify webhooks

Expected URIs:
 -      /buffer/customers
                to create or update a list of customers based on email
*/
/*app.post(
        ROOT_PATH + "/buffer/customers",
        function( req, res ) {

                logRequest( req );

                // Authentication check
                if( checkAuth( req, res ) ) {

                        // Request processing
                        pg.connect(
                                dbURL,
                                function( err, client, done ) {

                                        // Database connection error
                                        if( err ) processResponse( res, 500, "DB connection error: " + err );

                                        // Store data
                                        else {

                                                // Retrieve updateable columns
                                                var columnsQuery = composeColumnsQuery_buffer( BUFFER_SCHEMA + ".customer" );

                                                client.query( columnsQuery, function( err, result ) {

                                                        done();

                                                        // Query error
                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                        // Query success
                                                        else {

                                                                // DML composition
                                                                var columnNames = result.rows[0].columns;
                                                                var excludedColumnNames = result.rows[0].excludedcolumns;

                                                                var dml =
                                                                        "WITH json_array AS (\n"
                                                                 +              "SELECT * FROM\n"
                                                                 +              "json_populate_record(\n"
                                                                 +                      "NULL :: " + BUFFER_SCHEMA + ".customer,\n"
                                                                 +                      "'" + req.body + "'\n"
                                                                 +              ")\n"
                                                                 +      ")\n"

                                                                 +      "INSERT INTO " + BUFFER_SCHEMA + ".customer\n"
                                                                 +      "SELECT * FROM json_array\n"

                                                                 +      "ON CONFLICT ( email, shop ) DO UPDATE\n"

                                                                 +      "SET ( " + columnNames + " ) =\n"
                                                                 +      "( " + excludedColumnNames + " )";

                                                                console.log( "Executing DML: '" + dml + "'\n" );

                                                                // DML execution
                                                                client.query( dml, function( err, result ) {

                                                                        done();

                                                                        // DML error
                                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                        // DML success
                                                                        else processResponse( res, 204, '' );

                                                                });
                                                        }
                                                });

                                        }

                                }
                        );

                }
        }
);*/


// ---------- PRODUCTS ----------

/**
[GET] Products data retrieval

Expected URIs:
 -      [/sandbox]/buffer/(ENTITY NAME)
                to get a list of records

URI parameters:
 -      fromtime:               minimum last modified date of data to be retrieved
 -      datalength:             maximum number of records to be retrieved
*/
app.get(
        ROOT_PATH + "(/sandbox)?/buffer/products",
        function( req, res ) {

                logRequest( req );

                try {

                // Authentication check
                        if( checkAuth( req, res ) ) {

                                // TargetEntity (schema.table)
                                var targetEntity = (req.originalUrl.startsWith( ROOT_PATH + '/sandbox/buffer' )? BUFFER_SCHEMA_SANDBOX : BUFFER_SCHEMA)  + ".products";

                                // Request processing
                                pg.connect(
                                        dbURL,
                                        function( err, client, done ) {

                                                // Database connection error
                                                if( err ) processResponse( res, 500, "DB connection error: " + err );

                                                // Query data
                                                else {

                                                        // Query composition
                                                        var query =
                                                        " SET LOCAL application_name  = 'Salesforce_Sync'; \n"
                                                         +              "WITH ordersToSync AS (\n"
                                                         +              "SELECT *\n"
                                                         +              "FROM " + targetEntity + "\n"
                                                         +              "WHERE\n"
                                                         +      "(\n"
                                                         +                      "(( sync_status = 'syncing' AND last_modified_date <= CURRENT_TIMESTAMP - INTERVAL " + "'" + syncPeriod + " hours' )\n"
                                                         +                      " OR ( sync_status = 'not synced' OR sync_status IS NULL) \n"
                                                         +          " OR ( sync_status = 'synced' AND last_modified_date > last_synced_date) \n"
                                                         +          " OR ( sync_status = 'retry')) \n"
                                                         +              ")\n"
                                                         +          " ORDER BY sync_status ASC, last_modified_date ASC\n"
                                                         +              ( req.query.datalength? "LIMIT " + req.query.datalength + "\n" : "" )
                                                         +      "),\n"
                                                         +      "syncedOrders AS (\n"
                                                         +              "UPDATE " + targetEntity + " SET sync_status  = CASE WHEN " + targetEntity + ".sync_status = 'retry' THEN 'retry' ELSE 'syncing' END\n"
                                                         +              "FROM ordersToSync\n"
                                                         +              "WHERE " + targetEntity + ".shop = ordersToSync.shop AND " + targetEntity + ".sku = ordersToSync.sku\n"
                                                         +              "RETURNING " + targetEntity + ".*\n"
                                                         +      ") (\n"
                                                         +      "SELECT\n"
                                                         +              "JSON_AGG( ROW_TO_JSON ( syncedOrders ) ORDER BY last_modified_date ) AS jsondata\n"
                                                         +      "FROM syncedOrders\n"
                                                         +      ")";

                                                        console.log( "Executing query: '" + query + "'\n" );

                                                        // Query execution
                                                        client.query( query, function( err, result ) {

                                                                done();

                                                                // Query error
                                                                if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                // Query success
                                                                else {
                                                                        res.set( "Content-Type", "application/json;charset=UTF-8" );
                                                                        processResponse( res, 200, result.rows[0].jsondata? result.rows[0].jsondata : "[]" );
                                                                }

                                                        });

                                                }

                                        }
                                );

                        }
                }catch (error) {
                        processResponse( res, 502, "Request error getting products" + error );
                }
        }
);


/**
[POST] Products creation and update from Shopify webhooks

Expected URIs:
 -      /buffer/products
                to create or update a list of products based on sku and market
*/
/*app.post(
        ROOT_PATH + "/buffer/products",
        function( req, res ) {

                logRequest( req );

                // Authentication check
                if( checkAuth( req, res ) ) {

                        // Request processing
                        pg.connect(
                                dbURL,
                                function( err, client, done ) {

                                        // Database connection error
                                        if( err ) processResponse( res, 500, "DB connection error: " + err );

                                        // Store data
                                        else {

                                                // Retrieve updateable columns
                                                var columnsQuery = composeColumnsQuery_buffer( BUFFER_SCHEMA + ".products" );

                                                client.query( columnsQuery, function( err, result ) {

                                                        done();

                                                        // Query error
                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                        // Query success
                                                        else {

                                                                var columnNames = result.rows[0].columns;
                                                                var excludedColumnNames = result.rows[0].excludedcolumns;

                                                                // DML composition
                                                                var dml =
                                                                        "WITH json_array AS (\n"
                                                                 +              "SELECT * FROM\n"
                                                                 +              "json_populate_recordset(\n"
                                                                 +                      "NULL :: " + BUFFER_SCHEMA + ".products,\n"
                                                                 +                      "'" + req.body + "'\n"
                                                                 +              ")\n"
                                                                 +      ")\n"

                                                                 +      "INSERT INTO " + BUFFER_SCHEMA + ".products\n"
                                                                 +      "SELECT * FROM json_array\n"

                                                                 +      "ON CONFLICT ( sku, shop ) DO UPDATE\n"

                                                                 +      "SET ( " + columnNames + " ) =\n"
                                                                 +      "( " + excludedColumnNames + " )";

                                                                console.log( "Executing DML: '" + dml + "'\n" );

                                                                // DML execution
                                                                client.query( dml, function( err, result ) {

                                                                        done();

                                                                        // DML error
                                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                        // DML success
                                                                        else processResponse( res, 204, '' );

                                                                });
                                                        }
                                                });

                                        }

                                }
                        );

                }
        }
);*/


// ---------- ORDERS ----------

/**
[GET] Orders data retrieval

Expected URIs:
 -      [/sandbox]/buffer/(ENTITY NAME)
                to get a list of records

URI parameters:
 -      fromtime:               minimum last modified date of orders to be retrieved
 -      datalength:             maximum number of records to be retrieved. The last order whose items fit into this parameter will be the
                                        last order retrieved
*/
// ---------- ORDERS ----------

/**
[GET] Orders data retrieval

Expected URIs:
 -      [/sandbox]/buffer/(ENTITY NAME)
                to get a list of records

URI parameters:
 -      fromtime:               minimum last modified date of orders to be retrieved
 -      datalength:             maximum number of records to be retrieved. The last order whose items fit into this parameter will be the
                                        last order retrieved
*/
app.get(
        ROOT_PATH + "(/sandbox)?/buffer/orders",
        function( req, res ) {

                logRequest( req );

                try {
                // Authentication check
                        if( checkAuth( req, res ) ) {

                                // TargetEntity (schema.table)
                                var targetEntityOrders = (req.originalUrl.startsWith( ROOT_PATH + '/sandbox/buffer' )? BUFFER_SCHEMA_SANDBOX : BUFFER_SCHEMA) + ".orders";
                                var targetEntityLineItems = (req.originalUrl.startsWith( ROOT_PATH + '/sandbox/buffer' )? BUFFER_SCHEMA_SANDBOX : BUFFER_SCHEMA) + ".lineitems";

                                // Request processing
                                pg.connect(
                                        dbURL,
                                        function( err, client, done ) {

                                                // Database connection error
                                                if( err ) processResponse( res, 500, "DB connection error: " + err );

                                                // Query data
                                                else {

                                                        // Main query
                                                        var query =
                                                         " SET LOCAL application_name  = 'Salesforce_Sync'; \n"
                                                         +      "WITH orderIdsResult AS (\n"
                                                         +      "SELECT aggCount, syncStatus, id, shop, shopifyId\n"
                                                         +      "FROM (\n"
                                                         +              "SELECT id, SUM(nlineitems) OVER (ROWS UNBOUNDED PRECEDING) AS aggCount, syncStatus, shop, shopifyId\n"
                                                         +              "FROM (\n"
                                                         +                      "SELECT\n"
                                                         +                              "gbOrders.last_modified_date, gbOrders.id, gbOrders.sync_status as syncStatus,gbOrders.last_synced_date, gbOrders.shop AS shop, gbOrders.shopify_id as shopifyId, COUNT( gbLineItems.id )+1 AS nlineitems\n"
                                                         +                      "FROM " + targetEntityOrders + " AS gbOrders\n"
                                                         +                      "LEFT JOIN " + targetEntityLineItems + " AS gbLineItems ON ( gbLineItems.order_id = gbOrders.id AND gbLineItems.shop = gbOrders.shop )\n"
                                                         +                      "WHERE\n"
                                                         +              "(\n"
                                                         +                              "((gbOrders.sync_status = 'syncing' AND gbOrders.last_modified_date <= CURRENT_TIMESTAMP - INTERVAL " + "'" + syncPeriod + " hours' )\n"
                                                         +                              " OR ( gbOrders.sync_status = 'not synced' OR gbOrders.sync_status IS NULL) \n"
                                                         +              " OR ( gbOrders.sync_status = 'synced' AND gbOrders.last_modified_date > gbOrders.last_synced_date) \n"
                                                         +              " OR ( sync_status = 'retry')) \n"
                                                         +                      ")\n"
                                                         +                      "GROUP BY gbOrders.last_modified_date, gbOrders.id, gbOrders.sync_status, gbOrders.last_synced_date, gbOrders.shop,  gbOrders.shopify_id\n"
                                                         +                      "ORDER BY gbOrders.sync_status ASC, gbOrders.last_modified_date ASC \n"
                                                         +              ") AS aux\n"
                                                         +              ") AS aux2\n"
                                                         +              ( req.query.datalength != null? "WHERE aggCount <= " + req.query.datalength + "\n" : "" )
                                                         +      "),\n"
                                                         +      "syncedOrders AS (\n"
                                                         +              "UPDATE " + targetEntityOrders + " SET sync_status  = CASE WHEN " + targetEntityOrders + ".sync_status = 'retry' THEN 'retry' ELSE 'syncing' END\n"
                                                         +              "FROM orderIdsResult\n"
                                                         +              "WHERE " + targetEntityOrders + ".shop = orderIdsResult.shop AND " + targetEntityOrders + ".shopify_id = orderIdsResult.shopifyId\n"
                                                         +              "RETURNING " + targetEntityOrders + ".*\n"
                                                         +      ") \n"

                                                         +      "SELECT\n"
                                                         +              "JSON_AGG( ROW_TO_JSON( orders ) ORDER BY orders.last_modified_date ) AS jsondata\n"
                                                         +              "FROM (\n"
                                                         +                      "SELECT\n"
                                                         +                              "orders.*,\n"
                                                         +                              "(\n"
                                                         +                                      "SELECT JSON_AGG( ROW_TO_JSON( items ) ORDER BY items.id )\n"
                                                         +                                      "FROM " + targetEntityLineItems + " AS items\n"
                                                         +                                      "WHERE ( items.order_id = orders.id AND items.shop = orders.shop )\n"
                                                         +                              ") AS items\n"
                                                         +                      "FROM " + targetEntityOrders + " AS orders\n"
                                                         +                      "WHERE id IN ( SELECT id FROM orderIdsResult )\n"
                                                         +              ") AS orders";

                                                        console.log( "Executing query: '" + query + "'\n" );

                                                        // Query execution
                                                        client.query( query, function( err, result ) {

                                                                done();

                                                                // Query error
                                                                if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                // Query success
                                                                else {
                                                                        res.set( "Content-Type", "application/json;charset=UTF-8" );
                                                                        processResponse( res, 200, result.rows[0].jsondata? result.rows[0].jsondata : "[]" );
                                                                }

                                                        });

                                                }

                                        }
                                );

                        }
                } catch (error) {
                        processResponse( res, 502, "Request error getting orders" + error );
                }
        }
);

/**
[POST] Orders creation and update from Shopify webhooks

Expected URIs:
 -      /buffer/orders
                to create or update a list of orders based order id and market
*/
/*app.post(
        ROOT_PATH + "/buffer/orders",
        function( req, res ) {

                logRequest( req );

                // Authentication check
                if( checkAuth( req, res ) ) {

                        // Request processing
                        pg.connect(
                                dbURL,
                                function( err, client, done ) {

                                        // Database connection error
                                        if( err ) processResponse( res, 500, "DB connection error: " + err );

                                        // Store data
                                        else {

                                                // Retrieve updateable columns
                                                var columnsQuery = composeColumnsQuery_buffer( BUFFER_SCHEMA + ".orders" );

                                                client.query( columnsQuery, function( err, result ) {

                                                        done();

                                                        // Query error
                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                        // Query success
                                                        else {

                                                                var columnNames = result.rows[0].columns;
                                                                var excludedColumnNames = result.rows[0].excludedcolumns;
                                                                // TODO: DML
                                                                // DML composition
                                                                var dml;/* =
                                                                        "WITH json_array AS (\n"
                                                                 +              "SELECT * FROM\n"
                                                                 +              "json_populate_recordset(\n"
                                                                 +                      "NULL :: " + BUFFER_SCHEMA + ".customer,\n"
                                                                 +                      "'" + req.body + "'\n"
                                                                 +              ")\n"
                                                                 +      ")\n"

                                                                 +      "INSERT INTO " + BUFFER_SCHEMA + ".customer\n"
                                                                 +      "SELECT * FROM json_array\n"

                                                                 +      "ON CONFLICT ( email, shop ) DO UPDATE\n"

                                                                 +      "SET ( " + columnNames + " ) =\n"
                                                                 +      "( " + excludedColumnNames + " )";

                                                                console.log( "Executing DML: '" + dml + "'\n" );

                                                                // DML execution
                                                                client.query( dml, function( err, result ) {

                                                                        done();

                                                                        // DML error
                                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                                        // DML success
                                                                        else processResponse( res, 204, '' );

                                                                });

                                                        }
                                                });

                                        }

                                }
                        );

                }
        }
);*/

/**
[POST] Update sync status on buffer tables. If sandbox is specified, the sandbox schema will be used

Expected URIs:
 -      /buffer[/sandbox]/(ENTITY NAME)
                to update a list of records based on Heroku Ids
*/
app.post(
        ROOT_PATH + "(/sandbox)?/buffer/:entity",
        function( req, res ) {

                logRequest( req );

        // Authentication check
        if( checkAuth( req, res ) ) {

                // If this request is performed from sandbox, the sandbox data schema is used instead
                var targetEntity =
                        // Schema
                        ( req.originalUrl.startsWith( ROOT_PATH + '/sandbox/buffer' )? BUFFER_SCHEMA_SANDBOX : BUFFER_SCHEMA ) + '.'
                        // Table
                 +      req.params.entity;

                // Request processing
                pg.connect(
                        dbURL,
                        function( err, client, done ) {

                                // Database connection error
                                if( err ) processResponse( res, 500, "DB connection error: " + err );

                                // Delete data
                                else {

                                        try {

                                                var escapedBody = req.body.replace(/'/g,"''");
                                                // DML composition
                                                var dml =
                                                                        "SET LOCAL application_name  = 'Salesforce_Sync'; \n"
                                                                +       " UPDATE " + targetEntity + "\n"
                                                                +       " SET sync_status  = CASE WHEN sync_status = 'retry' THEN 'failed' WHEN sync_status = 'failed' THEN 'failed' ELSE 'retry' END\n"
                                                                +       " WHERE heroku_id IN (\n"
                                                                +               "SELECT (value ->> 'HerokuId__c')::numeric::int8 AS heroku_id\n"
                                                                +               "FROM JSON_ARRAY_ELEMENTS( '"+ escapedBody + "')\n"
                                                                +       ")";

                                                console.log( "Executing DML: '" + dml + "'\n" );

                                                // DML execution
                                                client.query( dml,function( err, result ) {

                                                        done();

                                                        // DML error
                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                        // DML success
                                                        else processResponse( res, 204, 'No Content' );

                                                });

                                        } catch( err ) {
                                                processResponse( res, 500, 'Internal error - ' + err );
                                        }

                                }

                        }
                );

        }
        }
);


// --------------- SALEFORCE MIRROR TABLES ---------------------------------------------------------------------------------

/**
[POST] Salesforce data creation and update on a mirror table. If sandbox is specified, the sandbox schema will be used

Expected URIs:
 -      /salesforce[/sandbox]/(ENTITY NAME)
                to create or update a list of records based on Salesforce Ids
*/
app.post(
        ROOT_PATH + "(/sandbox)?/salesforce/:entity",
        processSalesforceData
);

// -------------------------------------------------------------------------------------------------------------------------
// --------------- AUXILIARY METHODS ---------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------------------------------

// --------------- DATABASE OPERATIONS -------------------------------------------------------------------------------------

function composeColumnsQuery_buffer( tableName ) {

        return  "WITH\n"

                 +              "allColumns AS (\n"
                 +                      "SELECT a1.attname\n"
                 +                      "FROM   pg_attribute a1\n"

                 +                      "WHERE  a1.attrelid = '" + BUFFER_SCHEMA + "." + tableName + "'::regclass\n"
                 +                      "AND    a1.attnum > 0\n"
                 +                      "AND    NOT a1.attisdropped\n"
                 +              "),\n"

                 +              "keyColumns AS (\n"
                 +                      "SELECT a2.attname\n"
                 +                      "FROM   pg_index i\n"
                 +                      "JOIN   pg_attribute a2 ON a2.attrelid = i.indrelid\n"
                 +                                                               "AND a2.attnum = ANY(i.indkey)\n"
                 +                      "WHERE  i.indrelid = '" + BUFFER_SCHEMA + "." + tableName + "'::regclass\n"
                 +                      "AND    i.indisprimary\n"
                 +              ")\n"
                        // Key columns are not updated
                 +      "SELECT STRING_AGG( attname, ', ' ) AS columns, STRING_AGG( 'excluded.' || attname, ', ' ) AS excludedColumns FROM (\n"

                 +              "SELECT allColumns.attname FROM allColumns\n"
                 +              "LEFT JOIN keyColumns\n"
                 +              "ON allColumns.attname = keyColumns.attname\n"
                 +              "WHERE keyColumns.attname IS NULL\n"

                 +      ") AS resultSet";

}

function composeUpsertDML_mirror( data, table ) {

        return  "WITH data AS (\n"
                 +              "SELECT $1 :: JSON AS items\n"
                 +      ")\n"

                 +      "INSERT INTO " + table + " ( id, created_date, last_modified_date, properties, is_deleted )\n"
                 +              "SELECT value ->> 'Id' AS id, (value ->> 'CreatedDate') :: TIMESTAMPTZ AS created_date, (value ->> 'LastModifiedDate') :: TIMESTAMPTZ AS last_modified_date, value AS properties, (value ->> 'IsDeleted') :: BOOLEAN AS is_deleted\n"
                 +              "FROM JSON_ARRAY_ELEMENTS( ( SELECT items :: JSON FROM data ) )\n"

                 +      "ON CONFLICT ( id ) DO UPDATE\n"

                 +      "SET ( id, created_date, last_modified_date, properties, is_deleted ) =\n"
                 +              "( excluded.id, excluded.created_date, excluded.last_modified_date, " + table + ".properties || excluded.properties, excluded.is_deleted )\n"
                 // Update only newer records
                 +      "WHERE excluded.last_modified_date >= " + table + ".last_modified_date";

}

function composeDeleteDML_mirror( data, table ) {

        return  "UPDATE " + table + "\n"
                 +      "SET ( is_deleted ) = ( true )\n"
                 +      "WHERE id IN (\n"
                 +              "SELECT value#>>'{}' AS id\n"
                 +              "FROM JSON_ARRAY_ELEMENTS( '" + data + "' )\n"
                 +      ")";

}


// --------------- PROCESSING ----------------------------------------------------------------------------------------------

function processSalesforceData( req, res ) {

        logRequest( req );

        // Authentication check
        if( checkAuth( req, res ) ) {

                // If this request is performed from sandbox, the sandbox data schema is used instead
                var targetEntity =
                        // Schema
                        ( req.originalUrl.startsWith( ROOT_PATH + '/sandbox/salesforce' )? SALESFORCE_SCHEMA_SANDBOX : SALESFORCE_SCHEMA ) + '.'
                        // Table
                 +      req.params.entity;

                // Request processing
                pg.connect(
                        dbURL,
                        function( err, client, done ) {

                                // Database connection error
                                if( err ) processResponse( res, 500, "DB connection error: " + err );

                                // Delete data
                                else {

                                        try {

                                                // DML composition
                                                var dml;
                                                if( req.method == 'POST' ) {
                                                        dml = composeUpsertDML_mirror( req.body, targetEntity );
                                                } else if( req.method == 'DELETE' ) {
                                                        dml = composeDeleteDML_mirror( req.body, targetEntity );
                                                } else {
                                                        throw new Error( 'Unsopported method: ' + req.method );
                                                }

                                                console.log( "Executing DML: '" + dml + "'\n" );

                                                // DML execution
                                                client.query( dml,[req.body],function( err, result ) {

                                                        done();

                                                        // DML error
                                                        if( err ) processResponse( res, 500, "DB access error - " + err );

                                                        // DML success
                                                        else processResponse( res, 204, 'No Content' );

                                                });

                                        } catch( err ) {
                                                processResponse( res, 500, 'Internal error - ' + err );
                                        }

                                }

                        }
                );

        }

}

function processResponse( res, statusCode, bodyMessage ) {

        res.status( statusCode );
        // JSON response format
        if( bodyMessage instanceof Array || typeof bodyMessage === "object" ) {
                res.json( bodyMessage || null );
                logResult( statusCode, JSON.stringify( bodyMessage ) );

        // Text response format
        } else {
                res.set( "Content-Type", "text/html;charset=UTF-8" );
                res.send( bodyMessage || '' );
                logResult( statusCode, bodyMessage );
        }

}

// --------------- SECURITY ------------------------------------------------------------------------------------------------

function checkAuth( req, res ) {

        var authOk = true;

        // Request is not authorized if not performed on a secure connection, and it is not a test on a local machine
        if( req.headers[ "x-forwarded-proto" ] && req.headers[ "x-forwarded-proto" ] != "https" ) {
                authOk = false;
                res.set( "Connection", "Upgrade" );
                res.set( "Upgrade", "TLS/1.0, HTTP/1.1" );
                processResponse( res, 426, "Upgrade Required" );
        }

        // Request is not authorized if, being a token is established on this side, it does not match the one in the request
        // Authorization header
        if( authOk && ( token && req.get( "Authorization" ) != ( "Bearer " + token ) ) ) {
                authOk = false;
                res.set( "WWW-Authenticate", "Basic realm=Authorization Required" );
                processResponse( res, 401, "Invalid token" );
        }

        return authOk;

}


// --------------- LOGGING -------------------------------------------------------------------------------------------------

function logRequest( req ) {
        console.log(
                "\n\n------------------------------------------------------------------------------------------------------------------------\n\n"
         +      " ---- " + req.method + " " + req.url + "\n\n"
         +      " -- URL PARAMS\n\n"
         + JSON.stringify( req.query ) + "\n\n"
         +      " ---- PROCESSING START\n"
       );
}

function logResult( statusCode, statusMessage ) {
        console.log(
                " ---- PROCESSING FINISH\n\n"
         +      " -- RESULT\n\n"
         +      statusCode + " - " + statusMessage + "\n"
         +      "\n------------------------------------------------------------------------------------------------------------------------\n\n"
        );
}
