# vEden MongoDB

### Purpose

This package is a wrapper for the mongodb nodejs client

### Usage

```
const MongoDatabase = require('veden-mongodb').MongoDatabase;
const db = new MongoDatabase({
	DB_HOSTS : "localhost",
	DB_REPLICA_SET : "test",
	DB_USER : "admin",
	DB_PASS : "secret123"
});
// This will connect to the url: `mongodb://admin:secret123@localhost/?replicaSet=test`


let db_params = {
	database: "users",
	collection: "user",
	limit: 10,
	fields: ["username","email"],
	exclude_fields: ["_id"],
	sort:{
		field: "_id",
		order: "asc"
	}
};
db.find(db_params,{email:/.*@gmail.com/g}).then(result => {
	/* Use data */
});
```

### Default Parameters
All methods take DB Parameters as the first argument, which can be;

**database**: Database to connect to  
**collection**: Collection to use  
**limit**: number of docs to return  
**fields**: fields to include in result  
**exclude_fields**: fields to exclude from result  
**sort.field**: Sort by given field ascending by default  
**sort.order**: "asc" or "desc"

The Second argument is the query, uses mongodb query syntax. Default is empty query (return all documents).

Write operations (insert, update, etc) take the document in the third argument

All methods return promises which resolve to their return type

### Available Methods

#### getCursor()
Runs a find() with the given query and returns the resulting mongodb cursor object

#### count()

Returns an integer with the matching document count

#### find()

Returns an array with all matching documents

#### findOne()

Returns only the matching document

#### insertOne()

Returns
- \_id: ObjectId of inserted document
- result: Raw Response from database (varies by mongo version)

#### insertMany()
Returns
- \_ids: Array of ObjectIds of inserted documents
- result: Raw Response from database (varies by mongo version)

#### replaceOne()

Returns
- matched: Number of matched documents
- modified: Number of modified documents
- upserted: Number of upserted documents
- upserted_id: Id of created document if upserted
- result: Raw Response from database (varies by mongo version)

#### upsertOne()

Returns
- matched: Number of matched documents
- modified: Number of modified documents
- upserted: Number of upserted documents
- upserted_id: Id of created document if upserted
- result: Raw Response from database (varies by mongo version)

#### updateOne()

Returns
- matched: Number of matched documents
- modified: Number of modified documents
- result: Raw Response from database (varies by mongo version)


#### updateMany()

Returns
- matched: Number of matched documents
- modified: Number of modified documents
- result: Raw Response from database (varies by mongo version)

#### deleteOne()
Returns
- deleted: count of deleted documents

#### deleteMany()
Returns
- deleted: count of deleted documents

#### aggregate()

Allows you to use mongodb's aggregation pipeline

```
db.aggregate(db_params,[ ... aggregations ]).then(result => { ... })

```

#### startTransaction(opts,callback) => Promise

Starts a new ClientSession, begins a transaction and provides the required db_params in a callback. The transaction is commited when the callback resolves, and aborted if it throws an error.

opts
- `retries` default: 3 - Number of Retries before throwing error
- `delay` default: 1000 - Delay in ms before retrying
- `database` default: none - Default DB to provide in params
- `collection` default: none - Default collection to provide in params
- `retry_error_codes` default: [251,112] - Error codes to retry on

callback
- Recieves params object which can be passed to other db methods
- Must return a Promise (async / await does this automatically)
- Result will be resolved by the runTransaction function when the transaction finishes
- `params`:
  - `session` - always present, ClientSession with running transaction, pass into db_params for operation to be within the session
  - `database` - present if provided in opts
  - `collection` - present if provided in opts

```
db.runTransaction({database: 'db1'},(params) => {
	return Promise.all([
		db.updateOne(
			{database: params.database, collection: 'collection1', session: params.session},
			{},
			{closed: true}
		),
		db.updateMany(
			{database: params.database, collection: 'collection2', session: params.session, overwrite: true},
			{name:"update doc"},
			{$push:{_ids: "update"}}
		)
	]);
}).then(([result1,result2]) => {
	// Transaction successful
}).catch(e => {
	// Transaction Failed
})
```
