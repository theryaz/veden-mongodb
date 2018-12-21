const MongoClient = require('mongodb').MongoClient;

class MongoDatabase{
	constructor(opts={}){
		this.clients = {};
		this.connections = {}; // Holds open Database connections
		this.sessions = [];
		this.options = {
			DB_HOSTS : opts.DB_HOSTS,
			DB_REPLICA_SET : opts.DB_REPLICA_SET,
			DB_USER : opts.DB_USER,
			DB_PASS : opts.DB_PASS,
			DB_SSL : opts.DB_SSL,
			DB_RETRY_WRITES : opts.DB_RETRY_WRITES,
			DEBUG : opts.DEBUG,
		};
		if(this.options.DB_HOSTS === undefined) throw new Error("DB_HOSTS not provided.");
	}


	async mongoClient (database){
		let connection = this.connections[database];
		let client = await this.getClient(database);
		if(connection === undefined){
			this.connections[database] = await client.db(database);
		}
		return this.connections[database];
	}

	async getClient(database='default'){
		if(!this.clients[database] || !this.clients[database].isConnected()){
			let url = this.getConnectionUrl();
			this.clients[database] = await MongoClient.connect(url,{"useNewUrlParser": true,"reconnectTries":100,reconnectInterval:2000});
		}
		return this.clients[database];
	}

	getConnectionUrl(){
		let AUTH_STRING;
		if(this.options.DB_USER && this.options.DB_PASS) AUTH_STRING = `${this.options.DB_USER}:${this.options.DB_PASS}@`;
		let url = `mongodb://${AUTH_STRING?AUTH_STRING:''}${this.options.DB_HOSTS}?`;
		if(this.options.DB_REPLICA_SET) url += `&replicaSet=${this.options.DB_REPLICA_SET}`;
		if(AUTH_STRING) url += '&authMechanism=SCRAM-SHA-1&authSource=admin';
		if(this.options.DB_SSL) url += `&ssl=true`;
		if(this.options.DB_RETRY_WRITES) url += `&retryWrites=true`;
		if(this.options.DEBUG) console.info("veden-mongodb: USING URL",url);
		return url;
	}

	async getClientSession(){
		let client = await this.getClient();
		let session = client.startSession();
		return session;
	}

	/**
	@param options.retries default: 3
	 - Number of times to retry the transaction

	@param options.delay default: 1000
	 - Delay in miliseconds to wait before retrying

	@param options.database default: none
	 - Database to give in default_params

	@param options.collection default: none
	 - collection to give in default_params

	@param options.retry_error_codes default: [251,112]
	 - Error codes which trigger a retry attempt.
	 - https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
			251 === "Transaction not an in progress transaction"
			112 === WriteConflict
	*/
	async runTransaction(options,transactionFn){
		let delay = options.delay || 1000;
		let retries = options.retries || 3;
		let default_params = {}
		if(options.database) default_params.database = options.database;
		if(options.collection) default_params.collection = options.collection;

		let retry_error_codes = [251,112];
		if(Array.isArray(options.retry_error_codes)){
			retry_error_codes = options.retry_error_codes;
		}
		let errors = [];
		let client = await this.getClient();
		let session = await client.startSession();
		async function attemptTransaction(client,session,attempt=0){
			if(attempt >= retries) throw new Error(`Transaction failed all ${attempt} tries`,"Errors: " + JSON.stringify(errors));
			// console.log("Transaction Attempt #",attempt);
			default_params.session = session;
			try {
				await session.startTransaction();
				let result = await transactionFn(default_params);
				await session.commitTransaction();
				await session.endSession();
				return result;
			} catch (e) {
				// console.log("Aborting attempt");
				await session.abortTransaction();
				if(retry_error_codes.indexOf(e.code) !== -1){
					errors.push(e);
					console.log(`${attempt} Retrying transaction after error code`,e.code);
					await wait(delay);
					return attemptTransaction(client,session,++attempt);
				}else{
					// console.log("Ending session");
					await session.endSession();
					throw e;
				}
			}
		}
		return attemptTransaction(client,session);
	}

	getProjectionFromFields(params){
		let projection = {};
		if(Array.isArray(params.fields) && params.fields.length > 0){ // If fields is present, ignore exclude_fields
			for(let f of params.fields)
				projection[f] = 1;
			// _id is an exception, you can exclude _id while including other fields
			if(Array.isArray(params.exclude_fields) && params.exclude_fields.indexOf("_id") !== -1)
				projection['_id'] = 0;
		}else if(Array.isArray(params.exclude_fields)) {
			for(let f of params.exclude_fields)
				projection[f] = 0;
		}
		return projection;
	}


	async getCursor(db_params, query={}){
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let sort = {};
		if(Array.isArray(db_params.sort)){
			for(let s of db_params.sort){
				let [field,order] = s.split(':');
				switch (order) {
					case 'asc':
						sort[field] = 1;
						break;
					default:
						sort[field] = -1;
				}
			}
		}else{
			sort = {_id:1};
		}
		let limit = db_params.limit || 10;
		let timeout = db_params.timeout || 600000;
		let db = await this.mongoClient(db_params.database);
		let options = db_params.options || {};
		options.projection = projection;
		return db.collection(db_params.collection).find(query, options).maxTimeMS(timeout);
	}
	async count(db_params, query={}){
		let db = await this.mongoClient(db_params.database);
		return db.collection(db_params.collection).find(query).count();
	}
	async find(db_params, query={}){
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let sort = {};
		if(Array.isArray(db_params.sort)){
			for(let s of db_params.sort){
				let [field,order] = s.split(':');
				switch (order) {
					case 'asc':
						sort[field] = 1;
						break;
					default:
						sort[field] = -1;
				}
			}
		}else{
			sort = {_id:1};
		}
		let limit = db_params.limit || 10;
		if (db_params.from >= 1){
			let skip = db_params.from - 1;
			let db = await this.mongoClient(db_params.database);
			return db.collection(db_params.collection).find(query,{projection, session: db_params.session}).sort(sort).limit(limit).skip(skip).toArray();
		}else{
			let db = await this.mongoClient(db_params.database);
			return db.collection(db_params.collection).find(query,{projection, session: db_params.session}).sort(sort).limit(limit).toArray();
		}

	}
	async findOne(db_params, query={}){
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongoClient(db_params.database);
		return db.collection(db_params.collection).findOne(query,{projection, session: db_params.session});
	}
	async insertOne(db_params, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(doc === undefined)
			throw new Error("No document provided for insert operation")

		let db = await this.mongoClient(db_params.database);
		let insert_result = await db.collection(db_params.collection).insertOne(doc,{session: db_params.session});
		return {
			_id: insert_result.insertedId,
			result: insert_result.result
		};
	}
	async insertMany(db_params, docs){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(!Array.isArray(docs)) throw new Error("Provided data is not an array");
		let db = await this.mongoClient(db_params.database);
		let insert_result = await db.collection(db_params.collection).insertMany(docs,{session: db_params.session});
		return {
			_ids: insert_result.insertedIds,
			result: insert_result.result
		};
	}
	async replaceOne(db_params, query={}, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let body = doc;

		if(body === undefined)
			throw new Error("No document provided for replace operation")

			let upsert = db_params.upsert || false;

		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).replaceOne(query,body,{upsert: upsert, session: db_params.session});
		return {
			matched: result.matchedCount,
			modified: result.modifiedCount,
			upserted: result.upsertedCount,
			upserted_id: result.upsertedId,
			result: result.result
		};
	}
	async upsertOne(db_params, query={}, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let body = doc;

		if(body === undefined)
			throw new Error("No document provided for upsert operation")

		if(db_params.overwrite !== true)
			body = {$set:doc};

		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).update(query,body,{upsert: true, session: db_params.session});
		return {
			matched: result.matchedCount,
			modified: result.modifiedCount,
			upserted: result.upsertedCount,
			upserted_id: result.upsertedId,
			result: result.result
		};
	}
	async updateOne(db_params, query={}, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let body = doc;
		if(body === undefined)
			throw new Error("No document provided for update operation")
		if(db_params.overwrite !== true)
			body = {$set:doc};

		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).updateOne(query,body,{session: db_params.session});
		return {
			matched: result.matchedCount,
			modified: result.modifiedCount,
			result: result.result
		};
	}
	async updateMany(db_params, query={}, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let body = doc;
		if(db_params.overwrite !== true)
			body = {$set:doc};

		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).updateMany(query,body,{session: db_params.session});
		return {
			matched: result.matchedCount,
			modified: result.modifiedCount,
			result: result.result
		};
	}
	async deleteOne(db_params, query={}){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).deleteOne(query,{session: db_params.session});
		return {
			deleted: result.deletedCount
		};
	}
	async deleteMany(db_params, query={}){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongoClient(db_params.database);
		let result = await db.collection(db_params.collection).deleteMany(query,{session: db_params.session});
		return {
			deleted: result.deletedCount
		};
	}
	async aggregate(db_params, pipeline){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongoClient(db_params.database);
		return await db.collection(db_params.collection).aggregate(pipeline,{session: db_params.session}).toArray();
	}
	async distinct(db_params, field, query={}){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(field === undefined) throw new Error("Field is not provided");
		let db = await this.mongoClient(db_params.database);
		return await db.collection(db_params.collection).distinct(field,query,{session: db_params.session});
	}
	async findAndModify(db_params, query={}, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let sort = {};
		if(Array.isArray(db_params.sort)){
			for(let s of db_params.sort){
				let [field,order] = s.split(':');
				switch (order) {
					case 'asc':
						sort[field] = 1;
						break;
					default:
						sort[field] = -1;
				}
			}
		}else{
			sort = {_id:1};
		}
		let db = await this.mongoClient(db_params.database);
		return db.collection(db_params.collection).findAndModify(query,sort,doc,{session: db_params.session});
	}
}

module.exports = MongoDatabase;


function wait(ms){
	return new Promise(function(resolve, reject) {
		setTimeout(resolve,ms);
	});
}
