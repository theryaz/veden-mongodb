const MongoClient = require('mongodb').MongoClient;

class MongoDatabase{
	constructor(opts={}){
		this.connections = {}; // Holds open Database connections
		this.options = {
			DB_HOSTS : opts.DB_HOSTS,
			DB_REPLICA_SET : opts.DB_REPLICA_SET,
			DB_USER : opts.DB_USER,
			DB_PASS : opts.DB_PASS
		};
		if(this.options.DB_HOSTS === undefined) throw new Error("DB_HOSTS not provided.");
		if(this.options.DB_REPLICA_SET === undefined) throw new Error("DB_REPLICA_SET not provided.");
		if(this.options.DB_USER === undefined) throw new Error("DB_USER not provided.");
		if(this.options.DB_PASS === undefined) throw new Error("DB_PASS not provided.");

		this.mongo_client = async (database) => {
			let connection = this.connections[database];
			if(connection === undefined){
				let url = `mongodb://${this.options.DB_USER}:${this.options.DB_PASS}@${this.options.DB_HOSTS}/${database}?replicaSet=${this.options.DB_REPLICA_SET}&authMechanism=SCRAM-SHA-1&authSource=admin`;
				connection = await MongoClient.connect(url,{"reconnectTries":100,reconnectInterval:2000});
				this.connections[database] = connection;
			}
			return this.connections[database];
		};
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
		// console.log("Using projection",projection);
		return projection;
	}

	async getCursor(db_params, query={}){
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let sort = {_id:1};
		if(db_params.sort){
			if(db_params.sort.field && db_params.sort.order === "asc")
				sort = {[db_params.sort.field]:1}
			if(db_params.sort.field && db_params.sort.order === "desc")
				sort = {[db_params.sort.field]:-1}
			if(db_params.sort.field && db_params.sort.order === undefined)
				sort = {[db_params.sort.field]:1}
		}
		let limit = db_params.limit || 10;
		let db = await this.mongo_client(db_params.database);
		return db.collection(db_params.collection).find(query, {fields: projection});
	}
	async count(db_params, query={}){
		let db = await this.mongo_client(db_params.database);
		return db.collection(db_params.collection).find(query).count();
	}
	async find(db_params, query={}){
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let sort = {_id:1};
		if(db_params.sort){
			if(db_params.sort.field && db_params.sort.order === "asc")
				sort = {[db_params.sort.field]:1}
			if(db_params.sort.field && db_params.sort.order === "desc")
				sort = {[db_params.sort.field]:-1}
			if(db_params.sort.field && db_params.sort.order === undefined)
				sort = {[db_params.sort.field]:1}
		}
		let limit = db_params.limit || 10;
		if (db_params.from >= 1){
			let skip = db_params.from - 1;
			let db = await this.mongo_client(db_params.database);
			return db.collection(db_params.collection).find(query,projection).sort(sort).limit(limit).skip(skip).toArray();
		}else{
			let db = await this.mongo_client(db_params.database);
			return db.collection(db_params.collection).find(query,projection).sort(sort).limit(limit).toArray();
		}

	}
	async findOne(db_params, query={}){
		// console.log("db_params",db_params);
		let projection = this.getProjectionFromFields(db_params);
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongo_client(db_params.database);
		return db.collection(db_params.collection).findOne(query,projection);
	}
	async insertOne(db_params, doc){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(doc === undefined)
			throw new Error("No document provided for insert operation")

		let db = await this.mongo_client(db_params.database);
		let insert_result = await db.collection(db_params.collection).insertOne(doc);
		return {
			_id: insert_result.insertedId,
			result: insert_result.result
		};
	}
	async insertMany(db_params, docs){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(!Array.isArray(docs)) throw new Error("Provided data is not an array");
		let db = await this.mongo_client(db_params.database);
		let insert_result = await db.collection(db_params.collection).insertMany(docs);
		return {
			_ids: insert_result.insertedIds,
			result: insert_result.result
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

		let db = await this.mongo_client(db_params.database);
		let result = await db.collection(db_params.collection).update(query,body,{upsert: true});
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

		let db = await this.mongo_client(db_params.database);
		let result = await db.collection(db_params.collection).updateOne(query,body);
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

		let db = await this.mongo_client(db_params.database);
		let result = await db.collection(db_params.collection).updateMany(query,body);
		return {
			matched: result.matchedCount,
			modified: result.modifiedCount,
			result: result.result
		};
	}
	async deleteOne(db_params, query={}){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongo_client(db_params.database);
		let result = await db.collection(db_params.collection).deleteOne(query);
		return {
			deleted: result.deletedCount
		};
	}
	async deleteMany(db_params, query={}){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongo_client(db_params.database);
		let result = await db.collection(db_params.collection).deleteMany(query);
		return {
			deleted: result.deletedCount
		};
	}
	async aggregate(db_params, pipeline){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		let db = await this.mongo_client(db_params.database);
		return await db.collection(db_params.collection).aggregate(pipeline).toArray();
	}
	async distinct(db_params, field){
		if(db_params.database === undefined) throw new Error("Database not provided");
		if(db_params.collection === undefined) throw new Error("Collection not provided");
		if(field === undefined) throw new Error("Field is not provided");
		let db = await this.mongo_client(db_params.database);
		return await db.collection(db_params.collection).distinct(field);
	}
}

module.exports = MongoDatabase;
