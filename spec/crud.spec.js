describe("Test CRUD Functions", () => {
	const MongoDatabase = require('../lib/index.js').MongoDatabase;
	const options = {
		DB_HOSTS: 'mongo:27017',
	};
	const db_params = { database: 'jasmine_testing', collection: 'test', debug: true};
	// console.log('Using Options for DB',options,"\n",db_params,"\n");
	beforeAll(() => {
		db = new MongoDatabase(options);
	});

	afterAll(async (done) => {
		await db.deleteMany(Object.assign({},db_params),{});
		done();
	});

	it("Should run set db and collection in transaction", async () => {
		transaction_result = await db.runTransaction({
			debug:true,
			database:db_params.database,
			collection: db_params.collection
		},async (transaction_params) => {
			expect(transaction_params.database).toBe(db_params.database);
			expect(transaction_params.collection).toBe(db_params.collection);
		});
	});
	it("Should not set the database by default", async () => {
		transaction_result = await db.runTransaction({
			debug:true,
			collection: db_params.collection
		},async (transaction_params) => {
			expect(transaction_params.hasOwnProperty('database')).toBe(false);
			expect(transaction_params.collection).toBe(db_params.collection);
		});
	});

	it("Should run a transaction and abort", async () => {
		let docs = await db.find(Object.assign({},db_params));
		// console.log("Pre Transaction Docs",docs);

		let transaction_result
		try{
			transaction_result = await db.runTransaction({
				debug:true,
				database:db_params.database,
				collection: db_params.collection
			},async (transaction_params) => {
				await db.insertOne(transaction_params,{
					name: 'Jasmine Transaction Abort',
					email: 'jasmine@gmail.com',
					salary: ~~(Math.random() * 1000000) / 100,
					created: new Date('2017-11-27')
				});
				await db.insertOne(transaction_params,{
					name: 'Jasmine Transaction Abort',
					email: 'jasmine@gmail.com',
					salary: ~~(Math.random() * 1000000) / 100,
					created: new Date('2017-11-27')
				});
				await db.insertOne(transaction_params,{
					name: 'Jasmine Transaction Abort',
					email: 'jasmine@gmail.com',
					salary: ~~(Math.random() * 1000000) / 100,
					created: new Date('2017-11-27')
				});
				let transaction_docs = await db.find(transaction_params);
				// console.log("Pre Abort Transaction Docs",transaction_docs);
				expect(transaction_docs.length).toBe(3);
				throw new Error("Transaction Should Abort");
			});
		}catch(e){
			// console.log("Transaction Aborted",e);
		}

		let post_docs = await db.find(Object.assign({},db_params));
		// console.log("Post Transaction Docs",post_docs);
		expect(post_docs.length).toBe(0);
	});

	it("should insertOne document", async () => {
		let result = await db.insertOne(Object.assign({},db_params),{
			name: 'Jasmine',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		expect(result._id).toBeTruthy();
	});
	//
	it("should insertMany documents", async () => {
		let result = await db.insertMany(Object.assign({},db_params),[
			{
				name: 'Jasmine Many',
				email: 'jasmine.many1@gmail.com',
				salary: ~~(Math.random() * 1000000) / 100,
				created: new Date('2017-11-29')
			},
			{
				name: 'Jasmine Many',
				email: 'jasmine.many2@gmail.com',
				salary: ~~(Math.random() * 1000000) / 100,
				created: new Date('2017-11-29')
			}
		]);
		expect(result._ids).toBeTruthy();
	});


	it("should updateOne document", async () => {
		let result = await db.updateOne(Object.assign({},db_params),{name:'Jasmine'},{
			name: 'Jasmine Updated',
		});
		expect(result.modified).toBe(1);
	});
	it("should findOne document, and only return the name field", async () => {
		let result = await db.findOne(Object.assign({fields:["name"]},db_params),{name:'Jasmine Updated'});
		expect(result.name).toBe('Jasmine Updated');
		expect(result.email).toBe(undefined);
	});

	it("should updateMany documents", async () => {
		let result = await db.updateMany(Object.assign({},db_params),{name:'Jasmine Many'},{
			name: 'Jasmine Many Updated',
		});
		expect(result.modified > 1).toBeTruthy();
	});


	it("should find documents", async () => {
		let result = await db.find(Object.assign({},db_params),{name: 'Jasmine Many Updated'});
		expect(result.length > 1).toBeTruthy();
		expect(result[0].name).toBe('Jasmine Many Updated');
	});

	it("should find documents and sort", async () => {
		let result = await db.find(Object.assign({sort:{id:"asc",name:"desc",email:-1}},db_params),{name: 'Jasmine Many Updated'});
		expect(result.length > 1).toBeTruthy();
		expect(result[0].name).toBe('Jasmine Many Updated');
	});

	it("should find 3 documents", async () => {
		let result = await db.find(Object.assign({},db_params),{});
		expect(result.length).toBe(3);
	});

	it("should aggregate 3 documents", async () => {
		let result = await db.aggregate(Object.assign({},db_params),[{$match:{}}]);
		expect(result.length).toBe(3);
	});

	it("should get a cursor and find 3 documents", async () => {
		let cursor = await db.getCursor(Object.assign({timeout: false, fields:["name"]},db_params),{});
		let count = await cursor.count();
		while (await cursor.hasNext()) {
			let doc = await cursor.next();
			expect(doc.name.length).toBeGreaterThan(0);
			expect(doc.email).toBe(undefined);
		}
		expect(count).toBe(3);
		cursor.close();
	});

	it("should deleteOne document", async () => {
		let result = await db.deleteOne(Object.assign({},db_params),{name:'Jasmine Updated'});
		expect(result.deleted === 1).toBeTruthy();
	});

	it("should deleteMany documents", async () => {
		let result = await db.deleteMany(Object.assign({},db_params),{name: 'Jasmine Many Updated'});
		expect(result.deleted > 1).toBeTruthy();
	});

	it("should replaceOne document", async () => {
		let doc = {
			name: 'Jasmine Replace One',
			email: "jasmine@replaceone.ca",
			number : 2
		};
		let doc2 = {
			name: 'Jasmine Replaced One',
			email: "jasmine@replacedone.ca",
		};
		let insert_result = await db.insertOne(Object.assign({},db_params),doc);
		let replace_result = await db.replaceOne(Object.assign({},db_params),{_id:insert_result._id},doc2);
		let result = await db.findOne(Object.assign({},db_params),{_id:insert_result._id});
		expect(result.name).toBe(doc2.name);
		expect(result.number).toBe(undefined);
	});

	it("should findOneAndUpdate document", async () => {
		let doc = {
			name: 'Jasmine findOneAndUpdate',
			email: "jasmine@finandupdate.ca",
		};
		let update = {
			name: 'Jasmine findOneAndUpdate Updated',
			email: "jasmine@finandupdate.ca",
		};
		let insert_result = await db.insertOne(Object.assign({},db_params),doc);
		let update_result = await db.findOneAndUpdate(Object.assign({},db_params),{_id:insert_result._id},update);
		let updated_document = await db.findOne(Object.assign({},db_params),{_id: insert_result._id});
		expect(update_result.value.name).toBe(doc.name);
		expect(updated_document.name).toBe(update.name);
	});

	it("should findOneAndReplace document", async () => {
		let doc = {
			name: 'Jasmine findOneAndReplace',
			email: "jasmine@finandupdate.ca",
		};
		let doc2 = {
			name: 'Jasmine findOneAndReplace Replaced',
			email: "jasmine@findonereplacedone.ca",
		};
		let insert_result = await db.insertOne(Object.assign({},db_params),doc);
		let replace_result = await db.findOneAndReplace(Object.assign({},db_params),{_id:insert_result._id},doc2);
		let replaced_document = await db.findOne(Object.assign({},db_params),{_id: insert_result._id});
		expect(replace_result.value.name).toBe(doc.name);
		expect(replaced_document.name).toBe(doc2.name);
	});

	it("should findOneAndDelete document", async () => {
		let doc = {
			name: 'Jasmine findOneAndDelete',
			email: "jasmine@finanddelete.ca",
		};
		let insert_result = await db.insertOne(Object.assign({},db_params),doc);
		let delete_result = await db.findOneAndDelete(Object.assign({},db_params),{_id:insert_result._id});
		let deleted_doc = await db.findOne(Object.assign({},db_params),{_id:insert_result._id});
		expect(delete_result.value.name).toBe(doc.name);
		expect(deleted_doc).toBe(null);
	});



});
