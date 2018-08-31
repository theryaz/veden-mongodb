describe("Test CRUD Functions", () => {
	const MongoDatabase = require('../lib/index.js').MongoDatabase;
	const options = {
		DB_HOSTS: process.env.DB_HOSTS || 'localhost',
		DB_USER: process.env.DB_USER || 'root',
		DB_PASS: process.env.DB_PASS || 'changeme',
		DB_REPLICA_SET : process.env.DB_REPLICA_SET
	};
	const db_params = { database: 'jasmine_testing', collection: 'test', debug: true};
	console.log('Using Options for DB',options,"\n",db_params,"\n");
	beforeAll(() => {
		db = new MongoDatabase(options);
	});

	afterAll(async (done) => {
		await db.deleteMany(Object.assign({},db_params),{});
		done();
	});

	it("Should run a transaction and abort", async () => {
		let docs = await db.find(Object.assign({},db_params));
		console.log("Pre Transaction Docs",docs);

		let client_session = await db.getClientSession(db_params.database);
		await client_session.startTransaction();
		console.log("client_session.inTransaction()",client_session.inTransaction());
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Abort',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Abort',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Abort',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		let transaction_docs = await db.find(Object.assign({session:client_session},db_params));
		console.log("Pre Abort Transaction Docs",transaction_docs);
		expect(transaction_docs.length).toBe(3);
		expect(client_session.inTransaction()).toBe(true);

		await client_session.abortTransaction();
		await client_session.endSession();

		let post_docs = await db.find(Object.assign({},db_params));
		console.log("Post Transaction Docs",post_docs);
		expect(post_docs.length).toBe(0);
	});

	it("Should run a transaction and commit", async () => {
		let docs = await db.find(Object.assign({},db_params));

		let client_session = await db.getClientSession(db_params.database);
		await client_session.startTransaction();
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Commit',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Commit',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		await db.insertOne(Object.assign({session:client_session},db_params),{
			name: 'Jasmine Transaction Commit',
			email: 'jasmine@gmail.com',
			salary: ~~(Math.random() * 1000000) / 100,
			created: new Date('2017-11-27')
		});
		let transaction_docs = await db.find(Object.assign({session:client_session},db_params));
		expect(transaction_docs.length).toBe(3);
		await client_session.commitTransaction();
		await client_session.endSession();

		let post_docs = await db.find(Object.assign({},db_params));
		expect(post_docs.length).toBe(3);
		await db.deleteMany(Object.assign({},db_params),{_id:{$in:post_docs.map(x => x._id)}});
	});

	it("Should fail to start 2 transactions", async () => {
		let docs = await db.find(Object.assign({},db_params));
		console.log("Pre Transaction Docs",docs);

		let client_session = await db.getClientSession(db_params.database);
		await client_session.startTransaction();
		// MongoError: Transaction already in progress
		expect(client_session.startTransaction).toThrow();

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


});
