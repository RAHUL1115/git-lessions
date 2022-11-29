require("dotenv").config();
const fs = require("fs");
const pgClient = require("pg").Client;
const Cursor = require("pg-cursor");
const csv = require("fast-csv");

const config = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

const client = new pgClient(config);

(async () => {
  try {
    await client.connect();
    let writeStream = fs.createWriteStream("./output.csv");
    let batchSize = 100
    
    let createQuery = `CREATE TABLE IF NOT EXISTS "Million" (name character varying(255) COLLATE pg_catalog."default", joindate date );`

    await client.query(createQuery);

    let truncateQuery = `TRUNCATE "Million";`

    await client.query(createQuery);

    let insertQuery = `INSERT INTO "Million" (name, joindate) SELECT substr(md5(random()::text), 1, 10), DATE '2018-01-01' + (random() * 700)::integer FROM generate_series(1, 100000);`

    await client.query(insertQuery);

    // let query = `SELECT *, statement_timestamp() AS timestamp FROM "Million";`;
    let selectQuery = `SELECT *, current_timestamp AS timestamp FROM "Million";`;


    const cursor = client.query(new Cursor(selectQuery));
    
    // csvStream
    const csvStream = csv.format({ headers: true }).transform((row) => ({
      name: row.name,
      joindate: row.joindate.toJSON(),
      timestamp : row.timestamp.toJSON(),
      currentdate: (new Date()).toJSON(),
    }));

    // create fs writable stream
    csvStream.pipe(writeStream);
    
    // streaming data through csv stream
    let rows = [null]
    while(rows.length){
      rows = await cursor.read(batchSize);
      rows.forEach((row) => {
        csvStream.write(row);
      });
    }

    // close all connection
    csvStream.end();
    cursor.close();
    client.end();
    console.log("done");
  } catch (error) {
    console.error(error);
  }
})();
