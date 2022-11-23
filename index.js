require("dotenv").config();
const fs = require("fs");
const pgClient = require("pg").Client;
const Cursor = require("pg-cursor");
const csv = require("fast-csv");
const QueryStream = require("pg-query-stream");

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
    let batchSize = 100000

    let query = `SELECT *, EXTRACT(MILLISECONDS FROM statement_timestamp()) AS current_ms FROM "Million" LIMIT 100000;`;
    // let query = `SELECT *, EXTRACT(MILLISECONDS FROM current_timestamp) AS current_ms FROM "Million" LIMIT 100000;`;
    // let query = `SELECT *, statement_timestamp()::Varchar AS current_ms FROM "Million" LIMIT 100000;`;
    // let query = `SELECT *, current_timestamp()::Varchar AS current_ms FROM "Million" LIMIT 100000;`;
    const qs = new QueryStream(query);
    console.log(qs);


    const cursor = client.query(new Cursor(query));
    
    // csvStream
    const csvStream = csv.format({ headers: true }).transform((row) => ({
      current_ms : row.current_ms,
      name: row.name,
      joindate: row.joindate,
      currentdate: new Date(),
    }));

    // create fs writable stream
    var writeStream = fs.createWriteStream("./output2.csv");
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
  } catch (error) {
    console.error(error);
  }
})();
