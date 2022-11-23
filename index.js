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
    

    // let query = `SELECT *, statement_timestamp() AS timestamp FROM "Million";`;
    let query = `SELECT *, current_timestamp AS timestamp FROM "Million";`;


    const cursor = client.query(new Cursor(query));
    
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
  } catch (error) {
    console.error(error);
  }
})();
