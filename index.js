require("dotenv").config();
const fs = require("fs");
const pgClient = require("pg").Client;
const QueryStream = require("pg-query-stream");
const csv = require("fast-csv");

const config = {
  host: process.env.DB_HOST || "localhost",
  port: process.env.DB_PORT || "5432",
  user: process.env.DB_USER || "pguser",
  password: process.env.DB_PASSWORD || "root",
  database: process.env.DB_NAME || "test",
};

const client = new pgClient(config);

function withTransaction(client,filename) {
  return new Promise(async (resolve, reject) => {
    await client.query("BEGIN");
    try {
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv
        .format({ headers: true })
        .transform((row) => ({
          name: row.name,
          joindate: row.joindate.toJSON(),
          current_ts: row.current_ts.toJSON(),
          statement_ts: row.statement_ts.toJSON(),
        }))
        .on("end", () => {
          writeStream.end();
          resolve();
        });

      csvStream.pipe(writeStream);

      for (let i = 0; i < 5; i++) {
        const response = await client.query(
          `SELECT *, current_timestamp AS current_ts, statement_timestamp() AS statement_ts FROM "Million" OFFSET ${
            100 * i
          } LIMIT 100;`
        );
        response.rows.forEach((row) => {
          csvStream.write(row);
        });
      }
      await client.query("COMMIT");
      csvStream.end();
    } catch (err) {
      await client.query("ROLLBACK");
    }
  });
}

function withoutTransaction(client,filename) {
  return new Promise(async (resolve, reject) => {
    try {
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv
        .format({ headers: true })
        .transform((row) => ({
          name: row.name,
          joindate: row.joindate.toJSON(),
          current_ts: row.current_ts.toJSON(),
          statement_ts: row.statement_ts.toJSON(),
        }))
        .on("end", () => {
          writeStream.end();
          resolve();
        });

      csvStream.pipe(writeStream);

      for (let i = 0; i < 5; i++) {
        const response = await client.query(
          `SELECT *, current_timestamp AS current_ts, statement_timestamp() AS statement_ts FROM "Million" OFFSET ${
            100 * i
          } LIMIT 100;`
        );
        response.rows.forEach((row) => {
          csvStream.write(row);
        });
      }
      csvStream.end();
    } catch (err) {}
  });
}

function withStream(client,filename) {
  return new Promise(async (resolve, reject) => {
    try {
      const writeStream = fs.createWriteStream(filename);

      let selectQuery = `SELECT *, current_timestamp AS current_ts, statement_timestamp() AS statement_ts FROM "Million" LIMIT 500;`;
      const stream = client.query(new QueryStream(selectQuery));

      stream
        .pipe(csv.format({ headers: true }))
        .transform((row, next) => {
          return next(null, {
            name: row.name,
            joindate: row.joindate.toJSON(),
            current_ts: row.current_ts.toJSON(),
            statement_ts: row.statement_ts.toJSON(),
          });
        })
        .pipe(writeStream);

      stream.on("end", () => {
        writeStream.end();
        resolve();
      });
    } catch (err) {}
  });
}

(async () => {
  try {
    await client.connect();

    let createQuery = `CREATE TABLE IF NOT EXISTS "Million" (name character varying(255) COLLATE pg_catalog."default", joindate date );`;
    await client.query(createQuery);

    let truncateQuery = `TRUNCATE "Million";`;
    await client.query(truncateQuery);

    let insertQuery = `INSERT INTO "Million" (name, joindate) SELECT substr(md5(random()::text), 1, 10), DATE '2018-01-01' + (random() * 700)::integer FROM generate_series(1, 100000);`;
    await client.query(insertQuery);

    await withTransaction(client,'./output/withTransaction.csv');
    await withoutTransaction(client,'./output/withoutTransaction.csv');
    await withStream(client,'./output/stream.csv');

    // client.end();
    console.log('done');
  } catch (error) {
    console.error(error);
  }
})();
