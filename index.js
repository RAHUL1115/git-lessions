require("dotenv").config({
  path: process.env.IS_DOCKER ? ".env" : "local.env",
});
const fs = require("fs");
const pgClient = require("pg").Client;
const QueryStream = require("pg-query-stream");
const Cursor = require("pg-cursor");
const csv = require("fast-csv");
const { response } = require("express");

const config = {
  host: process.env.DB_HOST || "localhost",
  port: process.env.DB_PORT || "5432",
  user: process.env.DB_USER || "pguser",
  password: process.env.DB_PASSWORD || "root",
  database: process.env.DB_NAME || "test",
};

const client = new pgClient(config);

function init(client) {
  return new Promise(async (resolve, reject) => {
    try {
      await client.connect();

      let createQuery = `CREATE TABLE IF NOT EXISTS "Million" (name character varying(255) COLLATE pg_catalog."default", joindate date );`;
      await client.query(createQuery);

      let truncateQuery = `TRUNCATE "Million";`;
      await client.query(truncateQuery);

      let insertQuery = `INSERT INTO "Million" (name, joindate) SELECT substr(md5(random()::text), 1, 10), DATE '2018-01-01' + (random() * 700)::integer FROM generate_series(1, ${length});`;
      await client.query(insertQuery);

      if (!fs.existsSync("./output")) {
        fs.mkdirSync("./output");
      }

      resolve();
    } catch (error) {
      console.log('rej');
      reject(error);
    }
  });
}

function withoutTransaction(client, filename) {
  return new Promise(async (resolve, reject) => {
    try {
      // * define streams
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv.format({ headers: true }).transform((row) => ({
        name: row.name,
        joindate: row.joindate.toJSON(),
        current_ts: row.current_ts.toJSON(),
        statement_ts: row.statement_ts.toJSON(),
      }));
      csvStream.pipe(writeStream);

      console.time("withoutTransaction");
      let i = 0;
      let rows = [null];
      while (rows.length) {
        const response = await client.query(
          `${baseQuery} OFFSET ${100 * i} LIMIT ${batchSize};`
        );
        rows = response.rows;
        rows.forEach((row) => {
          csvStream.write(row);
        });
        i++;
      }
      console.timeEnd("withoutTransaction");

      csvStream.end();
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

function withTransaction(client, filename) {
  return new Promise(async (resolve, reject) => {
    try {
      // * define streams
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv.format({ headers: true }).transform((row) => ({
        name: row.name,
        joindate: row.joindate.toJSON(),
        current_ts: row.current_ts.toJSON(),
        statement_ts: row.statement_ts.toJSON(),
      }));
      csvStream.pipe(writeStream);

      console.time("withTransaction");
      await client.query("BEGIN");
      let rows = [null];
      let i = 0;
      while (rows.length) {
        const response = await client.query(
          `${baseQuery} OFFSET ${100 * i} LIMIT ${batchSize};`
        );
        rows = response.rows;
        rows.forEach((row) => {
          csvStream.write(row);
        });
        i++;
      }
      await client.query("COMMIT");

      console.timeEnd("withTransaction");
      csvStream.end();
      writeStream.end();
      resolve();
    } catch (err) {
      await client.query("ROLLBACK");
      reject(err);
    }
  });
}

function withStream(client, filename) {
  return new Promise(async (resolve, reject) => {
    try {
      let selectQuery = `${baseQuery};`;
      // let selectQuery = `SELECT *, current_timestamp AS current_ts, statement_timestamp() AS statement_ts FROM "Million" LIMIT 500;`;

      console.time("withStream");
      const writeStream = fs.createWriteStream(filename);
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
        console.timeEnd("withStream");
        writeStream.end();
        resolve();
      });
    } catch (err) {
      reject(err);
    }
  });
}

function withCursor(client, filename) {
  return new Promise(async (resolve, reject) => {
    try {
      // * define streams
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv.format({ headers: true }).transform((row) => ({
        name: row.name,
        joindate: row.joindate.toJSON(),
        current_ts: row.current_ts.toJSON(),
        statement_ts: row.statement_ts.toJSON(),
      }));
      csvStream.pipe(writeStream);

      // * queryStream and write in queryStream
      console.time("withCursor");
      await client.query("BEGIN;");
      await client.query(`DECLARE million_cur CURSOR FOR ${baseQuery};`);
      let rows = [null];
      while (rows.length) {
        let response = await client.query(
          `FETCH ${batchSize} FROM million_cur;`
        );
        rows = response.rows;
        rows.forEach((row) => {
          csvStream.write(row);
        });
      }
      await client.query("COMMIT;");

      // * post-process cleanup
      console.timeEnd("withCursor");
      csvStream.end();
      writeStream.end();
      resolve();
    } catch (err) {
      await client.query("ROLLBACK");
      reject(err);
    }
  });
}

function withCursorModule(client, filename) {
  return new Promise(async (resolve, reject) => {
    try {
      // * define streams
      const writeStream = fs.createWriteStream(filename);
      const csvStream = csv.format({ headers: true }).transform((row) => ({
        name: row.name,
        joindate: row.joindate.toJSON(),
        current_ts: row.current_ts.toJSON(),
        statement_ts: row.statement_ts.toJSON(),
      }));
      csvStream.pipe(writeStream);

      // * query and stream write
      console.time("withCursorModule");
      let cursor = await client.query(new Cursor(`${baseQuery};`));

      let rows = [null];
      while (rows.length) {
        rows = await cursor.read(batchSize);
        rows.forEach((row) => {
          csvStream.write(row);
        });
      }

      // * post-process cleanup
      console.timeEnd("withCursorModule");
      csvStream.end();
      writeStream.end();
      cursor.close();
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

const baseQuery = `SELECT m1.name, m1.joindate, current_timestamp AS current_ts, statement_timestamp() AS statement_ts FROM "Million" m1 INNER JOIN "Million" m2 ON m1.name = m2.name`;
const batchSize = 100;
const length = 3000000;
(async () => {
  try {
    // * initialize the client connection and create basic setups
    await init(client);

    // await withoutTransaction(client, "./output/withoutTransaction.csv");
    // await withTransaction(client, "./output/withTransaction.csv");
    // await withCursor(client, "./output/cursor.csv");
    // await withStream(client, "./output/stream.csv");
    await withCursorModule(client, "./output/cursorModule.csv");

    //  t->c->cm->s
    client.end();
    console.log("done");

    // * if the process is running in docker it will keep the process running
    process.env.IS_DOCKER ? process.stdin.resume() : null;
  } catch (error) {
    console.error("ts");
    console.error(error);
    process.exit(0);
  }
})();
