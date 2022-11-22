require('dotenv').config();
const fs = require('fs');
const pgClient = require('pg').Client
const Cursor = require('pg-cursor')
const csv = require('fast-csv');


let client = new pgClient({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});


client.connect().then(()=> {
    const query = {
        name: 'get-name',
        text: 'SELECT * FROM "Million"',
        rowMode: String,
    }
    
    const cursor = client.query(new Cursor(query.text));

    // 1000000
    cursor.read(50000, (err,rows)=>{
        var writeStream = fs.createWriteStream('./output.csv');
        const csvStream = csv.format({ headers: true }).transform(row => ({
            name : row.name,
            joindate : row.joindate,
            currentdate: new Date(),
        }));

        csvStream.pipe(writeStream)

        rows.forEach(row=>{
            csvStream.write(row);
        })

        csvStream.end();
        cursor.close()
        client.end()
    })
});