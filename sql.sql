-- add million rows
INSERT INTO "Million"
	(name, joindate)
SELECT 
	substr(md5(random()::text), 1, 10), DATE '2018-01-01' + (random() * 700)::integer
FROM 
	generate_series(1, 10000000);


-- show data count
SELECT 
	COUNT(*)
FROM
	"Million";


-- empty the table
TRUNCATE "Million";