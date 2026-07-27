
-- We haven’t talked about cloning yet (though that’s coming soon!), but for now, run the following code in a SQL Worksheet in Snowsight so we can mess around with the data in a table and use time travel to fix it without worrying that we’ll corrupt our other datasets.

CREATE TABLE tasty_bytes.raw_pos.truck_dev
   CLONE tasty_bytes.raw_pos.truck;
SELECT * FROM tasty_bytes.raw_pos.truck_dev;
SET saved_query_id = LAST_QUERY_ID();
SET saved_timestamp = CURRENT_TIMESTAMP;
UPDATE tasty_bytes.raw_pos.truck_dev t
   SET t.year = (YEAR(CURRENT_DATE()) -1000);

