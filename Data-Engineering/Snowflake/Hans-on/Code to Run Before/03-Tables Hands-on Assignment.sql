-- Use a SQL Worksheet in Snowsight to run the following. We created  test_database and   in a previous assignment, but if you do not have them, run the following code to create them,

CREATE DATABASE test_database;
CREATE SCHEMA test_database.test_schema;


-- Use the  test_database and test_schema to create the test_table and insert the data,

USE DATABASE test_database;
USE SCHEMA test_schema;

CREATE TABLE TEST_TABLE (
   TEST_NUMBER NUMBER,
   TEST_VARCHAR VARCHAR,
   TEST_BOOLEAN BOOLEAN,
   TEST_DATE DATE,
   TEST_VARIANT VARIANT,
   TEST_GEOGRAPHY GEOGRAPHY
);
INSERT INTO TEST_DATABASE.TEST_SCHEMA.TEST_TABLE
 VALUES
 (28, 'ha!', True, '2024-01-01', NULL, NULL);

