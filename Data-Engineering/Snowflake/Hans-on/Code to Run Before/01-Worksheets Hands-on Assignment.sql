/* Free Snowflake Trial
We’ve reached an exciting moment – Now you’ll get to create your Snowflake account, quickly load some data, and start doing work in Snowflake!
Signing up for a free account is very simple. As part of this course, you get a 120-day free trial account that you can access here.
You should get a welcome email from Snowflake. Follow the instructions on the email to "activate" your account and create a user/password combination to access your account.
And that’s it for signing up! You now have a 120-day free trial Snowflake account.
Ingesting One Table
Next we want to ingest the data you’ll be using in the following assignment. Just open up your free trial account, copy the code below, paste it into a SQL Worksheet, and run it. This will create your “tasty_bytes_sample_data.raw_pos.menu” table and load data into it. You don’t need to worry about what this code is doing – we’ll cover all of this later in the course. */
USE ROLE accountadmin;

USE WAREHOUSE compute_wh;

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

---> create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

---> create the Raw Menu Table
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
   menu_id NUMBER(19,0),
   menu_type_id NUMBER(38,0),
   menu_type VARCHAR(16777216),
   truck_brand_name VARCHAR(16777216),
   menu_item_id NUMBER(38,0),
   menu_item_name VARCHAR(16777216),
   item_category VARCHAR(16777216),
   item_subcategory VARCHAR(16777216),
   cost_of_goods_usd NUMBER(38,4),
   sale_price_usd NUMBER(38,4),
   menu_item_health_metrics_obj VARIANT
);

---> create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

---> query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

---> copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;


-- Great! Now you can move onto your first assignment!
