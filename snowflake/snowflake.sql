
USE Fundamentals_DB;
USE WAREHOUSE COMPUTE_WH;

DROP TABLE FUNDAMENTALS_DB.PUBLIC.CATEGORY;

select * from FUNDAMENTALS_DB.PUBLIC.CATEGORY;

create or replace TABLE FUNDAMENTALS_DB.PUBLIC.CATEGORY (
	CATEGORY VARCHAR(16777216)
);


DROP TABLE FUNDAMENTALS_DB.PUBLIC.PRODUCT;

select * from FUNDAMENTALS_DB.PUBLIC.PRODUCT;

create or replace TABLE FUNDAMENTALS_DB.PUBLIC.PRODUCT (
	UID VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	SUBTITLE VARCHAR(16777216),
	CATEGORY VARCHAR(16777216),
	PRODUCT_ID VARCHAR(16777216)
);


DROP TABLE FUNDAMENTALS_DB.PUBLIC.SALE;

select * from FUNDAMENTALS_DB.PUBLIC.SALE;


create or replace TABLE FUNDAMENTALS_DB.PUBLIC.SALE (
	UID VARCHAR(16777216),
	currency VARCHAR(16777216),
	sales FLOAT,
	quantity NUMBER(38,0),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0) 
);


/*Queries*/ 

/*Query the top 5 sales by product*/

WITH SALES_BY_PRODUCT AS (
    select SUM(sales) sales,p.product_id 
    from FUNDAMENTALS_DB.PUBLIC.SALE s
    JOIN  FUNDAMENTALS_DB.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.product_id
    order by sales desc
    limit 5
)
SELECT s.product_id,s.sales FROM SALES_BY_PRODUCT s;

/*Query the top 5 sales by category agrupation*/

WITH SALES_BY_CATEGORY AS (
    select SUM(sales) sales,p.category from FUNDAMENTALS_DB.PUBLIC.SALE s
    LEFT JOIN  FUNDAMENTALS_DB.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.category
    order by sales desc
    limit 5
)
SELECT s.category, s.sales FROM SALES_BY_CATEGORY s;

/*Query the least 5 sales by category agrupation*/

WITH SALES_BY_CATEGORY_LEAST AS (
    select SUM(sales) sales,p.category from FUNDAMENTALS_DB.PUBLIC.SALE s
    LEFT JOIN  FUNDAMENTALS_DB.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.category
    order by sales asc
    limit 5
)
SELECT s.category, s.sales FROM SALES_BY_CATEGORY_LEAST s;


/*Query the top 5 sales by title and subtitle agrupation*/

WITH SALES_BY_TITLE_SUBTITLE AS (
    select SUM(sales) sales,p.title,p.subtitle from FUNDAMENTALS_DB.PUBLIC.SALE s
    LEFT JOIN  FUNDAMENTALS_DB.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.title,p.subtitle
    order by sales desc
    limit 5
)
SELECT s.title, s.subtitle, s.sales FROM SALES_BY_TITLE_SUBTITLE s;

/*Query the top 3 products that has greatest sales by category*/

WITH SALES_BY_CATEGORY AS (
    select SUM(sales) sales,p.category,p.product_id from FUNDAMENTALS_DB.PUBLIC.SALE s
    JOIN  FUNDAMENTALS_DB.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.category,p.product_id
    order by category desc
),
TOP_BY_CATEGORY AS (
select *,
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales desc) AS top_number
  from SALES_BY_CATEGORY s
)
SELECT * FROM TOP_BY_CATEGORY
where top_number in (1,2,3);
