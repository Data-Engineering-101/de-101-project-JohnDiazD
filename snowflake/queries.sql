/*Queries*/ 

/*Query the top 5 sales by product*/

WITH SALES_BY_PRODUCT AS (
    select SUM(sales) sales,p.product_id 
    from NIKE.PUBLIC.SALE s
    JOIN  NIKE.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.product_id
    order by sales desc
    limit 5
)
SELECT s.product_id,s.sales FROM SALES_BY_PRODUCT s;

/*Query the top 5 sales by category agrupation*/

WITH SALES_BY_CATEGORY AS (
    select SUM(sales) sales,p.category from NIKE.PUBLIC.SALE s
    LEFT JOIN  NIKE.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.category
    order by sales desc
    limit 5
)
SELECT s.category, s.sales FROM SALES_BY_CATEGORY s;

/*Query the least 5 sales by category agrupation*/

WITH SALES_BY_CATEGORY_LEAST AS (
    select SUM(sales) sales,p.category from NIKE.PUBLIC.SALE s
    LEFT JOIN  NIKE.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.category
    order by sales asc
    limit 5
)
SELECT s.category, s.sales FROM SALES_BY_CATEGORY_LEAST s;


/*Query the top 5 sales by title and subtitle agrupation*/

WITH SALES_BY_TITLE_SUBTITLE AS (
    select SUM(sales) sales,p.title,p.subtitle from NIKE.PUBLIC.SALE s
    LEFT JOIN  NIKE.PUBLIC.PRODUCT p on p.uid = s.uid
    group by p.title,p.subtitle
    order by sales desc
    limit 5
)
SELECT s.title, s.subtitle, s.sales FROM SALES_BY_TITLE_SUBTITLE s;

/*Query the top 3 products that has greatest sales by category*/

WITH SALES_BY_CATEGORY AS (
    select SUM(sales) sales,p.category,p.product_id from NIKE.PUBLIC.SALE s
    JOIN  NIKE.PUBLIC.PRODUCT p on p.uid = s.uid
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