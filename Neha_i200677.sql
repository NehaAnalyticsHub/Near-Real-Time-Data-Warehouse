#Determine the top 3 store names who generated highest sales in September, 2017.
#Query No 1
select STORE_NAME from i200677.location
where STORE_ID in 
(select STORE_ID from i200677.tranformed
where TIME_ID 
in (select TIME_ID from i200677.time1
where MONTH(T_DATE)='09') order by sales DESC limit 3);


 
#Query No 2
select supplier_id from i200677.tranformed
where TIME_ID in 
(
select TIME_ID from i200677.time1
group by week(T_DATE)) order by sales DESC limit 10;

#Query No 3
select sum(sales) from i200677.tranformed
where TIME_ID in (
select TIME_ID from i200677.time1
group by month(T_DATE),quarter(T_DATE))
group by PRODUCT_ID,supplier_id ;

#Query No 4
select sum(sales) from i200677.tranformed
group by STORE_ID,PRODUCT_ID order by STORE_ID,PRODUCT_ID;

#Query No 5
Select sum(sales) from i200677.tranformed
where TIME_ID in
(select TIME_ID from i200677.time1
where quarter(T_DATE))
group by STORE_ID;


#Query No 6
select PRODUCT_NAME from i200677.products
where PRODUCT_ID in
(select PRODUCT_ID  from i200677.tranformed
where sum(sales) group by TIME_ID in (
select TIME_ID from i200677.time1
group by week(T_DATE)));

 
#Query No 7
select s.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME, sum(t.SALES)
from i200677.products p, i200677.location s, i200677.tranformed t, i200677.supplier sp
where t.PRODUCT_ID = p.PRODUCT_ID and t.STORE_ID = s.STORE_ID and t.SUPPLIER_ID = sp.SUPPLIER_ID
group by s.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME with rollup;
 
 #Query No 9
 SELECT PRODUCT_NAME, COUNT(PRODUCT_NAME) FROM i200677.products 
GROUP BY PRODUCT_NAME HAVING COUNT(PRODUCT_NAME)>1;

#Query No 10
CREATE TABLE i200677.materialisedview (
    id  INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
  , i200677 VARCHAR(64)  NOT NULL
  , products  VARCHAR(64)  NOT NULL
  , location VARCHAR(64)  NOT NULL
  , row_count   INT UNSIGNED NOT NULL
);
SELECT i200677, products,location, table_rows
  FROM i200677.tables
 WHERE table_type = 'BASE TABLE';
 #How the materialized view helps in OLAP query optimisation?
 #It Reduce the execution time for complex queries with JOINs and aggregate functions. 
 #The more complex the query, the higher the potential for execution-time saving. 
 #The most benefit is gained when a query's computation cost is high and the resulting data set is small.