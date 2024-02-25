/* Raw transactions. Drop temp table before importing data and then create new one*/
DROP TABLE IF EXISTS temp_transactions;

CREATE TABLE temp_transactions(
    transaction_id integer,
	product_id integer,
	customer_id integer,
	transaction_date timestamp,
	online_order bool,
	order_status varchar,
	brand varchar,
	product_line varchar,
	product_class varchar,
	product_size varchar,
	list_price float8,
	standard_cost float8
);


/*Raw customers. Drop temp table befor importing data and then create new one*/
DROP TABLE IF EXISTS temp_customers;

CREATE TABLE temp_customers(
    customer_id integer,
	first_name varchar,
	last_name varchar,
	gender varchar,
	DOB date,
	job_title varchar,
	job_industry_category varchar,
	wealth_segment varchar,
	deceased_indicator varchar,
	owns_car varchar,
	address varchar,
	postcode varchar,
	state varchar,
	country varchar,
	property_valuation varchar
);

/*1.Вывести распределение (количество) клиентов по сферам деятельности,
отсортировав результат по убыванию количества.*/


SELECT count(customer_id), job_industry_category 
FROM customer
GROUP BY job_industry_category 
ORDER BY count DESC ;


/*2.Найти сумму транзакций за каждый месяц по сферам деятельности,
отсортировав по месяцам и по сфере деятельности.*/ 

with combined_cte as (
 select date_trunc('month', t.transaction_date) as year_and_month, t.standard_cost, c.job_industry_category
 from "transaction" AS t 
 join customer AS c on t.customer_id = c.customer_id 
)
select cte.year_and_month, cte.job_industry_category, sum(cte.standard_cost) from combined_cte cte
group by cte.year_and_month, job_industry_category
order by cte.year_and_month, job_industry_category;


/*3.Вывести количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT*/

SELECT count(t.online_order), t.brand , c.job_industry_category
FROM "transaction"  AS t
JOIN customer AS c ON t.customer_id = c.customer_id
WHERE t.order_status='Approved' AND c.job_industry_category = 'IT' and online_order is true 

GROUP BY t.brand ,c.job_industry_category;

/*4.Найти по всем клиентам сумму всех транзакций (list_price), 
 максимум, минимум и количество транзакций, отсортировав результат по убыванию суммы транзакций
 и количества клиентов. Выполните двумя способами: используя только group by и используя только оконные функции. 
 Сравните результат.*/

/* 1. Способ, используя group by*/

SELECT c.customer_id, sum(t.list_price), max(t.list_price), min(t.list_price)
FROM "transaction"  AS t
JOIN customer AS c ON t.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY  c.customer_id DESC, sum(t.list_price) DESC, max(t.list_price), min(t.list_price);

/*2.Способ Запрос с использованием оконных функций*/
SELECT c.customer_id,
	sum(t.list_price)OVER (PARTITION BY c.customer_id) AS sum_transaction ,
	max(t.list_price)OVER (PARTITION BY c.customer_id) AS max_transaction,
	min(t.list_price)OVER (PARTITION BY c.customer_id) AS min_transaction

FROM "transaction"  AS t
JOIN customer AS c ON t.customer_id = c.customer_id;




/*5.Найти имена и фамилии клиентов с минимальной/максимальной суммой транзакций за весь период (сумма транзакций не может быть null).
Напишите отдельные запросы для минимальной и максимальной суммы.*/

/*Запрос для  максимальной суммы транзакций*/
CREATE VIEW t_tbl as (
	
SELECT  c.customer_id, c.first_name, c.last_name, count(t.transaction_id) AS transaction_count ,sum(list_price) as sum
	FROM "transaction"  AS t
	JOIN customer AS c ON t.customer_id = c.customer_id
	GROUP BY c.customer_id, c.first_name,c.last_name);

SELECT  first_name, last_name, sum
FROM t_tbl t
WHERE sum = (SELECT MAX(sum) FROM t_tbl);


/*Запрос для  минимальной суммы транзакций*/
CREATE VIEW s_tbl as (
SELECT  c.customer_id, c.first_name, c.last_name, count(t.transaction_id) AS transaction_count ,sum(list_price) as sum
	FROM "transaction"  AS t
	JOIN customer AS c ON t.customer_id = c.customer_id
	GROUP BY c.customer_id, c.first_name,c.last_name);

SELECT  first_name, last_name, sum
FROM s_tbl t
WHERE sum = (SELECT MIN(sum) FROM s_tbl);




/*6.Вывести только самые первые транзакции клиентов. 
Решить с помощью оконных функций. */

WITH ranked_transactions AS (
    SELECT 
        customer_id,
        transaction_id, 
        transaction_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) AS transaction_rank
    FROM transaction
)
SELECT 
    customer_id, 
    transaction_id, 
    transaction_date
FROM ranked_transactions
WHERE transaction_rank = 1;


/*7.Вывести имена, фамилии и профессии клиентов, 
между транзакциями которых был максимальный интервал 
(интервал вычисляется в днях)*/


WITH transaction_tab AS (
    SELECT 
     c.customer_id,
        c.first_name,
        c.last_name,
        c.job_title,
        
       lag(t.transaction_date) OVER (PARTITION BY t.customer_id order by t.transaction_date) AS lag_transaction,
       lead(t.transaction_date) OVER (PARTITION BY c.customer_id ORDER BY t.transaction_date) AS lead_transaction 
    
        
    FROM customer c
    JOIN transaction t ON c.customer_id = t.customer_id
)
SELECT distinct 
    first_name,
    last_name,
    job_title,
    transaction_tab.lead_transaction - transaction_tab.lag_transaction as max_interval
FROM transaction_tab
where (transaction_tab.lead_transaction - transaction_tab.lag_transaction) = 
(select MAX(transaction_tab.lead_transaction - transaction_tab.lag_transaction) FROM transaction_tab);






   



