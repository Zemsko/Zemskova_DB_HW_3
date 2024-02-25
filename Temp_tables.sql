
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


SELECT * FROM temp_transactions
WHERE transaction_id IS NULL