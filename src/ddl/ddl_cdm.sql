CREATE TABLE IF NOT EXISTS cdm.couriers_pay_mart 
(
	id	serial PRIMARY KEY,
	courier_id	integer,
	courier_name	varchar,
	settlement_year	integer,
	settlement_month	integer,
	orders_count	integer,
	orders_total_sum	numeric(14, 2),
	rate_avg	NUMERIC,
	order_processing_fee	NUMERIC,
	courier_order_sum	numeric(14, 2),
	courier_tips_sum	numeric(14, 2),
	courier_reward_sum	numeric(14, 2)
);
ALTER TABLE cdm.couriers_pay_mart ADD CONSTRAINT courier_unique UNIQUE (courier_id, settlement_year, settlement_month);