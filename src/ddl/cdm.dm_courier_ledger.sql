with agg_calc as (
	select
	courier_id_dwh,
	date_trunc('month',order_ts) as order_m,
	count(order_id_dwh) as orders_count,
	sum(total_sum) as orders_total_sum,
	avg(rate) as rate_avg,
	sum(tip_sum) as courier_tips_sum
	from dds.fct_deliveries fd
	group by courier_id_dwh, date_trunc('month',order_ts)
),
courier_sum as (
	select a2.courier_id_dwh, a2.order_m,
	sum( CASE
	WHEN a2.rate_avg < 4 THEN CASE WHEN d2.total_sum * 0.05 < 100 THEN 100 ELSE d2.total_sum * 0.05 END
	WHEN 4 <= a2.rate_avg and a2.rate_avg < 4.5 THEN CASE WHEN d2.total_sum * 0.07 < 150 THEN 150 ELSE d2.total_sum * 0.07 end
	WHEN 4.5 <= a2.rate_avg and a2.rate_avg < 4.9 THEN CASE WHEN d2.total_sum * 0.08 < 175 THEN 175 ELSE d2.total_sum * 0.08 end
	WHEN 4.9 <= a2.rate_avg THEN CASE WHEN d2.total_sum * 0.1 < 200 THEN 200 ELSE d2.total_sum * 0.1 END
	end) as courier_order_sum 
	from agg_calc a2 
	inner join (select courier_id_dwh, date_trunc('month',order_ts) as order_m, total_sum
				from dds.fct_deliveries) d2 on a2.courier_id_dwh = d2.courier_id_dwh and a2.order_m = d2.order_m
	group by a2.courier_id_dwh, a2.order_m
)

select 
	row_number() OVER () as id,
	ac.courier_id_dwh as courier_id,
	dc."name",
	to_char(date_trunc('month',ac.order_m),'YYYY') as settlement_year, 
	cast(to_char(date_trunc('month',ac.order_m),'MM') as int) as settlement_month,
	ac.orders_count,
	ac.orders_total_sum ,
	ac.rate_avg,
	ac.orders_total_sum * 0.25 as order_processing_fee,
	cs.courier_order_sum,
	ac.courier_tips_sum ,
	cs.courier_order_sum + ac.courier_tips_sum * 0.95 as courier_reward_sum 

from agg_calc ac
inner join dds.dm_couriers dc on dc.courier_id_dwh = ac.courier_id_dwh
inner join courier_sum cs on ac.courier_id_dwh = cs.courier_id_dwh and ac.order_m = cs.order_m;
