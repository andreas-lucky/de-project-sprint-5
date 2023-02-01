1. Cписок полей, которые необходимы для витрины.
id - идентификатор записи.
courier_id - ID курьера, которому перечисл€ем.
courier_name - ФИО курьера.
settlement_year - год отчета.
settlement_month - месяц отчета, где 1 - январь и 12 - декабрь.
orders_count - количество заказов за период (месяц).
orders_total_sum - общая стоимость заказов.
rate_avg - средний рейтинг курьера по оценкам пользователей.
order_processing_fee - сумма, удержанна€ компанией за обработку заказов, котора€ высчитываетс€ как orders_total_sum * 0.25.
courier_order_sum - сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. «а каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum - сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum - сумма, которую необходимо перечислить курьеру.

2. Список таблиц в слое DDS, из которых вы возьмЄте пол€ дл€ витрины. ќтметьте, какие таблицы уже есть в хранилище, а каких пока нет. 
Ќедостающие таблицы вы создадите позднее. ”кажите, как они будут называтьс€.

таблица dds.dm_couriers 
поля
	courier_id_dwh serial4 NOT NULL,
	courier_id_source varchar(30) NULL,
	"name" varchar NULL
	
таблица dds.dm_deliveries 
поля	
	delivery_id_dwh serial4 NOT NULL,
	delivery_id_source varchar(30) NULL,

таблица dds.dm_orders 
поля
	order_id_dwh serial4 NOT NULL,
	order_id_source varchar(30) NULL
	
таблица dds.fct_deliveries 
поля
	order_id_dwh int4 NOT NULL,
	delivery_id_dwh int4 NULL,
	courier_id_dwh int4 NULL,
	order_ts timestamp NULL,
	delivery_ts timestamp NULL,
	address varchar NULL,
	rate int4 NULL,
	tip_sum numeric(14, 2) NULL,
	total_sum numeric(14, 2) NULL,	
3. Ќа основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API. 
»спользовать все методы API необ€зательно: важно загрузить ту информацию, котора€ нужна дл€ выполнени€ задачи.

таблица stg.couriers 
поля:
	courier_id varchar(30) NULL,
	"name" varchar NULL,

таблица stg.deliveries 
поля: 
order_id varchar(30) NULL,
order_ts timestamp NULL,
delivery_id varchar(30) NULL,
courier_id varchar(30) NULL,
address varchar NULL,
delivery_ts timestamp NULL,
rate int4 NULL,
sum numeric(14, 2) NULL,
tip_sum numeric(14, 2) NULL,
