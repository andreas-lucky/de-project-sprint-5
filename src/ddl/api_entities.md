1. C����� �����, ������� ���������� ��� �������.
id - ������������� ������.
courier_id - ID �������, �������� �����������.
courier_name - ��� �������.
settlement_year - ��� ������.
settlement_month - ����� ������, ��� 1 - ������ � 12 - �������.
orders_count - ���������� ������� �� ������ (�����).
orders_total_sum - ����� ��������� �������.
rate_avg - ������� ������� ������� �� ������� �������������.
order_processing_fee - �����, ���������� ��������� �� ��������� �������, ������� ������������� ��� orders_total_sum * 0.25.
courier_order_sum - �����, ������� ���������� ����������� ������� �� ������������ ��/�� ������. �� ������ ������������ ����� ������ ������ �������� ��������� ����� � ����������� �� �������� (��. ����).
courier_tips_sum - �����, ������� ������������ �������� ������� � �������� ������.
courier_reward_sum - �����, ������� ���������� ����������� �������.

2. ������ ������ � ���� DDS, �� ������� �� ������� ���� ��� �������. ��������, ����� ������� ��� ���� � ���������, � ����� ���� ���. 
����������� ������� �� ��������� �������. �������, ��� ��� ����� ����������.

������� dds.dm_couriers 
����
	courier_id_dwh serial4 NOT NULL,
	courier_id_source varchar(30) NULL,
	"name" varchar NULL
	
������� dds.dm_deliveries 
����	
	delivery_id_dwh serial4 NOT NULL,
	delivery_id_source varchar(30) NULL,

������� dds.dm_orders 
����
	order_id_dwh serial4 NOT NULL,
	order_id_source varchar(30) NULL
	
������� dds.fct_deliveries 
����
	order_id_dwh int4 NOT NULL,
	delivery_id_dwh int4 NULL,
	courier_id_dwh int4 NULL,
	order_ts timestamp NULL,
	delivery_ts timestamp NULL,
	address varchar NULL,
	rate int4 NULL,
	tip_sum numeric(14, 2) NULL,
	total_sum numeric(14, 2) NULL,	
3. �� ������ ������ ������ � DDS ��������� ������ ��������� � �����, ������� ���������� ��������� �� API. 
������������ ��� ������ API �������������: ����� ��������� �� ����������, ������� ����� ��� ���������� ������.

������� stg.couriers 
����:
	courier_id varchar(30) NULL,
	"name" varchar NULL,

������� stg.deliveries 
����: 
order_id varchar(30) NULL,
order_ts timestamp NULL,
delivery_id varchar(30) NULL,
courier_id varchar(30) NULL,
address varchar NULL,
delivery_ts timestamp NULL,
rate int4 NULL,
sum numeric(14, 2) NULL,
tip_sum numeric(14, 2) NULL,
