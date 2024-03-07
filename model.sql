drop table ITEMS;
drop table Mytable;
drop table delivery;
drop table payment;
CREATE TABLE delivery(
	name varchar(30) ,
	phone varchar(15),
	zip varchar(15),
	city varchar(30),
	adress varchar(50),
	region varchar(30),
	email varchar(50),
	delivery_id SERIAL UNIQUE 
	
);
create table  payment(
 	transaction  varchar(30) UNIQUE,
    request_id VARCHAR(30),
    currency varchar(5),
    provider varchar(15),
    amount int,
    payment_dt int,
    bank varchar(25),
    delivery_cost int,
    goods_total int,
	custom_fee int,
	ID SERIAL
);
create table MyTable(
	entry	VARCHAR(10),
	track_number varchar(30) UNIQUE ,
	locale  varchar(15),
    internal_signature varchar(15),
  	customer_id varchar(15),
  	delivery_service varchar(15),
  	shardkey varchar(15),
   	sm_id int,
 	date_created varchar(25),
 	oof_shard varchar(15),
 	ID SERIAL UNIQUE
	);
create table items( 
	chrt_id int ,
	track_number varchar (30),
	price int,
	rid varchar(40),
	name varchar(30),
	sale int,
	size varchar(5),
	nm_id int,
	brand varchar(40),
	status int,
	ID serial,
	order_ID int,
	foreign key (track_number) references mytable (track_number),

	foreign key (ORDER_ID) references mytable (ID)
);

alter table mytable  add delivery_id SERIAL references delivery (delivery_id); 
alter table mytable  add order_uid VARCHAR(30) references payment(transaction);