-- 买入表
drop table if exists order_buy;
create table order_buy (mobile bigint,price decimal(10,3),amount  bigint,order_time datetime(6),key (price,order_time,mobile));
-- 卖出表
drop table if exists order_sell;
create table order_sell(mobile bigint,price decimal(10,3),amount bigint,order_time datetime(6),key (price,order_time,mobile));
-- 用户表
drop table if exists users;
create table users
(id int auto_increment primary key,mobile bigint,name varchar(12)
,identity varchar(20),password_hash varchar(128),role_id int);
-- 持仓表
drop table if exists holding;
create table holding(id int auto_increment primary key,mobile bigint,amount int
,amount_free int,price decimal(8,2),key (mobile));
-- 成交表
drop table if exists trade;
create table trade 
(id int auto_increment primary key,mobile bigint,amount int,price decimal(8,2),fee decimal(8,2)
,trade_type TINYINT,trade_time datetime(6));




call p_trade()

drop procedure if exists p_trade;

delimiter //
create procedure p_trade()
begin 
	
	declare v_price_buy decimal(8,2);
	declare v_order_time_buy datetime(6);
	declare v_amount_buy int;
	declare v_mobile_buy bigint;
	
	DECLARE cursor_null INT DEFAULT FALSE;
	DECLARE sql_error   INT DEFAULT FALSE;

	DECLARE CURSOR_buy CURSOR FOR 
	SELECT distinct b.price,b.order_time,b.amount,b.mobile
	FROM order_sell a 
	join order_buy  b on a.price<=b.price
	order by 1 desc,2;

	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET sql_error=True;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET cursor_null=True;	
	
	
	drop TEMPORARY table if exists trade_temp;
	create TEMPORARY table trade_temp
	as 
	select	 a.price      as price_sell
			,a.order_time as order_time_sell
			,a.amount     as amount_sell
			,a.mobile     as mobile_sell
			,b.price      as price_buy 
			,b.order_time as order_time_buy
			,b.amount     as amount_buy 
			,b.mobile     as mobile_buy 
			,0        as amount_trade
			,a.amount as amount_sell_left
			,b.amount as amount_buy_left
	from order_sell a 
	join order_buy  b on a.price<=b.price;
	
	create index index_trade_temp1 on trade_temp(price_sell,order_time_sell,mobile_sell);
	create index index_trade_temp2 on trade_temp(price_buy,order_time_buy,mobile_buy);
	
	open CURSOR_buy;
	read_loop: loop
		FETCH CURSOR_buy INTO v_price_buy,v_order_time_buy,v_amount_buy,v_mobile_buy;
		if cursor_null then 
			LEAVE read_loop; 
		end if;
		
		drop TEMPORARY table if exists v_trade;
		create TEMPORARY table v_trade
		as 
		select a.price_sell,a.order_time_sell,a.mobile_sell
			,case when a.amount_sell_left>=@amount_buy_left then a.amount_sell_left-@amount_buy_left
					else 0 end as amount_sell_left
			,case when a.amount_sell_left>=@amount_buy_left then @amount_buy_left
					else a.amount_sell_left end as amount_trade
			,case when a.amount_sell_left>=@amount_buy_left then @amount_buy_left:=0
					else @amount_buy_left:=@amount_buy_left-a.amount_sell_left end as amount_buy_left_last
		from (
				select distinct price_sell,order_time_sell,mobile_sell,amount_sell_left 
				from trade_temp where price_sell<=v_price_buy and amount_sell_left>0
				order by price_sell,order_time_sell
			) a,(select @amount_buy_left:=v_amount_buy,@amount_sell_left:=0) b;
		
		update trade_temp a
		join v_trade b on a.price_sell=b.price_sell and a.order_time_sell=b.order_time_sell and a.mobile_sell=b.mobile_sell
		set a.amount_trade=b.amount_trade,a.amount_buy_left=@amount_buy_left
		where a.price_buy=v_price_buy and a.order_time_buy=v_order_time_buy and a.mobile_buy=v_mobile_buy;
		
		update trade_temp a
		join v_trade b on a.price_sell=b.price_sell and a.order_time_sell=b.order_time_sell and a.mobile_sell=b.mobile_sell
		set a.amount_sell_left=b.amount_sell_left;

	end loop;
	
	close CURSOR_buy;
	
	
	-- 更新表
	start transaction;
	
	insert into trade(mobile,amount,price,fee,trade_type,trade_time)
	select mobile_sell,sum(amount_trade),price_sell,sum(amount_trade)*price_sell,0,now(6)
	from trade_temp where amount_trade>0 group by mobile_sell,price_sell;
	
	insert into trade(mobile,amount,price,fee,trade_type,trade_time)
	select mobile_buy,sum(amount_trade),price_sell,sum(amount_trade)*price_sell,1,now(6)
	from trade_temp where amount_trade>0 group by mobile_buy,price_sell;
	
	update holding a 
	join (
			select mobile_sell as mobile,sum(amount_trade) as amount_trade 
			from trade_temp where amount_trade>0 group by mobile_sell
		 ) b on a.mobile=b.mobile
	set amount=amount-amount_trade,amount_free=amount_free-amount_trade;
	
	update holding a 
	join (
			select mobile_buy  as mobile,sum(amount_trade) as amount_trade 
			from trade_temp where amount_trade>0 group by mobile_buy
		 ) b on a.mobile=b.mobile
	set amount=amount+amount_trade,amount_free=amount_free+amount_trade;
	
	
	update order_buy a 
	join (
			select distinct price_buy,order_time_buy,mobile_buy,amount_buy_left 
			from trade_temp where amount_buy_left>0 and amount_buy_left<amount_buy
		 ) b on a.price=b.price_buy and a.order_time=b.order_time_buy and a.mobile=b.mobile_buy
	set a.amount=b.amount_buy_left;
	
	delete a 
	from order_buy a 
	join (
			select distinct price_buy,order_time_buy,mobile_buy 
			from trade_temp where amount_buy_left=0
		 ) b on a.price=b.price_buy and a.order_time=b.order_time_buy and a.mobile=b.mobile_buy;
	
	update order_sell a 
	join (
			select distinct price_sell,order_time_sell,mobile_sell,amount_sell_left 
			from trade_temp where amount_sell_left>0 and amount_sell_left<amount_sell
		 ) b on a.price=b.price_sell and a.order_time=b.order_time_sell and a.mobile=b.mobile_sell
	set a.amount=b.amount_sell_left;
	
	delete a 
	from order_sell a 
	join (
			select distinct price_sell,order_time_sell,mobile_sell 
			from trade_temp where amount_sell_left=0
		 ) b on a.price=b.price_sell and a.order_time=b.order_time_sell and a.mobile=b.mobile_sell;
	
		
	if sql_error then 
		rollback;
	else 
		commit;
	end if;
	

end
//





/*
call p_trade(13501537141,5,5,1)

drop procedure p_trade;

delimiter //
create procedure p_trade(p_mobile bigint,p_price decimal(8,2),p_amount int,trade_type TINYINT)
begin 
	
	declare v_mobile bigint;
	declare v_price decimal(8,2);
	declare v_amount int;
	declare amount_tmp int default p_amount;
	DECLARE cursor_null INTEGER DEFAULT 0;
	DECLARE sql_error INTEGER DEFAULT 0;
	
	DECLARE CURSOR_buy CURSOR FOR SELECT mobile,price,amount FROM order_sell where price<=p_price order by price,order_time;

	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET sql_error=1;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET cursor_null = 1;	
	
	open CURSOR_buy;

	start transaction;
	read_loop: loop
		FETCH CURSOR_buy INTO v_mobile,v_price,v_amount;
		if cursor_null or amount_tmp=0 then 
			LEAVE read_loop; 
		end if;
		if amount_tmp<v_amount then
			update order_sell set amount=v_amount-amount_tmp where mobile=v_mobile and price=v_price;
			select sleep(10);
			insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
			values (p_mobile,amount_tmp,v_price,0,1,now(6)),(v_mobile,amount_tmp,v_price,0,0,now(6));
			set amount_tmp=0;
		elseif amount_tmp>v_amount then
			delete from order_sell where mobile=v_mobile and price=v_price;
			insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
			values (p_mobile,v_amount,v_price,0,1,now(6)),(v_mobile,v_amount,v_price,0,0,now(6));
			set amount_tmp=amount_tmp-v_amount;
		else
			delete from order_sell where mobile=v_mobile and price=v_price;
			insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
			values (p_mobile,v_amount,v_price,0,1,now(6)),(v_mobile,v_amount,v_price,0,0,now(6));
			set amount_tmp=0;
		end if;
	end loop;
	
	if sql_error then 
		rollback;
	else 
		commit;
	end if;
	
	close CURSOR_buy;

end
//
*/


	
	/*
	FETCH CURSOR_buy INTO v_mobile,v_price,v_amount;
	if not cursor_null then
	
		while amount_tmp>0 and not cursor_null do
			
			if amount_tmp<v_amount then
				update order_sell set amount=v_amount-amount_tmp where mobile=v_mobile and price=v_price;
				insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
				values (p_mobile,amount_tmp,v_price,0,1,now(6)),(v_mobile,amount_tmp,v_price,0,0,now(6));
				set amount_tmp=0;
			elseif amount_tmp>v_amount then
				delete from order_sell where mobile=v_mobile and price=v_price;
				insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
				values (p_mobile,v_amount,v_price,0,1,now(6)),(v_mobile,v_amount,v_price,0,0,now(6));
				set amount_tmp=amount_tmp-v_amount;
			else
				delete from order_sell where mobile=v_mobile and price=v_price;
				insert into trade(mobile,amount,price,fee,trade_type,trade_time) 
				values (p_mobile,v_amount,v_price,0,1,now(6)),(v_mobile,v_amount,v_price,0,0,now(6));
				set amount_tmp=0;
			end if;
			FETCH CURSOR_buy INTO v_mobile,v_price,v_amount;
		end while;

	end if;
	*/

