

-- 1. Справочник магазинов:

CREATE TABLE IF NOT EXISTS std18_4_fp.plants
(
    plant varchar(20) NOT null PRIMARY KEY,
    txt_p varchar(20)
)
DISTRIBUTED REPLICATED;


-- 2. Справочник акций:

CREATE TABLE IF NOT EXISTS std18_4_fp.promos
(
    id varchar NOT null PRIMARY KEY,
    name varchar(20),
    promo_type int8,
    material int8,
    discount int
)
DISTRIBUTED REPLICATED;


-- 3. Справочник типов акций:

create table if not exists std18_4_fp.promo_types
(
id varchar(20) not null PRIMARY KEY,
txt_pt text
)

distributed replicated;


-- 1. таблица фактов: bills_head

/*
На источнике есть свой PK
В целевой таблице это же поле и используется для ключа распределения
*/

CREATE TABLE IF NOT EXISTS std18_4_fp.bills_head (
    billnum int8 NULL,
    plant bpchar(4) NULL,
    calday date NULL
)
WITH (
    appendoptimized = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
DISTRIBUTED BY (billnum) -- Ключ дистрибуции!
PARTITION BY RANGE (calday) (
    START (date '2021-01-01') 
    END (date '2021-03-01') 
    EVERY (interval '1 month'),
    DEFAULT PARTITION def
);


-- 2. таблица фактов: billsitem

/*
Нет ключа распределения.
Решила использовать поле как у bills_head считая,
что в дальнейшем эти две таблицы будут часто джойниться по этому ключу
*/

CREATE TABLE IF NOT EXISTS std18_4_fp.bills_item (
    billnum int8 NULL,
    billitem int8 NULL,
    material int8 NULL,
    qty int8 NULL,
    netval numeric(17, 2) NULL,
    tax numeric(17, 2) NULL,
    rpa_sat numeric(17, 2) NULL,
    calday date NULL
)
WITH (
    appendoptimized = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE (calday) (
    START (date '2021-01-01') 
	END (date '2021-03-01') 
	EVERY (interval '1 month'),
    DEFAULT PARTITION def
);


-- 3. таблица фактов: traffic
/*
На источнике три поля PK: plant, date, time
Выбрала для ключа дистрибуции поле plant 
считая, что в дальнейшем будут проходить по нему частые джойны.
*/

CREATE TABLE IF NOT EXISTS std18_4_fp.traffic (
    plant bpchar(4) NULL,
    date date NULL,
    time time NULL,
    frame_id bpchar(10) NULL,
    quantity int4 NULL
)
WITH (
    appendoptimized = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
DISTRIBUTED BY (plant)
PARTITION BY RANGE (date) (
    START (date '2021-01-01') 
	END (date '2021-03-01') 
	EVERY (interval '1 month'),
    DEFAULT PARTITION def
);


-- 4. таблица фактов: coupons
/*
На источнике есть поле PK.
Решила его использовать как ключ распределения
*/

create table if not exists std18_4_fp.coupons (
    plant bpchar(4) NULL,
    day_c date NULL,
    number_b varchar(10) NULL, -- Оставляем как ключ дистрибуции
    coupon_promo text NULL,
    material int8 null,
    billnum int8 null
) with (
    appendoptimized = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
) 
distributed by (number_b) -- Хэш-распределение по уникальному varchar-полю
partition by range (day_c) (
    start (date '2021-01-01') 
    end (date '2021-03-01') 
    every (interval '1 month'),
    default partition def
);


-- Витрина

/*
Витрина с полем для даты и партицированием по дням.
Без вычисляемых финальных метрик
чтобы в Superset при переключении фильтра с «День» на «Месяц» 
правильно вычислялись метрики
*/
DROP TABLE IF EXISTS std18_4_fp.dm_plant_efficiency;

CREATE TABLE std18_4_fp.dm_plant_efficiency (
    report_date    date,
    plant          varchar(20),
    plant_txt      varchar(20),
    oborot         numeric,
    coupon_discount numeric,
    oborot_net     numeric,
    sold_qty       numeric,
    bills_cnt      int8,
    traffic_cnt    int8,
    promo_qty      int8
)
WITH (
    appendoptimized = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 3
)
DISTRIBUTED BY (plant, report_date)
PARTITION BY RANGE (report_date)
(
    START (date '2021-01-01') INCLUSIVE
    END   (date '2021-03-01') EXCLUSIVE
    EVERY (interval '1 month'),
    DEFAULT PARTITION other
);


