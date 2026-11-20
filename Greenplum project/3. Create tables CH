-- 1. Создание интеграционной таблицы (для связи с greenplum)


CREATE TABLE std18_4_fp.ch_dm_plant_efficiency_ext
(
    report_date Date,
    plant String,
    plant_txt String,
    oborot Decimal(15, 2),
    coupon_discount Decimal(15, 2),
    oborot_net Decimal(15, 2),
    sold_qty Decimal(10, 2),
    bills_cnt Int64,
    traffic_cnt Int64,
    promo_qty Int64
)
ENGINE = PostgreSQL
(
    '192.168.214.203:5432', 
    'adb', 
    'dm_plant_efficiency', 
    'std18_4', 
    '2LyDdxjbbEeq0WXR', 
    'std18_4_fp'
);

-- 2. Создание реплецированной таблицы - с партициями по месяцам

CREATE TABLE std18_4_fp.dm_plant_efficiency_local ON CLUSTER default_cluster
(
    `report_date`     Date,
    `plant`           String,
    `plant_txt`       String,
    `oborot`          Decimal(15, 2),
    `coupon_discount` Decimal(15, 2),
    `oborot_net`      Decimal(15, 2),
    `sold_qty`        Decimal(10, 2),
    `bills_cnt`       Int64,
    `traffic_cnt`     Int64,
    `promo_qty`       Int64
)
ENGINE = ReplicatedMergeTree('/click/std18_4_fp.dm_plant_efficiency_local/{shard}', '{replica}')
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, plant)
SETTINGS index_granularity = 8192;


-- 3. Создание распределенной таблицы

CREATE TABLE std18_4_fp.dm_plant_efficiency_distr ON CLUSTER default_cluster
AS std18_4_fp.dm_plant_efficiency_local
ENGINE = Distributed(
    default_cluster,
    std18_4_fp,
    dm_plant_efficiency_local,
    xxHash32(plant)
);
