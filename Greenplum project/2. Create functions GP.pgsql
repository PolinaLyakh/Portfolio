-- ================================
-- ===== 1. Основные ==============
-- ================================

-- 1. f_delta_partition

/*
Функция для загрузки данных в таблицы: bills_item, bills_head, traffic.
из источника: Postgres
Выполняет все подготовительные действия 
для загрузки данных в целевую таблицу в зависимости от типа таблицы
Не содержит основную логику delta_upsert 
Используется для вызова в DAG
*/


CREATE OR REPLACE FUNCTION std18_4_fp.f_delta_partition
	(
		in p_table text, 
		in p_partition_key text, 
		in p_start_date timestamp, 
		in p_end_date timestamp, 
		in p_pxf_table text, 
		in p_user_id text, 
		in p_pass text, 
		out o_inserted int8, 
		out o_updated int8
	)
	RETURNS record
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    v_ext_table text;
    v_temp_table text;
    v_sql text;
    v_sql_insert text;
    v_pxf text;
    v_dist_key text;
    v_params text;
    v_start_date date;
    v_end_date date;
    v_cnt int8;
    v_filter_end text;
    v_filter_start text;
    v_keys text[];
BEGIN
    v_ext_table := p_table || '_ext';
    v_temp_table := p_table || '_tmp';

    -- Достаем первичный ключ из вспомогательной функции
    v_keys := std18_4_fp.f_get_upsert_key(p_table);
    IF v_keys IS NULL THEN
        RAISE EXCEPTION 'Для таблицы % не задан ключ в f_get_upsert_key', p_table;
    END IF;

    -- Достаем клч дистрибуции и параметры хранения таблицы
    v_dist_key = std18_4_fp.f_get_distr_key(p_table := p_table);
    v_params = std18_4_fp.f_get_with(p_table := p_table);
    RAISE NOTICE 'v_dist_key = % , v_params = %', v_dist_key, v_params;

    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

    v_start_date := date_trunc('month', p_start_date)::date;
    v_end_date   := date_trunc('month', p_end_date)::date;

    -- Проверка, что начальная дата диапазона больше конечной
    IF v_start_date >= v_end_date THEN
        RAISE EXCEPTION 'Начальная дата (%) должна быть меньше конечной даты (%)', v_start_date, v_end_date;
    END IF;

    -- проверка есть ли у функции партиции на этот период и создание, если нету
    PERFORM std18_4_fp.f_check_and_create_partitions(p_table, v_end_date::timestamp);

    /*
    Разветвление для разных обработок таблиц с чеками и traffic
    Из-за поля даты, которое не в стандартном формате 
    у таблицы traffic
    */
    -- Формирование условия для pxf 
    IF lower(p_table) LIKE '%traffic%' THEN
        v_filter_start := to_char(v_start_date, 'DD.MM.YYYY');
        v_filter_end   := to_char(v_end_date, 'DD.MM.YYYY');
    ELSE
        v_filter_start := '''' || v_start_date || '''';
        v_filter_end   := '''' || v_end_date || '''';
    END IF;

    v_pxf := 'pxf://' || p_pxf_table ||
             '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver' ||
             '&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=' || p_user_id || '&PASS=' || p_pass ||
             '&FILTER=' || p_partition_key || ' ge ' || v_filter_start || ' and ' || p_partition_key || ' lt ' || v_filter_end;
    RAISE NOTICE 'pxf = %', v_pxf;

    /*
    Формирование запросов для создания внешней таблицы и INSERT данных
    В зависимости от типа таблицы (чеки или траффик)
    */
    IF lower(p_table) LIKE '%traffic%' THEN
        v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (
                    plant text,
                    "date" text,
                    "time" text,
                    frame_id text,
                    quantity text
                  )
                  LOCATION (''' || v_pxf || ''')
                  ON ALL
                  FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
                  ENCODING ''UTF8''';

        v_sql_insert := 'INSERT INTO ' || v_temp_table || ' (plant, date, time, frame_id, quantity)
                    SELECT
                        plant::bpchar(4),
                        to_date("date", ''DD.MM.YYYY''),
                        trim("time")::time,
                        frame_id::bpchar(10),
                        quantity::int4
                    FROM ' || v_ext_table || '
                    WHERE to_date("date", ''DD.MM.YYYY'') >= ' || quote_literal(v_start_date) || '::date
                      AND to_date("date", ''DD.MM.YYYY'') <  ' || quote_literal(v_end_date) || '::date';
        RAISE NOTICE 'Table is traffic';
    ELSE
        v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (LIKE ' || p_table || ')
                  LOCATION (' || quote_literal(v_pxf) || ')
                  ON ALL
                  FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
                  ENCODING ''UTF8''';

        /*
        Дополнительный фильтр 
        чтобы во временной таблице была только загружаемая дельта, 
        а не все данные из источника
        */
        v_sql_insert := 'INSERT INTO ' || v_temp_table || ' SELECT * FROM ' || v_ext_table ||
                        ' WHERE ' || quote_ident(p_partition_key) || ' >= ' || quote_literal(v_start_date) || '::date' ||
                        '   AND ' || quote_ident(p_partition_key) || ' <  ' || quote_literal(v_end_date) || '::date';
        RAISE NOTICE 'Table is Bills';
    END IF;

    -- Создание внешней таблицы по сформированному ранее скрипту
    EXECUTE v_sql;

    -- Создание tmp с тем же ключом дистрибуции, что и таргет 
    v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table || ';
              CREATE TABLE ' || v_temp_table || ' (LIKE ' || p_table || ') ' || v_params || ' ' || v_dist_key;
    EXECUTE v_sql;
    RAISE NOTICE 'Temp table is created';

    -- Загрузка данных по сформированному ранее скрипту
    EXECUTE v_sql_insert;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RAISE NOTICE 'Total rows loaded from external to temp table: %', v_cnt;

    /*
    Вставка новых / обновление изменённых / пропуск совпавших
    f_upsert_delta выполняет логику загрузки данных 
    и возвращает количество обновляемых и загружаемых строк
    */
    SELECT u.o_inserted, u.o_updated
      INTO o_inserted, o_updated
    FROM std18_4_fp.f_upsert_delta(p_table, v_temp_table, v_keys) u;

    RAISE NOTICE 'Итого % : вставлено = %, обновлено = %, пропущено = %',
                 p_table, o_inserted, o_updated, v_cnt - o_inserted - o_updated;

    EXECUTE 'DROP TABLE IF EXISTS ' || v_temp_table;
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
END;

$$
EXECUTE ON ANY;


-- 2. f_delta_partition_coupon

/*
Функция для загрузки данных в таблицу: coupons.
Выполняет все подготовительные действия 
для загрузки данных в целевую таблицу
Не содержит основную логику delta_upsert 
Используется для вызова в DAG
*/


CREATE OR REPLACE FUNCTION std18_4_fp.f_delta_partition_coupons
	(
		in p_start_date date, 
		in p_end_date date, 
		in p_csv_filename text, 
		out o_inserted int8, 
		out o_updated int8
	)
	RETURNS record
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    v_ext_table text := 'std18_4_fp.coupons_ext';
    v_temp_table text := 'std18_4_fp.coupons_tmp';
    v_target_table text := 'std18_4_fp.coupons';
    v_gpfdist_url text;
    v_sql text;
    v_cnt int8;
BEGIN
	-- Проверка есть ли партиции в целевой таблице на загружаемый период и создание, если из нет
    PERFORM std18_4_fp.f_check_and_create_partitions(v_target_table, p_end_date::timestamp);

    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
    EXECUTE 'DROP TABLE IF EXISTS ' || v_temp_table;

    v_gpfdist_url := 'gpfdist://172.16.128.134:8081/' || p_csv_filename || '.csv';

    -- Создание внешней таблицы
    v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (
                plant bpchar(4),
                day_c text,
                number_b varchar(10),
                coupon_promo text,
                material bigint,
                billnum bigint
              )
              LOCATION (''' || v_gpfdist_url || ''')
              FORMAT ''CSV'' (HEADER DELIMITER '';'' QUOTE ''"'' NULL '''')
              ENCODING ''UTF8''';
    EXECUTE v_sql;

    -- Создание временной таблицы
    v_sql := 'CREATE TABLE ' || v_temp_table || ' (LIKE ' || v_target_table || ')
              WITH (appendoptimized = true, orientation = column, compresstype = zstd)
              DISTRIBUTED BY (number_b)';
    EXECUTE v_sql;

    v_sql := 'INSERT INTO ' || v_temp_table || ' (plant, day_c, number_b, coupon_promo, material, billnum)
              SELECT
                  plant,
                  to_date(day_c, ''YYYYMMDD''),
                  number_b,
                  coupon_promo,
                  material,
                  billnum
              FROM ' || v_ext_table || '
              WHERE to_date(day_c, ''YYYYMMDD'') >= ''' || p_start_date || '''::date
                AND to_date(day_c, ''YYYYMMDD'') < ''' || p_end_date || '''::date';
    EXECUTE v_sql;

    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RAISE NOTICE 'Загружено строк из CSV во временную таблицу: %', v_cnt;

    SELECT u.o_inserted, u.o_updated
      INTO o_inserted, o_updated
    FROM std18_4_fp.f_upsert_delta(v_target_table, v_temp_table,
                                   std18_4_fp.f_get_upsert_key(v_target_table)) u;

    RAISE NOTICE 'Итого coupons: вставлено = %, обновлено = %, пропущено = %',
                 o_inserted, o_updated, v_cnt - o_inserted - o_updated;

    EXECUTE 'DROP TABLE IF EXISTS ' || v_temp_table;
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
END;


$$
EXECUTE ON ANY;


-- 3. f_full_load

/*
Функция для загрузки данных в таблицы словари
Полное удаление данных, а затем загрузка новых 
Используется для вызова в DAG
*/

create or replace function std18_4_fp.f_full_load
	(
		p_table text, 
		p_file_name text
	)
    returns int4
    language plpgsql
    volatile
as $$
declare
v_ext_table_name text;
v_sql text;
v_gpfdist text;
v_result int;
v_where_condition text := '';
v_col record;
v_schema_name text;
v_table_name text;
v_full_table_name text;
v_view_table_name text;
v_sql_at text;
begin
    -- Разделение схемы и имени таблицы по точке
    IF position('.' in p_table) > 0 THEN
        v_schema_name = split_part(p_table, '.', 1);
        v_table_name = split_part(p_table, '.', 2);
        v_full_table_name = p_table;
    ELSE
        -- Если схема не указана, используется схема по умолчанию
        v_schema_name = 'std18_4_fp';
        v_table_name = p_table;
        v_full_table_name = v_schema_name || '.' || v_table_name;
    END IF;
    
    v_ext_table_name = v_full_table_name || '_ext';
    v_view_table_name = v_full_table_name || '_view';
    

    -- Очистка целевой таблицы
    execute 'truncate table ' || v_full_table_name;
    
    -- Собирка структуры колонок вручную из каталога с добавлением _dummy_col для пустого столбца
    SELECT string_agg(quote_ident(attname) || ' ' || format_type(atttypid, atttypmod), ', ') || ', _dummy_col text'
    INTO v_sql_at
    FROM pg_attribute
    WHERE attrelid = v_full_table_name::regclass 
      AND attnum > 0 
      AND NOT attisdropped;

    -- Удаление внешней таблицы, если она существует
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table_name || ' CASCADE';

    -- Формирование URL gpfdist
    v_gpfdist = 'gpfdist://172.16.128.134:8081/' || p_file_name || '.csv';

    -- Создание внешней таблицы
    v_sql = 'CREATE EXTERNAL TABLE ' || v_ext_table_name || ' (' || v_sql_at || ')
            LOCATION (''' || v_gpfdist || ''') ON ALL
            FORMAT ''CSV'' (HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' FILL MISSING FIELDS)
            ENCODING ''UTF8''';
    
    RAISE NOTICE 'External table DDL: %', v_sql;
    EXECUTE v_sql;

    -- Собор списока только оригинальных колонок для SELECT
    SELECT string_agg(quote_ident(attname), ', ') INTO v_sql
    FROM pg_attribute
    WHERE attrelid = v_full_table_name::regclass 
      AND attnum > 0 
      AND NOT attisdropped;

    -- Создание VIEW, которая выбирает только реальные поля и отсекает пустые строки
    EXECUTE 'CREATE OR REPLACE VIEW ' || v_view_table_name || ' AS
             SELECT ' || v_sql || ' 
             FROM ' || v_ext_table_name || '
             WHERE ' || quote_ident(split_part(v_sql, ', ', 1)) || ' IS NOT NULL 
               AND ' || quote_ident(split_part(v_sql, ', ', 1)) || ' != ''''';

    -- Загрузка строк 
    execute 'insert into ' || v_full_table_name || ' 
                Select * 
                from ' || v_view_table_name;
    
    -- Подсчет количества вставленных строк
    execute 'Select count(1) from ' || v_full_table_name into v_result;
    
    RAISE NOTICE 'Inserted % rows into %', v_result, v_full_table_name;
    
    return v_result;
end;
$$
execute on any;


-- 4. f_create_and_load_mart

/*
Функция для загрузки данных в витрину 
Период (p_start_date, p_end_date): обе даты - первые числа месяцев, конечная не включается.
Каждый месяц периода пересчитывается отдельно: данные считаются во временную таблицу и подменяются
на место партиции через EXCHANGE PARTITION. 
Повторный расчёт месяца полностью заменяет предыдущий.
Если за месяц данных нет, партиция очищается.
Пример: ('2021-01-01', '2021-03-01') -> пересчёт января и февраля 2021.
Используется для вызова в DAG
*/


CREATE OR REPLACE FUNCTION std18_4_fp.f_create_and_load_mart(p_start_date date, p_end_date date)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE
    v_target    text := 'std18_4_fp.dm_plant_efficiency';
    v_stg       text := 'std18_4_fp.dm_plant_efficiency_stg';
    v_leaf      text;   -- имя партиции
    v_month     date := p_start_date;
    v_month_end date;
BEGIN

	-- Проверка на случай если даты не являются первыми датами месяца или конечная дата больше начальной
    IF p_start_date IS NULL OR p_end_date IS NULL OR p_end_date <= p_start_date
       OR p_start_date <> date_trunc('month', p_start_date)::date
       OR p_end_date   <> date_trunc('month', p_end_date)::date THEN
        RAISE EXCEPTION 'Ожидаются первые числа месяцев, конец больше начала, получено: % - %', p_start_date, p_end_date;
    END IF;

    -- Цикл по месяцам переданного периода
    WHILE v_month < p_end_date LOOP
        v_month_end := (v_month + interval '1 month')::date; 
        v_leaf := NULL;
        RAISE NOTICE 'Пересчёт месяца %', v_month;

        -- создаёт недостающие партицию для месяца v_month если ее нет в таблице
        PERFORM std18_4_fp.f_check_and_create_partitions(v_target, v_month::timestamp);

        EXECUTE 'DROP TABLE IF EXISTS ' || v_stg;

        -- Создаем временную таблицу по аналогии с целевой
        EXECUTE '
        CREATE TABLE ' || v_stg || ' (LIKE ' || v_target || ')
        WITH (
            appendoptimized = true,
            orientation = column,
            compresstype = zstd,
            compresslevel = 3
        )
        DISTRIBUTED BY (plant, report_date)';

        -- Заполнение временной таблицы 1 месяц
        EXECUTE '
        INSERT INTO ' || v_stg || '
        WITH 
            agg_bills AS (
                SELECT
                    h.plant,
                    h.calday as report_date,
                    SUM(i.rpa_sat) as oborot,                          
                    SUM(i.qty) as sold_qty,                           
                    COUNT(DISTINCT h.billnum) as bills_cnt            
                FROM std18_4_fp.bills_head h
                JOIN std18_4_fp.bills_item i 
                  ON h.billnum = i.billnum 
                  AND h.calday = i.calday
                WHERE h.calday >= $1 AND h.calday < $2
                GROUP BY h.plant, h.calday
            ),

            dedup_bill as (
    			select 
    				billnum,
    				material,
    				max(rpa_sat/qty::numeric) as item_price
    			from std18_4_fp.bills_item
    			where calday >= $1 AND calday < $2
    			group by billnum, material
    		),

    		agg_coupons as (
    			SELECT 
                    c.plant,
                    c.day_c as report_date,
                    COUNT(*) as promo_qty,                            
                    SUM(case when p.promo_type = ''001'' then COALESCE(p.discount, 0)
                    		 else db.item_price * COALESCE(p.discount, 0)/100::numeric
                    		 end
                    ) as coupon_discount  
                FROM std18_4_fp.coupons as c
                LEFT JOIN std18_4_fp.promos as p 
                    ON c.coupon_promo = p.id
                left join dedup_bill db 
                	on c.billnum = db.billnum
                	and c.material = db.material
                WHERE 1=1
                	and c.day_c >= $1 AND c.day_c < $2
                GROUP BY c.plant, c.day_c
    		),

            agg_traffic AS (
                SELECT 
                    plant,
                    date as report_date,
                    SUM(quantity) as traffic_cnt                      
                FROM std18_4_fp.traffic
                WHERE date >= $1 AND date < $2
                GROUP BY plant, date
            ),
        
            calendar_plants AS (
                SELECT DISTINCT plant, report_date FROM agg_bills
                UNION
                SELECT DISTINCT plant, report_date FROM agg_coupons
                UNION
                SELECT DISTINCT plant, report_date FROM agg_traffic
            )

        SELECT 
            cp.report_date::DATE as report_date,
            p.plant as plant, 
            p.txt_p as plant_txt,
        
            COALESCE(b.oborot, 0) as oborot,
            COALESCE(c.coupon_discount, 0) as coupon_discount,
            (COALESCE(b.oborot, 0) - COALESCE(c.coupon_discount, 0)) as oborot_net,
   
            COALESCE(b.sold_qty, 0) as sold_qty,
            COALESCE(b.bills_cnt, 0) as bills_cnt,
            COALESCE(t.traffic_cnt, 0) as traffic_cnt,
            COALESCE(c.promo_qty, 0) as promo_qty

        FROM calendar_plants cp
        JOIN std18_4_fp.plants p 
            ON cp.plant = p.plant
        LEFT JOIN agg_bills b 
            ON cp.plant = b.plant AND cp.report_date = b.report_date
        LEFT JOIN agg_coupons c 
            ON cp.plant = c.plant AND cp.report_date = c.report_date
        LEFT JOIN agg_traffic t 
            ON cp.plant = t.plant AND cp.report_date = t.report_date'
        USING v_month, v_month_end;

        -- подмена партиции месяца целевой таблицы расчётом из stg; 
        -- при пустом расчёте партиция становится пустой
        EXECUTE 'ALTER TABLE ' || v_target ||
                ' EXCHANGE PARTITION FOR (DATE ' || quote_literal(v_month::text) || ')' ||
                ' WITH TABLE ' || v_stg;

        -- в stg теперь расчёт месяца ренее находившийся в целевой таблице (удаляем)
        EXECUTE 'DROP TABLE ' || v_stg;

        /*
        Актуализация метаданных закэшированной статистики именно для того месяца, 
        который только что перезаписали, а затем обновление глобальной статистики таблицы
        */
        -- Поиск имени загруженной партиции по дате
        SELECT p.partitiontablename INTO v_leaf
        FROM pg_partitions p
        WHERE p.schemaname = 'std18_4_fp'
          AND p.tablename = 'dm_plant_efficiency'
          AND p.partitionisdefault = false
          AND replace(replace(p.partitionrangestart, '''', ''), '::date', '')::date = v_month;

        -- Если имя партиции найдено, то выполняется обновление статистики для партиции
        IF v_leaf IS NOT NULL THEN
            EXECUTE 'ANALYZE std18_4_fp.' || quote_ident(v_leaf);
        END IF;
        -- Выполнение глобальной статистики на уровне материнской (корневой) таблицы
        EXECUTE 'ANALYZE ROOTPARTITION ' || v_target;

        v_month := v_month_end;
    END LOOP;

END;

$$
EXECUTE ON ANY;


-- ====================================
-- ===== Технические ===========
-- ====================================

-- 1. f_check_and_create_partition

/*
Функция проверяет есть ли в целевой таблице партиция под передаваемый интервал
Если партиции нет, то функция создает ее из split default  
*/

CREATE OR REPLACE FUNCTION std18_4_fp.f_check_and_create_partitions(
    p_table_name text,
    p_partition_value timestamp
)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
    v_cnt_partitions int;
    v_schema_name text;
    v_table_name text;
    v_partition_end_sql text;
    v_partition_end timestamp;
    v_interval interval := '1 month'::interval; 
    v_ts_format text := 'YYYY-MM-DD HH24:MI:SS';    -- Удалить ???
    v_sql text; 
BEGIN
    -- Разделение схемы и имени таблицы
    IF position('.' in p_table_name) > 0 THEN
        v_schema_name := lower(split_part(p_table_name, '.', 1));
        v_table_name  := lower(split_part(p_table_name, '.', 2));
    ELSE
        -- значение схемы по умолчанию, если схема не указана
        v_schema_name := 'std18_4_fp';
        v_table_name  := lower(p_table_name);
    END IF;

    -- Проверка, партиционирована ли таблица (подсчет количества партиций)
    SELECT count(*) INTO v_cnt_partitions
    FROM pg_partitions as p 
    WHERE p.schemaname = v_schema_name 
      AND p.tablename = v_table_name;
    
    -- Если у таблицы есть партиции (минимум дефолтная + одна обычная)
    IF v_cnt_partitions >= 1 THEN
        LOOP 
            -- Поиск последней даты партиции для последней недефолтной партиции
            SELECT partitionrangeend INTO v_partition_end_sql
            FROM pg_partitions
            WHERE schemaname = v_schema_name 
              AND tablename = v_table_name
              AND partitionisdefault = false 
            ORDER BY 
              replace(replace(partitionrangestart, '''', ''), '::date', '')::date DESC
            LIMIT 1;

            -- Защита от бесконечного цикла
            IF v_partition_end_sql IS NULL THEN
                RAISE EXCEPTION 'Не удалось найти последнюю партицию для таблицы %.%. Проверьте её структуру.', v_schema_name, v_table_name;
            END IF;

            -- Вычисление конечной даты и запись ее в переменную
            EXECUTE 'SELECT ' || v_partition_end_sql INTO v_partition_end;

            -- Условие выхода: если последняя партиция уже покрывает нужную дату
            EXIT WHEN v_partition_end > p_partition_value;

            -- Нарезка новой партиции через SPLIT DEFAULT
            v_sql := 'ALTER TABLE ' || v_schema_name || '.' || v_table_name || 
                     ' SPLIT DEFAULT PARTITION START (' || v_partition_end_sql || ') ' ||
                     ' END (date ' || quote_literal(to_char(v_partition_end + v_interval, 'YYYY-MM-DD')) || ');';
            
            RAISE NOTICE 'Авто-партиционирование: Выполняю SPLIT для таблицы %.% до %', v_schema_name, v_table_name, (v_partition_end + v_interval)::date;
            EXECUTE v_sql;

        END LOOP;
    ELSE
        RAISE NOTICE 'Таблица %.% не имеет партиций в pg_partitions. Пропуск.', v_schema_name, v_table_name;
    END IF;
END;
$$ EXECUTE ON ANY;




-- 2. f_get_distr_key

/*
Функция вычисляет ключ дистрибуции переданной таблицы
и возвращает его в текстовом значении
*/

CREATE OR REPLACE FUNCTION std18_4_fp.f_get_distr_key(p_table text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE 
    v_dist_key text;
    v_table_oid int4;
BEGIN    

    SELECT c.oid INTO v_table_oid
    FROM pg_class c
    JOIN pg_namespace n 
    	ON c.relnamespace = n.oid
    WHERE (n.nspname || '.' || c.relname) = lower(p_table)
    LIMIT 1;

    -- Если ключ распределения не найден, то ключ распределения - рандомный
    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key := 'distributed randomly';
    ELSE
        v_dist_key := pg_get_table_distributedby(v_table_oid);  --   
    END IF;

    RETURN v_dist_key;
END;

$$
EXECUTE ON ANY;



-- 3. f_get_upsert_key

/*
Функция, которая в зависимости от переданного ей имени таблицы 
возвращает список ее уникальных полей(первичные ключи)
Передаются таблицы фактов
*/

CREATE OR REPLACE FUNCTION std18_4_fp.f_get_upsert_key(p_table text)
	RETURNS _text
	LANGUAGE plpgsql
	IMMUTABLE
AS $$
	
BEGIN
    RETURN CASE
        WHEN lower(p_table) LIKE '%bills_head%' THEN ARRAY['billnum']
        WHEN lower(p_table) LIKE '%bills_item%' THEN ARRAY['billnum', 'billitem']
        WHEN lower(p_table) LIKE '%traffic%' THEN ARRAY['plant', 'date', 'time']
        WHEN lower(p_table) LIKE '%coupons%' THEN ARRAY['number_b']
        ELSE NULL
    END;
END;

$$
EXECUTE ON ANY;



-- 4. f_get_with

/*
Функция, которая возвращает строку с параметрами хранения AO таблицы переданной ей.

*/

CREATE OR REPLACE FUNCTION std18_4_fp.f_get_with(
	p_table text
	)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE 
    v_params text;
BEGIN    
    SELECT COALESCE('with( ' || array_to_string(reloptions, ', ') || ')', '')
    INTO v_params
    FROM pg_class
    WHERE oid = p_table::REGCLASS; 

    RETURN v_params;
END;

$$
EXECUTE ON ANY;




-- 5. f_upsert_delta

/*
Функция, которая выполняет основную логику обновления/загрузки данных.
*/

CREATE OR REPLACE FUNCTION std18_4_fp.f_upsert_delta
		(
			in p_target text, 
			in p_delta text, 
			in p_keys _text, 
			out o_inserted int8, 
			out o_updated int8
		)
	RETURNS record
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    v_all_cols  text; 
    v_s_cols    text; 
    v_join      text; 
    v_diff      text;
    v_null_key  text;
    v_cnt       int8;
    v_dup       int8;
    v_nulls     int8;
    v_deleted   int8;
BEGIN
	-- Проверка на наличие ключа в переменной p_keys. Для просто переменной и для массива
    IF p_keys IS NULL OR array_length(p_keys, 1) IS NULL THEN
        RAISE EXCEPTION 'Не задан ключ для таблицы %', p_target;
    END IF;

    -- Колонки берутся из каталога таргета
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum), 
           string_agg('s.' || quote_ident(a.attname), ', ' ORDER BY a.attnum),
           coalesce(string_agg('t.' || quote_ident(a.attname) || ' IS DISTINCT FROM s.' || quote_ident(a.attname),
                               ' OR ' ORDER BY a.attnum)
                    FILTER (WHERE a.attname <> ALL (p_keys)), 'false')
      INTO v_all_cols, v_s_cols, v_diff 
    FROM pg_attribute a
    WHERE a.attrelid = p_target::regclass
      AND a.attnum > 0
      AND NOT a.attisdropped;

    -- Формирование условия соединения и проверки на null для upsert-ключей (PK)
    SELECT string_agg('t.' || quote_ident(k) || ' = s.' || quote_ident(k), ' AND '),
           string_agg('s.' || quote_ident(k) || ' IS NULL', ' OR ') 
      INTO v_join, v_null_key
    FROM unnest(p_keys) AS k;

    EXECUTE 'ANALYZE ' || p_delta;

    -- Контроль дельты: ключ не NULL и не повторяется (иначе будут дубли в таргете) 
    EXECUTE 'SELECT count(*) FROM ' || p_delta || ' s WHERE ' || v_null_key INTO v_nulls;
    IF v_nulls > 0 THEN
        RAISE EXCEPTION 'В дельте % строк с пустым ключом (%)', v_nulls, array_to_string(p_keys, ', ');
    END IF;

    -- Проверка, что ключ не повторяется (из общего количества строк вычитается количество уникальных)
    EXECUTE 'SELECT count(*) - (SELECT count(*) FROM (SELECT 1 FROM ' || p_delta ||
            ' GROUP BY ' || array_to_string(ARRAY(SELECT quote_ident(k) FROM unnest(p_keys) k), ', ') ||
            ') x) FROM ' || p_delta INTO v_dup;
    IF v_dup > 0 THEN
        RAISE EXCEPTION 'В дельте % дублей по ключу (%)', v_dup, array_to_string(p_keys, ', ');
    END IF;

    -- Новые ключи (подсчет до изменения таргета) Только новые, не обновляемые
    -- Поиск в подзапросе ключей которые совпадают у целевой и у дельты и их удаление.
    EXECUTE 'SELECT count(*) FROM ' || p_delta || ' s WHERE NOT EXISTS (SELECT 1 FROM ' ||
            p_target || ' t WHERE ' || v_join || ')' INTO o_inserted;

    -- Ключ есть, но значения изменились. Запись количества в переменную 
    EXECUTE 'SELECT count(*) FROM ' || p_delta || ' s JOIN ' || p_target ||
            ' t ON ' || v_join || ' WHERE ' || v_diff INTO o_updated;

    RAISE NOTICE '[%] в дельте: новых = %, изменённых = %', p_target, o_inserted, o_updated;

    -- Выборка строк из целевой у которых совпадает PK с дельтой и есть изменения в неключевых столбцах
    IF o_updated > 0 THEN
        EXECUTE 'DELETE FROM ' || p_target || ' t USING ' || p_delta || ' s WHERE ' ||
                v_join || ' AND (' || v_diff || ')';  
        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        IF v_deleted <> o_updated THEN
            RAISE EXCEPTION 'Удалено % строк, ожидалось % (дубли ключа в таргете?)', v_deleted, o_updated;
        END IF;
    END IF;

    -- Загрузка всего, чего теперь нет в таргете: новые + новые версии изменённых.
    -- Неизменившиеся строки находятся по ключу и пропускаются.
    IF o_inserted + o_updated > 0 THEN
        EXECUTE 'INSERT INTO ' || p_target || ' (' || v_all_cols || ') SELECT ' || v_s_cols ||
                ' FROM ' || p_delta || ' s WHERE NOT EXISTS (SELECT 1 FROM ' ||
                p_target || ' t WHERE ' || v_join || ')';
        GET DIAGNOSTICS v_cnt = ROW_COUNT;
        IF v_cnt <> o_inserted + o_updated THEN
            RAISE EXCEPTION 'Вставлено % строк, ожидалось %', v_cnt, o_inserted + o_updated;
        END IF;
    END IF;

END;

$$
EXECUTE ON ANY;
