-- Pig script for loading the table


-- Change to this line to use only the fist 100k rows
-- trips_tmp = LOAD 'hdfs://cm:9000/uhadoop2020/g7/viajes201908_laboral_100k.csv'

-- Load table from hadoop dfs, ignoring headers
trips_tmp = LOAD 'hdfs://cm:9000/uhadoop2020/g7/viajes201908_laboral.csv.gz'
using org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
AS (
    netapa					:int,
    etapas					:chararray,
    netapassinbajada		:int,
    ultimaetapaconbajada	:int,
    tviaje_seg				:int,
    tviaje_min				:float,
    dviajeeuclidiana_mts	:float,
    dviajeenruta_mts		:int,
    paraderosubida			:chararray,
    paraderobajada			:chararray,
    comunasubida			:chararray,
    comunabajada			:chararray,
    diseno777subida			:int,
    diseno777bajada			:int,
    tiemposubida			:chararray,
    tiempobajada			:chararray,
    periodosubida			:chararray,
    periodobajada			:chararray,
    tipodia					:chararray,
    mediahora				:chararray,
    contrato				:chararray,
    factorexpansion			:float,
    tiempomediodeviaje		:chararray,
    periodomediodeviaje		:chararray,
    mediahoramediodeviaje	:chararray,
    tipodiamediodeviaje		:chararray,
    t_1era_etapa			:float,
    d_1era_etapa			:int,
    tespera_1era_etapa		:float,
    ttrasbordo_1era_etapa	:float,
    tcaminata_1era_etapa	:float,
    dtransbordo_1era_etapa	:float,
    t_2da_etapa				:float,
    d_2da_etapa				:int,
    tespera_2da_etapa		:float,
    ttrasbordo_2da_etapa	:float,
    tcaminata_2da_etapa		:float,
    dtransbordo_2da_etapa	:float,
    t_3era_etapa			:float,
    d_3era_etapa			:int,
    tespera_3era_etapa		:float,
    ttrasbordo_3era_etapa	:float,
    tcaminata_3era_etapa	:float,
    dtransbordo_3era_etapa	:float,
    t_4ta_etapa				:float,
    d_4ta_etapa				:int,
    tespera_4ta_etapa		:float,
    ttrasbordo_4ta_etapa	:float,
    tcaminata_4ta_etapa		:float,
    dtransbordo_4ta_etapa	:float,
    op_1era_etapa			:int,
    op_2da_etapa			:int,
    op_3era_etapa			:int,
    op_4ta_etapa			:int,
    tipoop_1era_etapa		:chararray,
    tipoop_2da_etapa		:chararray,
    tipoop_3era_etapa		:chararray,
    tipoop_4ta_etapa		:chararray,
    serv_1era_etapa			:chararray,
    serv_2da_etapa			:chararray,
    serv_3era_etapa			:chararray,
    serv_4ta_etapa			:chararray,
    linea_metro_subida_1	:chararray,
    linea_metro_subida_2	:chararray,
    linea_metro_subida_3	:chararray,
    linea_metro_subida_4	:chararray,
    linea_metro_bajada_1	:chararray,
    linea_metro_bajada_2	:chararray,
    linea_metro_bajada_3	:chararray,
    linea_metro_bajada_4	:chararray,
    paraderosubida_1era		:chararray,
    paraderosubida_2da		:chararray,
    paraderosubida_3era		:chararray,
    paraderosubida_4ta		:chararray,
    tiemposubida_1era		:chararray,
    tiemposubida_2da		:chararray,
    tiemposubida_3era		:chararray,
    tiemposubida_4ta		:chararray,
    zona777subida_1era		:int,
    zona777subida_2da		:int,
    zona777subida_3era		:int,
    zona777subida_4ta		:int,
    paraderobajada_1era		:chararray,
    paraderobajada_2da		:chararray,
    paraderobajada_3era		:chararray,
    paraderobajada_4ta		:chararray,
    tiempobajada_1era		:chararray,
    tiempobajada_2da		:chararray,
    tiempobajada_3era		:chararray,
    tiempobajada_4ta		:chararray,
    zona777bajada_1era		:int,
    zona777bajada_2da		:int,
    zona777bajada_3era		:int,
    zona777bajada_4ta		:int,
    tipotransporte_1era		:chararray,
    tipotransporte_2da		:chararray,
    tipotransporte_3era		:chararray,
    tipotransporte_4ta		:chararray,
    tespera_1era			:float,
    tespera_2da				:float,
    tespera_3era			:float,
    tespera_4ta				:float,
    escolar					:boolean,
    tviaje_en_vehiculo_min	:float,
    tipo_corte_etapa_viaje	:chararray,
    proposito				:chararray,
    dviajeenbus				:int
    );

-- Convert fields that represente datetime to datetime. This is necessary because the load function
-- doesn't understand the date format. Also '-' fields are replaced by null, because otherwise 
-- ToDate throws an error.

trips = FOREACH trips_tmp GENERATE 
    netapa AS netapa,
    (etapas == '-' ? NULL : etapas) AS etapas,
    netapassinbajada AS netapassinbajada,
    ultimaetapaconbajada AS ultimaetapaconbajada,
    tviaje_seg AS tviaje_seg,
    tviaje_min AS tviaje_min,
    dviajeeuclidiana_mts AS dviajeeuclidiana_mts,
    dviajeenruta_mts AS dviajeenruta_mts,
    (paraderosubida == '-' ? NULL : paraderosubida) AS paraderosubida,
    (paraderobajada == '-' ? NULL : paraderobajada) AS paraderobajada,
    (comunasubida == '-' ? NULL : comunasubida) AS comunasubida,
    (comunabajada == '-' ? NULL : comunabajada) AS comunabajada,
    diseno777subida AS diseno777subida,
    diseno777bajada AS diseno777bajada,
    (tiemposubida == '-' ? NULL : ToDate(tiemposubida, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida,
    (tiempobajada == '-' ? NULL : ToDate(tiempobajada, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada,
    (periodosubida == '-' ? NULL : periodosubida) AS periodosubida,
    (periodobajada == '-' ? NULL : periodobajada) AS periodobajada,
    (tipodia == '-' ? NULL : tipodia) AS tipodia,
    (mediahora == '-' ? NULL : mediahora) AS mediahora,
    (contrato == '-' ? NULL : contrato) AS contrato,
    factorexpansion AS factorexpansion,
    (tiempomediodeviaje == '-' ? NULL : ToDate(tiempomediodeviaje, 'yyyy-MM-dd HH:mm:ss')) AS tiempomediodeviaje,
    (periodomediodeviaje == '-' ? NULL : periodomediodeviaje) AS periodomediodeviaje,
    (mediahoramediodeviaje == '-' ? NULL : mediahoramediodeviaje) AS mediahoramediodeviaje,
    (tipodiamediodeviaje == '-' ? NULL : tipodiamediodeviaje) AS tipodiamediodeviaje,
    t_1era_etapa AS t_1era_etapa,
    d_1era_etapa AS d_1era_etapa,
    tespera_1era_etapa AS tespera_1era_etapa,
    ttrasbordo_1era_etapa AS ttrasbordo_1era_etapa,
    tcaminata_1era_etapa AS tcaminata_1era_etapa,
    dtransbordo_1era_etapa AS dtransbordo_1era_etapa,
    t_2da_etapa AS t_2da_etapa,
    d_2da_etapa AS d_2da_etapa,
    tespera_2da_etapa AS tespera_2da_etapa,
    ttrasbordo_2da_etapa AS ttrasbordo_2da_etapa,
    tcaminata_2da_etapa AS tcaminata_2da_etapa,
    dtransbordo_2da_etapa AS dtransbordo_2da_etapa,
    t_3era_etapa AS t_3era_etapa,
    d_3era_etapa AS d_3era_etapa,
    tespera_3era_etapa AS tespera_3era_etapa,
    ttrasbordo_3era_etapa AS ttrasbordo_3era_etapa,
    tcaminata_3era_etapa AS tcaminata_3era_etapa,
    dtransbordo_3era_etapa AS dtransbordo_3era_etapa,
    t_4ta_etapa AS t_4ta_etapa,
    d_4ta_etapa AS d_4ta_etapa,
    tespera_4ta_etapa AS tespera_4ta_etapa,
    ttrasbordo_4ta_etapa AS ttrasbordo_4ta_etapa,
    tcaminata_4ta_etapa AS tcaminata_4ta_etapa,
    dtransbordo_4ta_etapa AS dtransbordo_4ta_etapa,
    op_1era_etapa AS op_1era_etapa,
    op_2da_etapa AS op_2da_etapa,
    op_3era_etapa AS op_3era_etapa,
    op_4ta_etapa AS op_4ta_etapa,
    (tipoop_1era_etapa == '-' ? NULL : tipoop_1era_etapa) AS tipoop_1era_etapa,
    (tipoop_2da_etapa == '-' ? NULL : tipoop_2da_etapa) AS tipoop_2da_etapa,
    (tipoop_3era_etapa == '-' ? NULL : tipoop_3era_etapa) AS tipoop_3era_etapa,
    (tipoop_4ta_etapa == '-' ? NULL : tipoop_4ta_etapa) AS tipoop_4ta_etapa,
    (serv_1era_etapa == '-' ? NULL : serv_1era_etapa) AS serv_1era_etapa,
    (serv_2da_etapa == '-' ? NULL : serv_2da_etapa) AS serv_2da_etapa,
    (serv_3era_etapa == '-' ? NULL : serv_3era_etapa) AS serv_3era_etapa,
    (serv_4ta_etapa == '-' ? NULL : serv_4ta_etapa) AS serv_4ta_etapa,
    (linea_metro_subida_1 == '-' ? NULL : linea_metro_subida_1) AS linea_metro_subida_1,
    (linea_metro_subida_2 == '-' ? NULL : linea_metro_subida_2) AS linea_metro_subida_2,
    (linea_metro_subida_3 == '-' ? NULL : linea_metro_subida_3) AS linea_metro_subida_3,
    (linea_metro_subida_4 == '-' ? NULL : linea_metro_subida_4) AS linea_metro_subida_4,
    (linea_metro_bajada_1 == '-' ? NULL : linea_metro_bajada_1) AS linea_metro_bajada_1,
    (linea_metro_bajada_2 == '-' ? NULL : linea_metro_bajada_2) AS linea_metro_bajada_2,
    (linea_metro_bajada_3 == '-' ? NULL : linea_metro_bajada_3) AS linea_metro_bajada_3,
    (linea_metro_bajada_4 == '-' ? NULL : linea_metro_bajada_4) AS linea_metro_bajada_4,
    (paraderosubida_1era == '-' ? NULL : paraderosubida_1era) AS paraderosubida_1era,
    (paraderosubida_2da == '-' ? NULL : paraderosubida_2da) AS paraderosubida_2da,
    (paraderosubida_3era == '-' ? NULL : paraderosubida_3era) AS paraderosubida_3era,
    (paraderosubida_4ta == '-' ? NULL : paraderosubida_4ta) AS paraderosubida_4ta,
    (tiemposubida_1era == '-' ? NULL : ToDate(tiemposubida_1era, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_1era,
    (tiemposubida_2da == '-' ? NULL : ToDate(tiemposubida_2da, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_2da,
    (tiemposubida_3era == '-' ? NULL : ToDate(tiemposubida_3era, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_3era,
    (tiemposubida_4ta == '-' ? NULL : ToDate(tiemposubida_4ta, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_4ta,
    zona777subida_1era AS zona777subida_1era,
    zona777subida_2da AS zona777subida_2da,
    zona777subida_3era AS zona777subida_3era,
    zona777subida_4ta AS zona777subida_4ta,
    (paraderobajada_1era == '-' ? NULL : paraderobajada_1era) AS paraderobajada_1era,
    (paraderobajada_2da == '-' ? NULL : paraderobajada_2da) AS paraderobajada_2da,
    (paraderobajada_3era == '-' ? NULL : paraderobajada_3era) AS paraderobajada_3era,
    (paraderobajada_4ta == '-' ? NULL : paraderobajada_4ta) AS paraderobajada_4ta,
    (tiempobajada_1era == '-' ? NULL : ToDate(tiempobajada_1era, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_1era,
    (tiempobajada_2da == '-' ? NULL : ToDate(tiempobajada_2da, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_2da,
    (tiempobajada_3era == '-' ? NULL : ToDate(tiempobajada_3era, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_3era,
    (tiempobajada_4ta == '-' ? NULL : ToDate(tiempobajada_4ta, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_4ta,
    zona777bajada_1era AS zona777bajada_1era,
    zona777bajada_2da AS zona777bajada_2da,
    zona777bajada_3era AS zona777bajada_3era,
    zona777bajada_4ta AS zona777bajada_4ta,
    (tipotransporte_1era == '-' ? NULL : tipotransporte_1era) AS tipotransporte_1era,
    (tipotransporte_2da == '-' ? NULL : tipotransporte_2da) AS tipotransporte_2da,
    (tipotransporte_3era == '-' ? NULL : tipotransporte_3era) AS tipotransporte_3era,
    (tipotransporte_4ta == '-' ? NULL : tipotransporte_4ta) AS tipotransporte_4ta,
    tespera_1era AS tespera_1era,
    tespera_2da AS tespera_2da,
    tespera_3era AS tespera_3era,
    tespera_4ta AS tespera_4ta,
    escolar AS escolar,
    tviaje_en_vehiculo_min AS tviaje_en_vehiculo_min,
    (tipo_corte_etapa_viaje == '-' ? NULL : tipo_corte_etapa_viaje) AS tipo_corte_etapa_viaje,
    (proposito == '-' ? NULL : proposito) AS proposito,
    dviajeenbus AS dviajeenbus;
-- Extends 'load_trips.pig'

-- Identify trips that correspond to commutes
-- The criteria used is trips that list purpose equal to "TRABAJO" 
-- (Other options are HOGAR, SINBAJADA, OTROS, MENOS1MINUTO).

commutes = FILTER trips BY proposito == 'TRABAJO';

morning_commutes = FILTER commutes BY GetHour(tiemposubida) <= 12;
evening_commutes = FILTER commutes BY GetHour(tiemposubida) > 12;

morning_commutes_proj = FOREACH morning_commutes 
    GENERATE 
    comunasubida,
    comunabajada,
    MinutesBetween(tiempobajada, tiemposubida)+60*HoursBetween(tiempobajada, tiemposubida) AS duracion;

evening_commutes_proj = FOREACH evening_commutes
    GENERATE
    comunasubida,
    comunabajada,
    MinutesBetween(tiempobajada, tiemposubida)+60*HoursBetween(tiempobajada, tiemposubida) AS duracion;

------------------------
-- Duration of commutes --
------------------------

-- Average duration of morning commutes from every municipality, in minutes
morning_commutes_grouped = GROUP morning_commutes_proj BY comunasubida;

average_morning_commute_length_per_municipality = FOREACH morning_commutes_grouped 
    GENERATE $0 AS origin, AVG(morning_commutes_proj.duracion) AS avg_duration;

-- Average duration of evening commutes to every municipality, in minutes

evening_commutes_grouped = GROUP evening_commutes_proj BY comunabajada;
average_evening_commute_length_per_municipality = FOREACH evening_commutes_grouped 
    GENERATE $0 AS destination, AVG(evening_commutes_proj.duracion) AS avg_duration;

-- Average duration of commutes for each municipality, in minutes
-- Assumening people commute from their municipality in the morning and to it in the evening.
a = FOREACH morning_commutes_proj
    GENERATE comunasubida AS municipality, duracion AS duration;

b = FOREACH evening_commutes_proj
    GENERATE comunabajada AS municipality, duracion AS duration;

c = UNION a, b;

c_grouped = GROUP c BY municipality;

average_commute_length_per_municipality = FOREACH c_grouped
    GENERATE $0 as municipality, AVG(c.duration) AS avg_duration;


-------------------------------------------
-- Morning commute start and evening end --
-------------------------------------------

morning_commutes_proj_2 = FOREACH morning_commutes 
    GENERATE 
    comunasubida,
    comunabajada,
    GetMinute(tiemposubida) AS minutosubida,
    GetHour(tiemposubida) AS horasubida;

morning_commutes_grouped_2 = GROUP morning_commutes_proj_2 BY comunasubida;

morning_commute_start_time = FOREACH morning_commutes_grouped_2
    GENERATE 
    $0 AS municipality,
    (int) (AVG(morning_commutes_proj_2.horasubida)*60+AVG(morning_commutes_proj_2.minutosubida))/60 AS start_hour,
    (int) (AVG(morning_commutes_proj_2.horasubida)*60+AVG(morning_commutes_proj_2.minutosubida))%60 AS start_minute;

evening_commutes_proj_2 = FOREACH evening_commutes 
    GENERATE 
    comunasubida,
    comunabajada,
    GetMinute(tiempobajada) AS minutobajada,
    GetHour(tiempobajada) AS horabajada;

evening_commutes_grouped_2 = GROUP evening_commutes_proj_2 BY comunabajada;

evening_commute_end_time = FOREACH evening_commutes_grouped_2
    GENERATE 
    $0 as municipality, 
    (int) (AVG(evening_commutes_proj_2.horabajada)*60+AVG(evening_commutes_proj_2.minutobajada))/60 AS end_hour,
    (int) (AVG(evening_commutes_proj_2.horabajada)*60+AVG(evening_commutes_proj_2.minutobajada))%60 AS end_minute;


-------------------
-- Store results --
-------------------

STORE average_morning_commute_length_per_municipality   INTO '/uhadoop2020/g7/results/average_morning_commute_length_per_municipality';
STORE average_evening_commute_length_per_municipality   INTO '/uhadoop2020/g7/results/average_evening_commute_length_per_municipality';
STORE average_commute_length_per_municipality           INTO '/uhadoop2020/g7/results/average_commute_length_per_municipality';
STORE morning_commute_start_time                        INTO '/uhadoop2020/g7/results/morning_commute_start_time';
STORE evening_commute_end_time                          INTO '/uhadoop2020/g7/results/evening_commute_end_time';
