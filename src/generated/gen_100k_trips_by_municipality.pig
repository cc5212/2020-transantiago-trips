-- Pig script for loading the table

-- Load table from hadoop dfs, ignoring headers
trips_tmp = LOAD 'hdfs://cm:9000/uhadoop2020/g7/viajes201908_laboral_100k.csv'
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
    (etapas == '-' ? NULL : TRIM(etapas)) AS etapas,
    netapassinbajada AS netapassinbajada,
    ultimaetapaconbajada AS ultimaetapaconbajada,
    tviaje_seg AS tviaje_seg,
    tviaje_min AS tviaje_min,
    dviajeeuclidiana_mts AS dviajeeuclidiana_mts,
    dviajeenruta_mts AS dviajeenruta_mts,
    (paraderosubida == '-' ? NULL : TRIM(paraderosubida)) AS paraderosubida,
    (paraderobajada == '-' ? NULL : TRIM(paraderobajada)) AS paraderobajada,
    (comunasubida == '-' ? NULL : TRIM(comunasubida)) AS comunasubida,
    (comunabajada == '-' ? NULL : TRIM(comunabajada)) AS comunabajada,
    diseno777subida AS diseno777subida,
    diseno777bajada AS diseno777bajada,
    (tiemposubida == '-' ? NULL : ToDate(tiemposubida, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida,
    (tiempobajada == '-' ? NULL : ToDate(tiempobajada, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada,
    (periodosubida == '-' ? NULL : TRIM(periodosubida)) AS periodosubida,
    (periodobajada == '-' ? NULL : TRIM(periodobajada)) AS periodobajada,
    (tipodia == '-' ? NULL : TRIM(tipodia)) AS tipodia,
    (mediahora == '-' ? NULL : TRIM(mediahora)) AS mediahora,
    (contrato == '-' ? NULL : TRIM(contrato)) AS contrato,
    factorexpansion AS factorexpansion,
    (tiempomediodeviaje == '-' ? NULL : ToDate(tiempomediodeviaje, 'yyyy-MM-dd HH:mm:ss')) AS tiempomediodeviaje,
    (periodomediodeviaje == '-' ? NULL : TRIM(periodomediodeviaje)) AS periodomediodeviaje,
    (mediahoramediodeviaje == '-' ? NULL : TRIM(mediahoramediodeviaje)) AS mediahoramediodeviaje,
    (tipodiamediodeviaje == '-' ? NULL : TRIM(tipodiamediodeviaje)) AS tipodiamediodeviaje,
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
    (tipoop_1era_etapa == '-' ? NULL : TRIM(tipoop_1era_etapa)) AS tipoop_1era_etapa,
    (tipoop_2da_etapa == '-' ? NULL : TRIM(tipoop_2da_etapa)) AS tipoop_2da_etapa,
    (tipoop_3era_etapa == '-' ? NULL : TRIM(tipoop_3era_etapa)) AS tipoop_3era_etapa,
    (tipoop_4ta_etapa == '-' ? NULL : TRIM(tipoop_4ta_etapa)) AS tipoop_4ta_etapa,
    (serv_1era_etapa == '-' ? NULL : TRIM(serv_1era_etapa)) AS serv_1era_etapa,
    (serv_2da_etapa == '-' ? NULL : TRIM(serv_2da_etapa)) AS serv_2da_etapa,
    (serv_3era_etapa == '-' ? NULL : TRIM(serv_3era_etapa)) AS serv_3era_etapa,
    (serv_4ta_etapa == '-' ? NULL : TRIM(serv_4ta_etapa)) AS serv_4ta_etapa,
    (linea_metro_subida_1 == '-' ? NULL : TRIM(linea_metro_subida_1)) AS linea_metro_subida_1,
    (linea_metro_subida_2 == '-' ? NULL : TRIM(linea_metro_subida_2)) AS linea_metro_subida_2,
    (linea_metro_subida_3 == '-' ? NULL : TRIM(linea_metro_subida_3)) AS linea_metro_subida_3,
    (linea_metro_subida_4 == '-' ? NULL : TRIM(linea_metro_subida_4)) AS linea_metro_subida_4,
    (linea_metro_bajada_1 == '-' ? NULL : TRIM(linea_metro_bajada_1)) AS linea_metro_bajada_1,
    (linea_metro_bajada_2 == '-' ? NULL : TRIM(linea_metro_bajada_2)) AS linea_metro_bajada_2,
    (linea_metro_bajada_3 == '-' ? NULL : TRIM(linea_metro_bajada_3)) AS linea_metro_bajada_3,
    (linea_metro_bajada_4 == '-' ? NULL : TRIM(linea_metro_bajada_4)) AS linea_metro_bajada_4,
    (paraderosubida_1era == '-' ? NULL : TRIM(paraderosubida_1era)) AS paraderosubida_1era,
    (paraderosubida_2da == '-' ? NULL : TRIM(paraderosubida_2da)) AS paraderosubida_2da,
    (paraderosubida_3era == '-' ? NULL : TRIM(paraderosubida_3era)) AS paraderosubida_3era,
    (paraderosubida_4ta == '-' ? NULL : TRIM(paraderosubida_4ta)) AS paraderosubida_4ta,
    (tiemposubida_1era == '-' ? NULL : ToDate(tiemposubida_1era, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_1era,
    (tiemposubida_2da == '-' ? NULL : ToDate(tiemposubida_2da, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_2da,
    (tiemposubida_3era == '-' ? NULL : ToDate(tiemposubida_3era, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_3era,
    (tiemposubida_4ta == '-' ? NULL : ToDate(tiemposubida_4ta, 'yyyy-MM-dd HH:mm:ss')) AS tiemposubida_4ta,
    zona777subida_1era AS zona777subida_1era,
    zona777subida_2da AS zona777subida_2da,
    zona777subida_3era AS zona777subida_3era,
    zona777subida_4ta AS zona777subida_4ta,
    (paraderobajada_1era == '-' ? NULL : TRIM(paraderobajada_1era)) AS paraderobajada_1era,
    (paraderobajada_2da == '-' ? NULL : TRIM(paraderobajada_2da)) AS paraderobajada_2da,
    (paraderobajada_3era == '-' ? NULL : TRIM(paraderobajada_3era)) AS paraderobajada_3era,
    (paraderobajada_4ta == '-' ? NULL : TRIM(paraderobajada_4ta)) AS paraderobajada_4ta,
    (tiempobajada_1era == '-' ? NULL : ToDate(tiempobajada_1era, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_1era,
    (tiempobajada_2da == '-' ? NULL : ToDate(tiempobajada_2da, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_2da,
    (tiempobajada_3era == '-' ? NULL : ToDate(tiempobajada_3era, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_3era,
    (tiempobajada_4ta == '-' ? NULL : ToDate(tiempobajada_4ta, 'yyyy-MM-dd HH:mm:ss')) AS tiempobajada_4ta,
    zona777bajada_1era AS zona777bajada_1era,
    zona777bajada_2da AS zona777bajada_2da,
    zona777bajada_3era AS zona777bajada_3era,
    zona777bajada_4ta AS zona777bajada_4ta,
    (tipotransporte_1era == '-' ? NULL : TRIM(tipotransporte_1era)) AS tipotransporte_1era,
    (tipotransporte_2da == '-' ? NULL : TRIM(tipotransporte_2da)) AS tipotransporte_2da,
    (tipotransporte_3era == '-' ? NULL : TRIM(tipotransporte_3era)) AS tipotransporte_3era,
    (tipotransporte_4ta == '-' ? NULL : TRIM(tipotransporte_4ta)) AS tipotransporte_4ta,
    tespera_1era AS tespera_1era,
    tespera_2da AS tespera_2da,
    tespera_3era AS tespera_3era,
    tespera_4ta AS tespera_4ta,
    escolar AS escolar,
    tviaje_en_vehiculo_min AS tviaje_en_vehiculo_min,
    (tipo_corte_etapa_viaje == '-' ? NULL : TRIM(tipo_corte_etapa_viaje)) AS tipo_corte_etapa_viaje,
    (proposito == '-' ? NULL : TRIM(proposito)) AS proposito,
    dviajeenbus AS dviajeenbus;
-- Loads the data and defines the trips identifier
-- Extends 'load_trips.pig'

origin_dest = FOREACH trips GENERATE 
    comunasubida, comunabajada;

-- Trips by origin municipality
trips_grouped_by_origin = GROUP origin_dest BY comunasubida;

trips_by_origin_municipality = FOREACH trips_grouped_by_origin GENERATE
    $0 AS comunasubida, COUNT($1) AS num_trips;


-- Trips by destination municipality
trips_grouped_by_destination = GROUP origin_dest BY comunabajada;

trips_by_destination_municipality = FOREACH trips_grouped_by_destination GENERATE
    $0 AS comunabajada, COUNT($1) AS num_trips;

STORE trips_by_origin_municipality INTO '/uhadoop2020/g7/results/trips_by_origin_municipality/';
STORE trips_by_destination_municipality INTO '/uhadoop2020/g7/results/trips_by_destination_municipality/';
