-- Pig script for loading the table


-- Change to this line to use only the fist 100k rows
-- trips = LOAD 'hdfs://cm:9000/uhadoop2020/g7/viajes201908_laboral_100k.csv'USING PigStorage(';') AS

-- Load table from hadoop dfs, ignoring headers
trips = LOAD 'hdfs://cm:9000/uhadoop2020/g7/viajes201908_laboral.csv.gz'
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
