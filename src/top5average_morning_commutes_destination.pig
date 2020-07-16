-- Extends 'load_trips.pig'

-- Identify trips that correspond to commutes
-- The criteria used is trips that list purpose equal to "TRABAJO" 
-- (Other options are HOGAR, SINBAJADA, OTROS, MENOS1MINUTO).

commutes = FILTER trips BY proposito == 'TRABAJO';

morning_commutes = FILTER commutes BY GetHour(tiemposubida) <= 9 AND GetHour(tiemposubida) >= 5;

morning_sanbernardo= FILTER commutes BY comunasubida == 'SAN BERNARDO';
morning_lapintana= FILTER commutes BY comunasubida == 'LA PINTANA';
morning_puentealto= FILTER commutes BY comunasubida == 'PUENTE ALTO';
morning_elbosque= FILTER commutes BY comunasubida == 'EL BOSQUE';
morning_quilicura= FILTER commutes BY comunasubida == 'QUILICURA';





morning_sanbernardo_proj =  FOREACH morning_sanbernardo
GENERATE
comunabajada;
morning_lapintana_proj =  FOREACH morning_lapintana
GENERATE
comunabajada;
morning_puentealto_proj =  FOREACH morning_puentealto
GENERATE
comunabajada;
morning_elbosque_proj =  FOREACH morning_elbosque
GENERATE
comunabajada;
morning_quilicura_proj =  FOREACH morning_quilicura
GENERATE
comunabajada;




------------------------
-- Duration of commutes --
------------------------

-- Average duration of morning commutes from every municipality, in minutes
morning_sanbernardo_grouped = GROUP morning_sanbernardo_proj BY comunabajada;
morning_lapintana_grouped = GROUP morning_lapintana_proj BY comunabajada;
morning_puentealto_grouped = GROUP morning_puentealto_proj BY comunabajada;
morning_elbosque_grouped = GROUP morning_elbosque_proj BY comunabajada;
morning_quilicura_grouped = GROUP morning_quilicura_proj BY comunabajada;




morning_sanbernardo_count = FOREACH morning_sanbernardo_grouped 
    GENERATE FLATTEN(group) as Destino,COUNT($1) as Total;
morning_lapintana_count = FOREACH morning_lapintana_grouped 
    GENERATE FLATTEN(group) as Destino,COUNT($1) as Total;
    morning_puentealto_count = FOREACH morning_puentealto_grouped 
    GENERATE FLATTEN(group) as Destino,COUNT($1) as Total;
    morning_elbosque_count = FOREACH morning_elbosque_grouped 
    GENERATE FLATTEN(group) as Destino,COUNT($1) as Total;
    morning_quilicura_count = FOREACH morning_quilicura_grouped 
    GENERATE FLATTEN(group) as Destino,COUNT($1) as Total;

sanbernardo = ORDER morning_sanbernardo_count BY Total DESC;
lapintana = ORDER morning_lapintana_count BY Total DESC;
puentealto = ORDER morning_puentealto_count BY Total DESC;
elbosque = ORDER morning_elbosque_count BY Total DESC;
quilicura = ORDER morning_quilicura_count BY Total DESC;



STORE sanbernardo  INTO '/uhadoop2020/g7/results/viajescomunas/sanbernardo';
STORE lapintana  INTO '/uhadoop2020/g7/results/viajescomunas/lapintana';
STORE puentealto  INTO '/uhadoop2020/g7/results/viajescomunas/puentealto';
STORE elbosque  INTO '/uhadoop2020/g7/results/viajescomunas/elbosque';
STORE quilicura  INTO '/uhadoop2020/g7/results/viajescomunas/quilicura';
