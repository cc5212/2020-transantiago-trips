-- Extends 'load_trips.pig'

-- Same as commutes.pig, except using intervals instead of average
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
    ((int) MinutesBetween(tiempobajada, tiemposubida)/10)*10 AS intervalo;

evening_commutes_proj = FOREACH evening_commutes
    GENERATE
    comunasubida,
    comunabajada,
    ((int) MinutesBetween(tiempobajada, tiemposubida)/10)*10 AS intervalo;


------------------------
-- Duration of commutes --
------------------------

-- Frequency of duration of morning commutes from every municipality, in 30 minute intervals
morning_commutes_grouped = GROUP morning_commutes_proj BY (comunasubida, intervalo);

morning_commute_length_per_municipality = FOREACH morning_commutes_grouped 
    GENERATE FLATTEN($0) AS (origin, interval), COUNT(morning_commutes_proj.intervalo) AS frequency;

morning_commute_length_per_municipality_sorted = ORDER morning_commute_length_per_municipality BY origin DESC, interval ASC;

evening_commutes_grouped = GROUP evening_commutes_proj BY (comunabajada, intervalo);

evening_commute_length_per_municipality = FOREACH evening_commutes_grouped 
    GENERATE FLATTEN($0) AS (destination, interval), COUNT(evening_commutes_proj.intervalo) AS frequency;

evening_commute_length_per_municipality_sorted = ORDER evening_commute_length_per_municipality BY destination DESC, interval ASC;

-- Frequency of duration of commutes for each municipality, in 30 minute intervals
-- Assuming people commute from their municipality in the morning and to it in the evening.
a = FOREACH morning_commutes_proj
    GENERATE comunasubida AS municipality, intervalo AS interval;

b = FOREACH evening_commutes_proj
    GENERATE comunabajada AS municipality, intervalo AS interval;

c = UNION a, b;

c_grouped = GROUP c BY (municipality, interval);

commute_length_per_municipality = FOREACH c_grouped
    GENERATE FLATTEN($0) AS (municipality, interval), COUNT(c.interval) AS frequency;

commute_length_per_municipality_sorted = ORDER commute_length_per_municipality BY municipality DESC, interval ASC;

-------------------------------------------
-- Morning commute start and evening end --
-------------------------------------------

-- Frequency of commute start and commute end for every municipality, in 1 hour intervals

morning_commutes_proj_2 = FOREACH morning_commutes 
    GENERATE 
    comunasubida,
    comunabajada,
    GetHour(tiemposubida) AS intervalo;

morning_commutes_grouped_2 = GROUP morning_commutes_proj_2 BY (comunasubida, intervalo);

morning_commute_start_time = FOREACH morning_commutes_grouped_2
    GENERATE 
    FLATTEN($0) AS (municipality, interval),
    COUNT(morning_commutes_proj_2.intervalo);

morning_commute_start_time_sorted = ORDER morning_commute_start_time BY municipality DESC, interval ASC;

evening_commutes_proj_2 = FOREACH evening_commutes 
    GENERATE 
    comunasubida,
    comunabajada,
    GetHour(tiempobajada) AS intervalo;

evening_commutes_grouped_2 = GROUP evening_commutes_proj_2 BY (comunabajada, intervalo);

evening_commute_end_time = FOREACH evening_commutes_grouped_2
    GENERATE 
    FLATTEN($0) AS (municipality, interval),
    COUNT(evening_commutes_proj_2.intervalo);

evening_commute_end_time_sorted = ORDER evening_commute_end_time BY municipality DESC, interval ASC;

-------------------
-- Store results --
-------------------

STORE morning_commute_length_per_municipality_sorted INTO '/uhadoop2020/g7/results/commute_intervals/morning_commute_length_per_municipality';
STORE evening_commute_length_per_municipality_sorted INTO '/uhadoop2020/g7/results/commute_intervals/evening_commute_length_per_municipality';
STORE commute_length_per_municipality_sorted         INTO '/uhadoop2020/g7/results/commute_intervals/commute_length_per_municipality';
STORE morning_commute_start_time_sorted              INTO '/uhadoop2020/g7/results/commute_intervals/morning_commute_start_time';
STORE evening_commute_end_time_sorted                INTO '/uhadoop2020/g7/results/commute_intervals/evening_commute_end_time';
