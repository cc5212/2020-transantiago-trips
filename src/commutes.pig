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
