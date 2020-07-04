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
