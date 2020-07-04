-- Loads the data and defines the trips identifier
run 'load_trips.pig'

origin_dest = FOREACH trips GENERATE 
    comunasubida, comunabajada;

trips_grouped_by_origin = GROUP origin_dest BY comunasubida;

trips_by_origin_municipality = FOREACH trips_grouped_by_origin GENERATE
    $0 AS comunasubida, COUNT($1) AS num_trips;

trips_grouped_by_destination = GROUP origin_dest BY comunabajada;

trips_by_destination_municipality = FOREACH trips_grouped_by_destination GENERATE
    $0 AS comunabajada, COUNT(*) AS num_trips;
