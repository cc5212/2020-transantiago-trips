#!/bin/bash
# Concatenates load_trips.pig and load_trips_100k.pig with all the .pig files
# that contain "-- Extends 'load_trips.pig'"
for f in *.pig
do
    if grep -q "\-\- Extends 'load_trips\.pig'" $f
    then
        cat load_trips.pig $f > generated/gen_$f
        cat load_trips_100k.pig $f > generated/gen_100k_$f
    fi
done
