# 2020-transantiago-trips
Analysis of trips in transantiago with Apache Pig and Hadoop. [Joaquín Diaz, Valentina Ramos, José Luis Romero. Grupo 7]

# Overview

<!-- State what is the main goal of the project. State what sorts of question(s) you want to answer or what sort of system you want to build. (Questions may be non-technical -- e.g., is there a global correlation between coffee consumption and research output -- so long as they require data analysis or other technical solutions.) -->

<!-- Esto es temporal, la idea es cambiarlo despues según que consultas hagamos -->
The goal of the project is to gather insights from the Transantiago trip data. We would like to answer questions like: Which buses are the most packed? At what time do people start their commute to work in each borough? How long on average are trips starting from each borough? Where are most jobs concentrated?

# Data

<!-- Describe the raw dataset that you considered for your project. Where did it come from? Why was it chosen? What information does it contain? What format was it in? What size was it? How many lines/records? Provide links. -->

We're using the trip data from [this dataset](http://www.dtpm.cl/index.php/documentos/matrices-de-viaje) in the DTPM (Departamento de Transporte Público Metropolitano) website, the public transport department for the Metropolitan Region in Chile. We chose this dataset because we would like to know how people use this conveyance, and because its a good start point to use for the future, to compare (when there is data aviable) how the usage has changed after the social outburst and the coronavirus pandemic.\
The dataset contains information of individual trips in transantiago, each pair of file contains the data of a week, separated in weekdays and weekend. The data for each week is about 24 million rows long, structured in a single table with about 100 columns in csv format using semicolons as separators. Uncompressed the data for each week is about 10GB in size. Given the space constraints of sharing a cluster with many groups we will be working only with the data from a few <!-- maybe only one --> weeks.

# Methods

<!-- Detail the methods used during the project. Provide an overview of the techniques/technologies used, why you used them and how you used them. Refer to the source-code delivered with the project. Describe any problems you encountered. -->

The main technologies we used were Apache Hadoop and Apache Pig. We chose these technologies, because they allowed us to process the large ammount of data relatively easily and in a relatively small ammount of time. The data was loaded on the Hadoop Distributed File System and then was processed using various Apache Pig scripts.

# Results

<!-- Detail the results of the project. Different projects will have different types of results; e.g., run-times or result sizes, evaluation of the methods you're comparing, the interface of the system you've built, and/or some of the results of the data analysis you conducted. -->

Para el análisis de los datos, se tomaron en cuenta las comunas en las cuales las peronas que ahí residen invierten más tiempo en transportarse. Además se realizó un análisis del tiempo invertido en transportarse a los lugares de trabajo de las personas, tomando en cuenta que los horarios más comunes para viajar previo al inicio de la jornada laboral corresponden a horas entre las 5 y 9 am. De la misma forma se trabajó con los horarios de transporte posteriores a la jornada laboral, es decir, el tiempo que invierten las personas para transportarse de vuelta a sus hogares después del trabajo, este horario correspondería a las horas que se encuentran entre las 4 y 8 pm.

Los resultados relevantes son los siguientes:

- Existe una gran diferencia en los tiempos de translado hacia el lugar de trabajo de las personas que residen en las comunas de Providencia, Ñuñoa, Santiago y Las Condes, siendo estos tiempos mucho menores a los del resto de las comunas. Se llega a la conclusión de que en estas comunas se concentra la mayor cantidad de lugares de trabajo de Santiago, por lo que si una persona reside en estas comunas, lo más probable es que también trabaje dentro de estas y su traslado sea menor. Esto coincide con los datos de un estudio realizado con CEPCHILE el 2019 (https://www.cepchile.cl/cep/site/artic/20180405/asocfile/20180405120239/dpp_029_abril2018_srazmilic.pdf)

- Existe una gran cantidad de personas que residen en comunas periféricas, las cuales registran un mayor tiempo de traslado hacia sus lugares de trabajo. Este factor tiene relación directa con el valor de suelo de estas comunas. Según estudios de CEPCHILE, el menor valor de suelo se encuentra en las comunas más alejadas del centro de Santiago, lo que ha provocado una masiva construcción de viviendas sociales alejadas de los lugares donde se concentra la mayor oferta laboral.

- Los trabajos más probables de las personas que residen en las comunas mencionadas en el punto anterior corresponden a:

	- Oficiales y operarios de la construcción
	- Personal que realiza labores domésticas y afines.
	- Personal que trabaja en lavanderías y tintorerías.
	- Mensajeros, porteros y afines.

Personas que realizan estos trabajos coinciden con las personas que residen en comunas con mayor índice de prioridad social (http://www.desarrollosocialyfamilia.gob.cl/storage/docs/INDICE._DE_PRIORIDAD_SOCIAL_2019.pdf)

- Se puede concluir con todo lo mencionado anteriormente, que las personas que invierten más tiempo trasldándose a sus trabajos, coincide con las personas que suelen tener menores ingresos y por lo tanto mayor índice de prioridad social. Esto por no tener los recursos suficientes para residir en una comuna con alta concentración de oferta laboral o en una cercana a estas.

# Conclusion

<!-- Summarise main lessons learnt. What was easy? What was difficult? What could have been done better or more efficiently? -->


# Appendix

<!-- You can use this for key code snippets that you don't want to clutter the main text. -->
