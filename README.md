# 2020-transantiago-trips
Analysis of trips in transantiago with Apache Pig and Hadoop. [Joaquín Diaz, Valentina Ramos, José Luis Romero. Grupo 7]

# Overview

<!-- State what is the main goal of the project. State what sorts of question(s) you want to answer or what sort of system you want to build. (Questions may be non-technical -- e.g., is there a global correlation between coffee consumption and research output -- so long as they require data analysis or other technical solutions.) -->

<!-- Esto es temporal, la idea es cambiarlo despues según que consultas hagamos -->
The goal of the project is to gather insights from the Transantiago trip data. We would like to answer questions like: Which buses are the most packed? At what time do people start their commute to work in each borough? How long on average are trips starting from each borough? Where are most jobs concentrated?

# Data

<!-- Describe the raw dataset that you considered for your project. Where did it come from? Why was it chosen? What information does it contain? What format was it in? What size was it? How many lines/records? Provide links. -->

We're using the trip data from [this dataset](http://www.dtpm.cl/index.php/documentos/matrices-de-viaje) in the DTPM (Departamento de Transporte Público Metropolitano) website, the public transport department for the Metropolitan Region in Chile. We chose this dataset because we would like to know how people use this conveyance, and because its a good start point to use for the future, to compare (when there is data aviable) how the mobility of people was changed at the social outburst and the coronavirus pandemic.\
The dataset contains information of individual trips in transantiago, each pair of file contains the data of a week, separated in weekdays and weekend. The data for each week is about 24 million rows long, structured in a single table with about 100 columns in csv format using semicolons as separators. Uncompressed the data for each week is about 10GB in size. Given the space constraints of sharing a cluster with many groups we will be working only with the data from a few <!-- maybe only one --> weeks.

# Methods

<!-- Detail the methods used during the project. Provide an overview of the techniques/technologies used, why you used them and how you used them. Refer to the source-code delivered with the project. Describe any problems you encountered. -->

The main technologies we used were Apache Hadoop and Apache Pig. We chose these technologies, because they allowed us to process the large ammount of data relatively easily and in a relatively small ammount of time. The data was loaded on the Hadoop Distributed File System and then was processed using various Apache Pig scripts.

# Results

<!-- Detail the results of the project. Different projects will have different types of results; e.g., run-times or result sizes, evaluation of the methods you're comparing, the interface of the system you've built, and/or some of the results of the data analysis you conducted. -->


# Conclusion

<!-- Summarise main lessons learnt. What was easy? What was difficult? What could have been done better or more efficiently? -->


# Appendix

<!-- You can use this for key code snippets that you don't want to clutter the main text. -->
