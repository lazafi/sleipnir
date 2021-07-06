# usefull commands

## debug
~/app/spark-3.1.1-bin-hadoop2.7/bin/spark-submit --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777" target/sleipnir.jar 
