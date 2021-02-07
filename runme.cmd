set SPARK_HOME="C:\Users\joseluis\OneDrive\Aplicaciones\spark-3.0.1-hadoop-3.2"
set PYTHON_HOME="C:\Program Files\Python39"
set PATH="%PATH%;%SPARK_HOME%\bin;%PYTHON_HOME%"
rem set CLASSPATH="%CLASSPATH%"



HADOOP_HOME
PYSPARK_PYTHON
JDK_HOME


set MAIN=org.example.App
set JAR="C:\Users\joseluis\OneDrive\TFM\scala-spark-test\target\scala-spark-test-1.0-SNAPSHOT.jar"

echo 1
call %SPARK_HOME%\bin\find-spark-home.cmd

echo 2
call %SPARK_HOME%\bin\spark-submit2.cmd --class %MAIN% --master local[2] %JAR%
