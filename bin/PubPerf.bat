SET CLASSPATH=.

java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -ms512m -mx512m org.nats.benchmark.PubPerf 100000 1024

pause

