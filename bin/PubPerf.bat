SET PATH="C:\Program Files\Java\jdk1.7.0_04\bin"
SET CLASSPATH=.

java -agentlib:hprof=heap=sites -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -ms512m -mx512m org.nats.benchmark.PubPerf 100000 1024

pause