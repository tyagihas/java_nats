SET PATH="C:\Program Files\Java\jdk1.7.0_04\bin"
SET CLASSPATH=.

java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -ms512m -mx512m nats.benchmark.PubPerf 100000 1024

pause