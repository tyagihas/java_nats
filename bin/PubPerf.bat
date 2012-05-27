SET PATH="C:\Program Files\Java\jre6\bin"
REM SET PATH="C:\Program Files\Java\jdk1.7.0_04\bin"
SET CLASSPATH=.

REM java -ms128m -mx128m nats.benchmark.PubPerf
java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -ms512m -mx512m nats.benchmark.PubPerf 500000 16
REM java -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -ms128m -mx128m nats.benchmark.PubPerf

REM pause