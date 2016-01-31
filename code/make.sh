javac -classpath commons-cli-1.2.jar:hadoop-core-1.2.1.jar *.java -d class/
jar cvf branchandbound.jar -C class/ .

