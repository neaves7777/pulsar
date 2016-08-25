cd `dirname $0`
/usr/java/jre1.7.0_71/bin/java -Djava.security.egd=file:/dev/./urandom -classpath pulsar.jar:ojdbc6.jar pulsar.pulsar ${@}
