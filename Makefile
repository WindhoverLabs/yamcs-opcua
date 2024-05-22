build:
	mvn clean install
format:
	mvn com.coveo:fmt-maven-plugin:format

install-opc-ua-stack:
	mvn install:install-file \
   	-Dfile=lib/opc-ua-stack/opc-ua-stack-1.4.1-224.jar \
   	-DgroupId=org.opcfoundation.ua \
   	-DartifactId=opc-ua-stack \
   	-Dversion=1.4.1.1-SNAPSHOT \
   	-Dpackaging=jar \
   	-DgeneratePom=true
