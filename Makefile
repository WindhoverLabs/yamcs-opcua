build:
	mvn clean install

dev-build:
	mvn clean install -Dfmt.skip -DskipTests

check-format:
	mvn com.spotify.fmt:fmt-maven-plugin:check

format:
	mvn com.coveo:fmt-maven-plugin:format

generate-coverage-reports: build
	mvn  org.jacoco:jacoco-maven-plugin:report

bundle-deps:
	mvn -DskipTests install dependency:copy-dependencies


install-opc-ua-stack:
	mvn install:install-file \
   	-Dfile=lib/opc-ua-stack/opc-ua-stack-1.4.1.1-SNAPSHOT.jar \
   	-DgroupId=org.opcfoundation.ua \
   	-DartifactId=opc-ua-stack \
   	-Dversion=1.4.1.1-SNAPSHOT \
   	-Dpackaging=jar \
   	-DgeneratePom=true

