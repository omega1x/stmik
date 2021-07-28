# Build

First, download the standalone dependency - *libwebsocket-1.1.jar* - from [release page](https://github.com/omega1x/stmik/releases/tag/0.1.1):

```shell
wget -P /tmp https://github.com/omega1x/stmik/releases/download/0.1.1/libwebsocket-1.1.jar
```

Then install *libwebsocket-1.1.jar* to local [maven](https://maven.apache.org/)-repository:

```shell
mvn install:install-file -Dfile=/tmp/libwebsocket-1.1.jar -DgroupId=fr.bmartel -DartifactId=libwebsocket -Dversion=1.1 -Dpackaging=jar -DgeneratePom=true
```

