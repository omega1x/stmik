
[![CodeFactor](https://www.codefactor.io/repository/github/omega1x/stmik/badge)](https://www.codefactor.io/repository/github/omega1x/stmik)



`stmik` - Industrial [web-socket](https://en.wikipedia.org/wiki/WebSocket) connector to *BTSK-telemetry service*.

Connect to *BTSK-telemetry service* through [web-socket](https://en.wikipedia.org/wiki/WebSocket) interface and store received [telemetry](https://en.wikipedia.org/wiki/Telemetry) data in [ClickHouse](https://clickhouse.tech/docs/en/) backend database.


# Installation
Somehow install *Java 1.11+ Runtime* so that

```sh
java --version
```

produces the output in the nature of 

```sh
openjdk 16.0.1 2021-04-20
OpenJDK Runtime Environment (build 16.0.1+9-Ubuntu-120.04)
OpenJDK 64-Bit Server VM (build 16.0.1+9-Ubuntu-120.04, mixed mode, sharing)
```

Download the latest release of the `stmik` *fat-jar*:

```sh
wget https://github.com/omega1x/stmik/releases/download/0.9.0/stmik.jar
``` 

> **NOTE!** In order to connect to *BTSK-telemetry service* you must have appropriate credentials: a [PKCS 12](https://en.wikipedia.org/wiki/PKCS_12) file and password.

# Usage

## Usage synopsis
Get usage synopsis:
```sh
java -jar ./stmik.jar --help

```

## Run as *ETL*-service
Start communicating with *BTSK-telemetry service* with local [ClickHouse](https://clickhouse.tech/docs/en/) backend:

```sh
java -jar ./stmik.jar --key-file=~/key.p12 --password=keypass
```
where `~/key.p12` is a [PKCS 12](https://en.wikipedia.org/wiki/PKCS_12) file and `keypass` is a password both provided to you by *BTSK-telemetry service* administration. 

Local [ClickHouse](https://clickhouse.tech/docs/en/) backend should provide the next connection details:

```yaml
Host    : 127.0.0.1
Port    : 9000
Database: default
User    : default
Password: default
```
It is equivalent to usage with the next *jdbc-connection string*:

```sh
java -jar ./target/stmik.jar --key-file=~/key.p12 --password=keypass "jdbc:clickhouse://127.0.0.1:9000/default?user=default&password=default"
```

You may specify another connection details by providing your own *jdbc-connection string*.

## Monitor
Monitor `stmik` status (Linux/WSL):
```sh
tail -f stmik.log
```

## Stop
Press **Ctrl+C** in terminal to invoke `System.exit()` method. 



# Build

First, download the standalone dependency - *libwebsocket-1.1.jar* - from [release page](https://github.com/omega1x/stmik/releases/tag/0.1.1):

```sh
wget -P /tmp https://github.com/omega1x/stmik/releases/download/0.1.1/libwebsocket-1.1.jar
```

Then install *libwebsocket-1.1.jar* to local [maven](https://maven.apache.org/)-repository:

```sh
mvn install:install-file -Dfile=/tmp/libwebsocket-1.1.jar -DgroupId=fr.bmartel -DartifactId=libwebsocket -Dversion=1.1 -Dpackaging=jar -DgeneratePom=true
```

Clone the repository:
```sh
git clone https://github.com/omega1x/stmik.git
```

Build *fat-jar*:

```sh
cd ./stmik
mvn package -f pom.xml
```
