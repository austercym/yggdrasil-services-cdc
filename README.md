# yggdrasil-party-create
create-party topology that writes to mariadb.

Typical scenario:
- Receive request in "subscriber.topic" kafka topic defined in topology.properties.
- Write element in mariadb.
- Return response in  "publisher.topic" kafka topic defined in topology.properties.


## Configuration:

topology.properties:
- "zookeeper.host" (set by maven profiles, see "Build").
- Topic names (for request, response, error).
- Optional "zookeeper.topology.config.subbranch" with default value = "/yggdrasil/topologies-defaults".

Topology configuration params read from zookeeper under znode as defined in "zookeeper.topology.config.subbranch" in topology.properties.

Kafka bootstrap servers param read from zookeeper. Eg configuration in zookeeper (OVH integration environment):

```sh
create /com/orwellg/yggdrasil/topologies-defaults "topologies default params"
create /com/orwellg/yggdrasil/topologies-defaults/kafka-bootstrap-host hdf-node1:6667,hdf-node4:6667,hdf-node5:6667
```

Mariadb connection params read from zookeeper. Eg configuration in zookeeper (OVH integration environment):

```sh
create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.host 217.182.88.190
create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.port 3306
create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.dbname IPAGOO_Customer_DB_TEST
create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user root
create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password passwd
```


## Build

Unit tests will start a zookeeper instance on port 6969. Integration tests will start spotify/kafka docker on ports 2181 (zookeeper) and 9042 (kafka).

```sh
mvn clean install (for "development" environment and unit tests)
mvn clean verify -P integration-test (for integration tests)
mvn clean install -P integration (for "integration" environment, eg: to be deployed in OVH test; does not execute tests)
```

## Run manually in local cluster

```sh
java -cp target/yggdrasil-party-create-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.orwellg.yggdrasil.party.create.topology.CreatePartyTopology local
```

## Upload to storm cluster in OVH test environment:

Upload generated jar-with-dependencies.


## Deploy in OVH test storm cluster:


```sh
storm jar <name>-jar-with-dependencies.jar com.orwellg.yggdrasil.party.create.topology.CreatePartyTopology -c nimbus.host=hdf-node2
```

## Manually load CSV and sent to topology (send requests to kafka topic and wait response):

With a previously deployed topology:

```sh
java -cp <name>-jar-with-dependencies.jar com.orwellg.yggdrasil.party.create.csv.PartyCsvLoader <kafka-bootstrap-server>:9092
```
