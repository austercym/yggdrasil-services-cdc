# yggdrasil-contract-cdc
Topology that reads CDC events from topic and writes to scylla.

Typical scenario:
- Receive CDC event in "subscriber.topic" kafka topic defined in topology.properties.
- Write element in scylladb.
- Write response in "publisher.topic" kafka topic defined in topology.properties.


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

Scylla connection params read from zookeeper. Eg configuration in zookeeper (OVH integration environment):

```sh
create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list scylla-node1:9042,scylla-node2:9042,scylla-node3:9042
create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace.customer.product Customer_Product_DB
```


## Build

Unit tests will start a zookeeper instance on port 6969. Integration tests will start spotify/kafka docker on ports 2181 (zookeeper) and 9042 (kafka).

```sh
mvn clean install (for "development" environment and unit tests)
mvn clean verify -P integration-test (for integration tests)
mvn clean install -P integration (for "integration" environment, eg: to be deployed in OVH SID; does not execute tests)
```

## Run manually in local cluster

```sh
java -cp target/yggdrasil-contract-cdc-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.orwellg.yggdrasil.contract.cdc.topology.CDCContractTopology local
```

## Upload to storm cluster in OVH test environment:

Upload generated jar-with-dependencies.


## Deploy in OVH test storm cluster:


```sh
storm jar <name>-jar-with-dependencies.jar com.orwellg.yggdrasil.contract.cdc.topology.CDCContractTopology -c nimbus.host=hdf-node2
```


## Manually send requests to topology (for testing):

With a previously deployed topology, send one or many requests and wait responses:

```sh
java -cp <name>-jar-with-dependencies.jar com.orwellg.yggdrasil.contract.cdc.topology.CDCContractRequestSender 1
```

