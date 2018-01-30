package com.orwellg.yggdrasil.services.cdc.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.orwellg.umbrella.commons.repositories.scylla.ServicesByContractIdRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ServicesByContractIdRepositoryImpl;
import com.orwellg.umbrella.commons.types.scylla.entities.ServicesByContractId;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lable.oss.uniqueid.GeneratorException;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCServicesChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.ServicesRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ServicesRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.scylla.entities.Services;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;

import javax.print.DocFlavor;

public class CDCServicesRequestSender {

	protected ServicesRepository servicesDao;
	protected ServicesByContractIdRepository servicesByContractIdDao;
	protected UniqueIDGenerator idGen;
	protected Gson gson = new Gson();
    
    public final static Logger LOG = LogManager.getLogger(CDCServicesRequestSender.class);

    public CDCServicesRequestSender(Session ses) {
		servicesDao = new ServicesRepositoryImpl(ses);
		servicesByContractIdDao = new ServicesByContractIdRepositoryImpl(ses);
		idGen = new UniqueIDGeneratorLocal();
	}

	/**
	 * Test an already loaded topology by sending a Create event to kafka
	 * topic, and wait for Created response.<br/>
	 * Pre: topology already loaded in storm.
	 */
	public void requestManyCreateToTopologyAndWaitResponse(int numElements) throws Exception {
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();

		// Given producer
		String requestTopic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		// Generate ChangeRecords
		List<CDCServicesChangeRecord> events = generateChangeRecordElements(numElements);
		
		// Send ChangeRecords
		sendRequestEventsToKafka(events, producer, requestTopic);
		
		// Check all elements inserted in DB
		int maxRetries = 120;
		int interval = 1000;
		List<Services> processedElements = waitAndGetInDbAllElements(events, maxRetries, interval);
		if (events.size() != processedElements.size()) {
			throw new RuntimeException("All elements should be inserted in scylla DB.");
		}

		List<ServicesByContractId> processedServicesByContractId = waitAndGetInDbAllServicesByContractId(events, maxRetries, interval);
		if (events.size() != processedServicesByContractId.size()) {
			throw new RuntimeException("All ServicesByContractId should be inserted in scylla DB.");
		}
	}

	/**
	 * Test an already loaded topology by sending a Create event to kafka
	 * topic, and wait for Created response.<br/>
	 * Pre: topology already loaded in storm.
	 */
	public void requestToTopologyAndCauseError() throws Exception {
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();

		// Given producer
		String requestTopic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		// Send ChangeRecords
		sendRequestEventToKafkaToCauseError(producer, requestTopic);
	}

	public static void main(String[] args) throws Exception {
		ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
		CDCServicesRequestSender rs = new CDCServicesRequestSender(ScyllaManager.getInstance(scyllaParams.getNodeList()).getSession(scyllaParams.getKeyspace()));
		// Test cause error
		rs.requestToTopologyAndCauseError();
		// And correct events
		rs.requestManyCreateToTopologyAndWaitResponse(Integer.valueOf(args[0]));
	}

	private void sendRequestEventsToKafka(List<CDCServicesChangeRecord> events, Producer<String, String> producer, String topic) {
		for (Iterator<CDCServicesChangeRecord> iterator = events.iterator(); iterator.hasNext();) {
			CDCServicesChangeRecord changeRecord = iterator.next();
			String json = gson.toJson(changeRecord);

			// Write CreateServices event to kafka topic.
			LOG.debug("Sending changeRecord {} to topic {}...", json, topic);
			producer.send(new ProducerRecord<String, String>(topic, json));
			LOG.debug("ChangeRecord {} sent to topic {}.", json, topic);
		}
		LOG.info("{} events sent to topic {}.", events.size(), topic);
	}

	private void sendRequestEventToKafkaToCauseError(Producer<String, String> producer, String topic) {
		String json = "bad json to cause error";

		// Write event to kafka topic.
		LOG.debug("Sending changeRecord {} to topic {}...", json, topic);
		producer.send(new ProducerRecord<String, String>(topic, json));
		LOG.debug("ChangeRecord {} sent to topic {}.", json, topic);
		LOG.info("{} events sent to topic {}.", 1, topic);
	}
	
	protected List<CDCServicesChangeRecord> generateChangeRecordElements(int numElements) throws IOException, GeneratorException {
		ArrayList<CDCServicesChangeRecord> l = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			String id = idGen.generateLocalUniqueIDStr();
			String contractId = idGen.generateLocalUniqueIDStr();

			CDCServicesChangeRecord element = generateChangeRecordElement(i, id, contractId);
			
			l.add(element);
		}
		return l;
	}

	private CDCServicesChangeRecord generateChangeRecordElement(int eventNumber, String id, String contractId) {
		CDCServicesChangeRecord element = new CDCServicesChangeRecord();
		
		element.setSequence(eventNumber);
		element.setEventNumber(eventNumber);
		element.setTimestamp((int) System.currentTimeMillis());
		element.setEventType(EVENT_TYPES.insert);
		
		element.setServiceID(id);
		element.setContractID(contractId);
		return element;
	}

	private Producer<String, String> makeProducer(String bootstrapServer) {
		// Using kafka-clients library:
		// https://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
		Properties propsP = new Properties();
		propsP.put("bootstrap.servers", bootstrapServer);
		// props.put("bootstrap.servers", "172.31.17.121:6667");
		propsP.put("acks", "all");
		propsP.put("retries", 0);
		propsP.put("batch.size", 16384);
		propsP.put("linger.ms", 1);
		propsP.put("buffer.memory", 33554432);
		propsP.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsP.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(propsP);
		return producer;
	}

	protected Services waitAndGetById(int maxRetries, int interval, String elementID)
			throws SQLException {
		int retries;
		retries = 0;
		com.orwellg.umbrella.commons.types.scylla.entities.Services p = null;
		while (p == null && retries < maxRetries) {
			p = servicesDao.getService(elementID);

			if (p == null) {
				retries++;
				LOG.info("Element with Id = {} not found yet, retry {} after sleeping {}s...", elementID,
						retries, interval);
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return p;
	}

	protected List<Services> waitAndGetInDbAllElements(List<CDCServicesChangeRecord> originEvents, int maxRetries, int interval) throws SQLException {
		List<String> ids = new ArrayList<>();
		for (Iterator<CDCServicesChangeRecord> iterator = originEvents.iterator(); iterator.hasNext();) {
			CDCServicesChangeRecord originEvent = iterator.next();
			ids.add(originEvent.getServiceID());
		}
		
		// Get from db until all found or maxRetries
		List<Services> elementsFound = new ArrayList<>();
		for (Iterator<String> iterator = ids.iterator(); iterator.hasNext();) {
			String id = iterator.next();
			
			LOG.info("getting from DB id = {}, elementsFound = {} of {}...", id, elementsFound.size(), ids.size());
			Services e = waitAndGetById(maxRetries, interval, id);
			if (e != null) {
				elementsFound.add(e);
			}
		}
		
		return elementsFound;
	}

	protected ServicesByContractId waitAndGetServicesByContractId(int maxRetries, int interval, String contractId, String serviceId)
			throws SQLException {
		int retries;
		retries = 0;
		com.orwellg.umbrella.commons.types.scylla.entities.ServicesByContractId p = null;
		while (p == null && retries < maxRetries) {
			p = servicesByContractIdDao.getService(contractId, serviceId);

			if (p == null) {
				retries++;
				LOG.info("Element with Id = {} not found yet, retry {} after sleeping {}s...", contractId,
						retries, interval);
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return p;
	}

	protected List<ServicesByContractId> waitAndGetInDbAllServicesByContractId(List<CDCServicesChangeRecord> originEvents, int maxRetries, int interval) throws SQLException {
		List<ServicesByContractIdKey> ids = new ArrayList<>();
		for (Iterator<CDCServicesChangeRecord> iterator = originEvents.iterator(); iterator.hasNext();) {
			CDCServicesChangeRecord originEvent = iterator.next();
			ids.add(new ServicesByContractIdKey(originEvent.getContractID(), originEvent.getServiceID()));
		}

		// Get from db until all found or maxRetries
		List<ServicesByContractId> elementsFound = new ArrayList<>();
		for (Iterator<ServicesByContractIdKey> iterator = ids.iterator(); iterator.hasNext();) {
			ServicesByContractIdKey id = iterator.next();

			LOG.info("getting from DB id = {}, elementsFound = {} of {}...", id, elementsFound.size(), ids.size());
			ServicesByContractId e = waitAndGetServicesByContractId(maxRetries, interval, id.ContractId, id.ServiceId);
			if (e != null) {
				elementsFound.add(e);
			}
		}

		return elementsFound;
	}

	private class ServicesByContractIdKey
	{
		public String ContractId;
		public String ServiceId;

		public ServicesByContractIdKey(String contractId, String serviceId) {
			ContractId = contractId;
			ServiceId = serviceId;
		}
	}
}
