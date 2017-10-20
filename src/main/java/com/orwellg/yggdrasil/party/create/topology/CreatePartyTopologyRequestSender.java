package com.orwellg.yggdrasil.party.create.topology;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.accounting.TransactionLog;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.TransactionEvents;

public class CreatePartyTopologyRequestSender {

    public final static Logger LOG = LogManager.getLogger(CreatePartyTopologyRequestSender.class);

    protected Gson gson = new Gson();
    
	public static void main(String[] args) {
		String accountId = null;
		if (args.length >= 1) {
			accountId = args[0];
			LOG.info("accountId parameter received = {}", accountId);
		}
		
		// Send request to "lasttransactionlog.get.event.1" topic and 
		// wait response in "lasttransactionlog.get.result.1" topic
		CreatePartyTopologyRequestSender test = new CreatePartyTopologyRequestSender();
		LOG.info("Requesting balance (last transactionlog) for account = {} to topology...", accountId);
		double balance = test.requestGetLastTransactionLogAndWaitResponse(accountId);
		// Print result
		LOG.info("Retrieved balance = {}", balance);
	}


	public double requestGetLastTransactionLogAndWaitResponse(String accountId) {
		if (accountId == null) {
			accountId = "1234IPAGO";
		}

//		SubscriberKafkaConfiguration configuration = SubscriberKafkaConfiguration
//				.loadConfiguration("get-lasttransactionlog-subscriber.yaml");
		String bootstrapServer = TopologyConfigFactory.getTopologyConfig().getKafkaBootstrapHosts();
		KafkaConsumer<String, String> consumer = makeConsumer(bootstrapServer);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		//
		// Generate event
		String parentKey = "GetLastTransactionlogTopologyTest";
		String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
		String uuid = UUID.randomUUID().toString();
		String eventKeyId = "EVENT-" + uuid;
		LOG.info("eventKeyId = {}", eventKeyId);
		String eventName = TransactionEvents.GET_LASTTRANSACTIONLOG.getEventName();
		// Send Id as eventData
		String serializedAccountId = accountId;
		Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedAccountId);
		// Serialize event
		String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
		
		//
		// Write event to kafka topic.
		String topic = TopologyConfigFactory.getTopologyConfig().getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		LOG.info("Sending event {} to topic {}...", event, topic);
		producer.send(new ProducerRecord<String, String>(topic, base64Event));
		LOG.info("Event {} sent to topic {}.", event, topic);
		
		//
		// Wait results
		int maxRetries = 120;
		int retries = 0;
		int interval = 1;
		// Check if topology returns kafka result event
		LOG.info("Checking if topology has returned to kafka topic...");
//		PublisherKafkaConfiguration topoPublisherConfig = PublisherKafkaConfiguration
//				.loadConfiguration("get-lasttransactionlog-publisher-result.yaml");
		String responseTopic = TopologyConfigFactory.getTopologyConfig().getKafkaPublisherBoltConfig().getTopic().getName().get(0);
		consumer.subscribe(Arrays.asList(responseTopic));
		TransactionLog txLog = waitAndConsume(eventKeyId, maxRetries, retries, interval, consumer);
		if (txLog != null) {
			LOG.info("CORRECT Response for eventKeyId = {} found, txLog = {}.", eventKeyId, txLog);
		} else {
			throw new RuntimeException("No response.");
		}
		consumer.close();
		return txLog.getActualBalance();
	}

	protected KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
		Properties propsC = new Properties();
		propsC.put("bootstrap.servers", bootstrapServer);
		propsC.put("acks", "all");
		propsC.put("retries", 0);
		propsC.put("batch.size", 16384);
		propsC.put("linger.ms", 1);
		propsC.put("buffer.memory", 33554432);
		propsC.put("group.id", "test");
		propsC.put("enable.auto.commit", "true");
		propsC.put("auto.commit.interval.ms", "1000");
		propsC.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsC);
		return consumer;
	}

	protected Producer<String, String> makeProducer(String bootstrapServer) {
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

	/**
	 * Based on KafkaEventGeneratorBolt.generateEvent(), modified for this test.
	 * 
	 * @param parentKey
	 * @param processId
	 * @param eventKeyId
	 * @param eventName
	 * @param data
	 * @return
	 */
	private Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

		String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);

		LOG.info("{} Generating event {} for topology", logPreffix, eventName);

		// Create the event type
		EventType eventType = new EventType();
		eventType.setName(eventName);
		eventType.setVersion(Constants.getDefaultEventVersion());
		eventType.setParentKey(parentKey);
		eventType.setKey(eventKeyId);
		eventType.setSource(this.getClass().getSimpleName());
		SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
		eventType.setTimestamp(format.format(new Date()));

		eventType.setData(serializedData);

		ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
		processIdentifier.setUuid(processId);

		EntityIdentifierType entityIdentifier = new EntityIdentifierType();
		entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
		entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

		// Create the corresponden event
		Event event = new Event();
		event.setEvent(eventType);
		event.setProcessIdentifier(processIdentifier);
		event.setEntityIdentifier(entityIdentifier);

		LOG.info("{}Event generated correctly: {}", logPreffix, event);

		return event;
	}

	protected TransactionLog waitAndConsume(String eventKeyId, int maxRetries, int retries,
			int interval, KafkaConsumer<String, String> consumer) {
		TransactionLog txLog = null;
		while (txLog == null && retries < maxRetries) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent = {}", responseEvent);
				String eventDataStr = responseEvent.getEvent().getData().toString();
				// Check if eventKeyId of the request matches the parent of the response
				if (eventKeyId.equals(responseEvent.getEvent().getParentKey().toString())) {
					txLog = gson.fromJson(eventDataStr, TransactionLog.class);
				}
			}
			if (txLog == null) {
				retries++;
				LOG.info(
						"Response for eventKeyId = {} not found yet, retry {} after sleeping {}s...",
						eventKeyId, retries, interval);
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return txLog;
	}
}
