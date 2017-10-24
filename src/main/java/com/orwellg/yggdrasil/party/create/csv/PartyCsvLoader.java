package com.orwellg.yggdrasil.party.create.csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.commons.AddressType;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.party.ActivityCodeList;
import com.orwellg.umbrella.avro.types.party.ID_List_Record;
import com.orwellg.umbrella.avro.types.party.PartyChannelType;
import com.orwellg.umbrella.avro.types.party.PartyContractType;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyNotificationPreferencesType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyQuestionaireType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;

public class PartyCsvLoader {

	public static final String CSV_PARTY_PERSONAL_FILENAME = "party-personal.csv";
	public static final String CSV_PARTY_NONPERSONAL_FILENAME = "party-nonpersonal.csv";
//	public static final String CSV_PARTY_FILENAME = "PartyCsvLoaderTest-party.csv";

	public final static Logger LOG = LogManager.getLogger(PartyCsvLoader.class);

	protected Gson gson = new Gson();
	
	public PartyCsvLoader() {
	}
	
	public List<PartyType> loadCsvParty(Reader in) throws IOException {
		// Reader in = new FileReader("path/to/file.csv");
		Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(in);
		List<PartyType> l = new ArrayList<>();
		for (CSVRecord record : records) {
			// Party_ID,FirstName,LastName,BusinessName,TradingName,Title,Salutation,OtherNames,ID_List,Addresses,Party_InfoAssets
			PartyType p = new PartyType();
			p.setId(new PartyIdType(record.get("Party_ID")));
			p.setFirstName(record.get("FirstName"));
			p.setLastName(record.get("LastName"));
			p.setBussinesName(record.get("BusinessName"));
			p.setTradingName(record.get("TradingName"));
			p.setTitle(record.get("Title"));
			p.setSalutation(record.get("Salutation"));
			p.setOtherNames(record.get("OtherNames"));
			p.setIDList(gson.fromJson(record.get("ID_List"), ID_List_Record.class));
			p.setAddresses(new ArrayList<AddressType>());
//			TODO p.setAddresses(record.get("Addresses"));
			// ?
//			TODO p.set?(record.get("Party_InfoAssets"));
			p.setChannels(new ArrayList<PartyChannelType>());
			p.setContracts(new ArrayList<PartyContractType>());
			p.setQuestionaires(new ArrayList<PartyQuestionaireType>());
			p.setNotifications(new ArrayList<PartyNotificationPreferencesType>());
			
			// PersonalDetails
			if (record.isMapped("EmploymentDetails")) {
				// EmploymentDetails Citizenships	TaxResidency	Email	Telephone	StaffIndicator	Staff	Gender	DateOfBirth	Nationality	Tags
				PartyPersonalDetailsType det = new PartyPersonalDetailsType();
				det.setEmploymentDetails(record.get("EmploymentDetails"));
				det.setCitizenships(record.get("Citizenships"));
				det.setTaxResidency(record.get("TaxResidency"));
				det.setEmail(record.get("Email"));
//				det.setTelephone(value);
				String staffIndicator = record.get("StaffIndicator");
				det.setStaffIndicator(("VERDADERO".equalsIgnoreCase(staffIndicator) || "TRUE".equals(staffIndicator) ? true : false));
				det.setStaff(record.get("Staff"));
				det.setGender(record.get("Gender"));
//				det.setDateOfBirth(value);
//				det.setNationality
//				det.setTags
				// XXX ...
				
				p.setPersonalDetails(det);
			}

			// NonPersonalDetails
			if (record.isMapped("SICCodes")) {
				// NPP_ID	SICCodes	BusinessRegistrationCountry	BusinessRegistrationDate	TradingStartDate	IncorporationDetail	OtherAddresses	Website	NPPContactDetails	StockMarket	Turnover
				PartyNonPersonalDetailsType det = new PartyNonPersonalDetailsType();
				det.setSICCodes(gson.fromJson(record.get("SICCodes"), ActivityCodeList.class));
				det.setBusinessRegistrationCountry(record.get("BusinessRegistrationCountry"));
				// XXX ...
				
				p.setNonPersonalDetails(det);
			}
			
			l.add(p);
		}
		LOG.info("{} records loaded from csv.", l.size());
		return l;
	}
	
	public void sendToKafkaParty(String bootstrapServer, List<Event> events) {
		String topic = TopologyConfigWithLdapFactory.getTopologyConfig().getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
			Event event = iterator.next();
			String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
			
			// Write CreateParty event to "party.action.event.1" kafka topic.
			LOG.debug("Sending event {} to topic {}...", event, topic);
			producer.send(new ProducerRecord<String, String>(topic, base64Event));
			LOG.debug("Event {} sent to topic {}.", event, topic);
		}
		LOG.info("{} events sent to topic {}.", events.size(), topic);
		producer.close();
	}

	public Producer<String, String> makeProducer(String bootstrapServer) {
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

	public KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
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
	
	/**
	 * 
	 * @param partyCsvFilename
	 * @param bootstrapServer
	 * @return List of events sent
	 * @throws IOException
	 */
	public List<Event> csvToKafkaParty(String partyCsvFilename, String bootstrapServer) throws IOException {
		// CSV file with Parties
		InputStream csvStream = PartyCsvLoader.class.getClassLoader().getResourceAsStream(partyCsvFilename);
		if (csvStream == null) {
			csvStream = new FileInputStream(partyCsvFilename);
		}
		Reader in = new InputStreamReader(csvStream);
		
		// Load CSV
		List<PartyType> parties = loadCsvParty(in);

		// Generate events
		String parentKey = "PartyCsvLoader";
		String eventName = PartyEvents.CREATE_PARTY.getEventName();
		List<Event> events = generateEventList(parentKey, eventName, parties);
		
		// Send events to kafka topic "party.action.event.1"
		sendToKafkaParty(bootstrapServer, events);
		
		return events;
	}

	/**
	 * Load party hierarchy and related entities from CSV, send to topologies, wait responses.
	 * @param args
	 */
	public static void main(String[] args) {
		String bootstrapServer;
		if (args.length >= 1) {
			bootstrapServer = args[0];
			LOG.info("bootstrapServer argument received = {}", bootstrapServer);
		} else {
			bootstrapServer = "localhost:9092";
			LOG.info("You could pass bootstrapServer argument.");
			LOG.info("Using bootstrapServer = {}", bootstrapServer);
		}
		
		try {
			// Get (and init if they weren't before) party-storm application-wide params. Tries to connect to zookeeper:
			TopologyConfig config = TopologyConfigWithLdapFactory.getTopologyConfig();

			PartyCsvLoader pcl = new PartyCsvLoader();

			int maxRetries = 120;
			int interval = 1000;
			
			KafkaConsumer<String, String> consumer = pcl.makeConsumer(bootstrapServer);
			String partyResponseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);

			/////////////
			// PERSONAL PARTY
			{
				List<Event> events = pcl.csvToKafkaParty(CSV_PARTY_PERSONAL_FILENAME, bootstrapServer);
				// Wait for result responses to all events
				// Check if PartyTopology returns kafka PartyCreated result event
				LOG.info("{} PersonalParty events sent to topology. Checking response to all events...", events.size());
				consumer.subscribe(Arrays.asList(partyResponseTopic));
				pcl.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, PartyEvents.CREATE_PARTY_COMPLETE.getEventName());
			}

			/////////////
			// NONPERSONAL PARTY
			{
				List<Event> events = pcl.csvToKafkaParty(CSV_PARTY_NONPERSONAL_FILENAME, bootstrapServer);
				// Wait for result responses to all events
				// Check if PartyTopology returns kafka PartyCreated result event
				LOG.info("{} NonPersonalParty events sent to topology. Checking response to all events...", events.size());
				consumer.subscribe(Arrays.asList(partyResponseTopic));
				pcl.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, PartyEvents.CREATE_PARTY_COMPLETE.getEventName());
			}

//			/////////////
//			// IPR
//			String iprResponseTopic = "ipr.create.result.1";
//			{
//				// Load csv and send events to topic
//				IPRCsvLoader iprCsvLoader = new IPRCsvLoader();
//				String topic = "ipr.create.event.1";
//				List<Event> events = iprCsvLoader.csvToKafka(IPRCsvLoader.CSV_IPR_FILENAME, bootstrapServer, topic);
//				// Wait for result responses to all events
//				// Check if topology returns kafka created result event
//				LOG.info("{} IPR events sent to topology. Checking response to all events...", events.size());
//				consumer.subscribe(Arrays.asList(iprResponseTopic));
//				pcl.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, PartyEvents.CREATE_IPR_COMPLETE.getEventName());
//			}
//			
//			/////////////
//			// CONTRACT
//			String contractResponseTopic = "contract.create.result.1";
//			{
//				// Load csv and send events to topic
//				ContractCsvLoader iprCsvLoader = new ContractCsvLoader();
//				List<Event> events = iprCsvLoader.csvToKafkaContract(ContractCsvLoader.CSV_CONTRACT_FILENAME, 
//						bootstrapServer);
//				// Wait for result responses to all events
//				// Check if topology returns kafka created result event
//				LOG.info("{} Contract events sent to topology. Checking response to all events...", events.size());
//				consumer.subscribe(Arrays.asList(contractResponseTopic));
//				pcl.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, ContractEvents.CREATE_CONTRACT_COMPLETE.getEventName());
//			}
//
//			/////////////
//			// PARTYCONTRACT
//			String partyContractResponseTopic = "partycontract.create.result.1";
//			{
//				// Load csv and send events to topic
//				PartyContractCsvLoader pcCsvLoader = new PartyContractCsvLoader();
//				List<Event> events = pcCsvLoader.csvToKafkaPartyContract(PartyContractCsvLoader.CSV_PARTYCONTRACT_FILENAME, 
//						bootstrapServer);
//				// Wait for result responses to all events
//				// Check if topology returns kafka created result event
//				LOG.info("{} Contract events sent to topology. Checking response to all events...", events.size());
//				consumer.subscribe(Arrays.asList(partyContractResponseTopic));
//				pcl.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, ContractEvents.CREATE_PARTYCONTRACT_COMPLETE.getEventName());
//			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public List<Event> generateEventList(String parentKey, String eventName, List<PartyType> partiesNP) {
		// Generate CreateParty event with Party as eventData
		// Send Party as eventData
		// PartyType partyType = PartyType.newBuilder().build();
		List<Event> events = new ArrayList<>();
		for (Iterator<PartyType> iterator = partiesNP.iterator(); iterator.hasNext();) {
			PartyType pt = iterator.next();
//			String serializedPartyType = RawMessageUtils.encodeToString(pt.getSchema(), pt);
			String serializedPartyType = gson.toJson(pt);
			String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
			String uuid = UUID.randomUUID().toString();
			String eventKeyId = "EVENT-" + uuid;
			LOG.debug("eventKeyId = {}", eventKeyId);
			Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedPartyType);
			events.add(event);
		}
		LOG.info("{} events generated with eventName = {}.", events.size(), eventName);
		return events;
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
	public Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

		String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);

		LOG.debug("{} Generating event {} for PartyTopology", logPreffix, eventName);

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

		LOG.debug("{}Event generated correctly: {}", logPreffix, event);

		return event;
	}

	/**
	 * 
	 * @param originEvents
	 * @param maxRetries
	 * @param interval
	 * @param consumer
	 * @return Map (parentKey, responseEvent)
	 */
	public Map<String, Event> waitAndConsumeAllResponses(List<Event> originEvents, int maxRetries, int interval,
			KafkaConsumer<String, String> consumer, String eventToExpect) {
		
		// parentKeys are originEvents keys
		Set<String> parentKeys = new HashSet<>();
		for (Iterator<Event> iterator = originEvents.iterator(); iterator.hasNext();) {
			Event originEvent = (Event) iterator.next();
			parentKeys.add(originEvent.getEvent().getKey());
		}
		
		// parentKey, event
		Map<String, Event> parentKeyAndResponseEvents = new HashMap<>();
		
		// Consume responses until all matched or maxRetries
		int retries = 0;
		int matchedResponses = 0;
		while (matchedResponses < parentKeys.size() && retries < maxRetries) {
			LOG.info("consumer polling topic {}...", consumer.subscription());
			ConsumerRecords<String, String> records = consumer.poll(100);
			if (records.isEmpty()) {
				retries++;
				LOG.info("empty records polled, retry {}...", retries);
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (ConsumerRecord<String, String> record : records) {
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent data with parentKey {} found = {}", responseEvent.getEvent().getParentKey(), responseEvent);
				// Check if eventKeyId of the request matches the parent of the response
				if (parentKeys.contains(responseEvent.getEvent().getParentKey()) && responseEvent.getEvent().getName().equals(eventToExpect)) {
					parentKeyAndResponseEvents.put(responseEvent.getEvent().getParentKey(), responseEvent);
					LOG.info("responseEvent data with parentKey {} matched.", responseEvent.getEvent().getParentKey());
					matchedResponses++;
				} else {
					LOG.info("responseEvent data with parentKey {} NOT matched = {}.", responseEvent.getEvent().getParentKey(), responseEvent);
				}
			}
		}
		
		return parentKeyAndResponseEvents;
	}
}
