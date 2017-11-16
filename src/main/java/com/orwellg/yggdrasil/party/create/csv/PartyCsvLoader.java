package com.orwellg.yggdrasil.party.create.csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.commons.AddressType;
import com.orwellg.umbrella.avro.types.commons.CitizenshipsList;
import com.orwellg.umbrella.avro.types.commons.TaxResidencyList;
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
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetails;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.utils.cli.CommandLineParserCsvLoader;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;
import com.orwellg.yggdrasil.party.create.topology.CreatePartyRequestSender;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;

public class PartyCsvLoader {

	public static final String BOOTSTRAPSERVER = "localhost:9092";
	public static final String CSV_PARTY_PERSONAL_FILENAME = "party-personal.csv";
	public static final String CSV_PARTY_NONPERSONAL_FILENAME = "party-nonpersonal.csv";
//	public static final String CSV_PARTY_FILENAME = "PartyCsvLoaderTest-party.csv";

	public final static Logger LOG = LogManager.getLogger(PartyCsvLoader.class);

	protected Gson gson = new Gson();
	protected CreatePartyRequestSender requestSender;

	protected String csvFilename;
	protected String bootstrapServer;
	protected String eventToExpect;

	public PartyCsvLoader() {
	}
	
	public PartyCsvLoader(String csvFilename, String bootstrapserver, String eventToExpect) {
		this.csvFilename = csvFilename;
		this.bootstrapServer = bootstrapserver;
		this.eventToExpect = eventToExpect;
	}

	/**
	 * Load party hierarchy and related entities from CSV, send to topologies, wait responses.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		PartyCsvLoader csvLoader = new PartyCsvLoader();

		CommandLineParserCsvLoader cli = new CommandLineParserCsvLoader();
		cli.parseCommandLineArguments(args);
		csvLoader.configFromCommandLineArguments(cli);
		
		if (cli.getCmd().hasOption("help")) {
			return;
		}

		csvLoader.loadCsvAndSendToKafka();
	}

	public void loadCsvAndSendToKafka() throws IOException, SQLException {
		TopologyConfig config = TopologyConfigWithLdapFactory.getTopologyConfig();
		requestSender = new CreatePartyRequestSender(MariaDbManager.getInstance().getConnection());

		int maxRetries = 120;
		int interval = 1000;
		
		KafkaConsumer<String, String> consumer = requestSender.makeConsumer(bootstrapServer);
		String partyResponseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);

		consumer.subscribe(Arrays.asList(partyResponseTopic));
		
		if (csvFilename == null) {

			/////////////
			// PERSONAL PARTY
			{
				List<Event> events = csvToKafkaParty(CSV_PARTY_PERSONAL_FILENAME, bootstrapServer);
				// Wait until topology finish process of all events
				LOG.info("{} PersonalParty events sent to topology. Checking processing of all events...", events.size());
				if (eventToExpect != null) {
					Map<String, Event> responses = requestSender.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, eventToExpect);
					LOG.info("PersonalParty work finished; {} elements processed by topology.", responses.size()); 
				} else {
					LOG.info("eventToExpect null, so will not check topology responses, only DB insertions.");
					List<PartyType> processedElements = requestSender.waitAndGetInDbAllElements(events, maxRetries, interval);
					LOG.info("PersonalParty work finished; {} elements processed by topology.", processedElements.size()); 
				}
			}

			/////////////
			// NONPERSONAL PARTY
			{
				List<Event> events = csvToKafkaParty(CSV_PARTY_NONPERSONAL_FILENAME, bootstrapServer);
				// Wait until topology finish process all events
				LOG.info("{} NonPersonalParty events sent to topology. Checking response to all events...", events.size());
				if (eventToExpect != null) {
					Map<String, Event> responses = requestSender.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, eventToExpect);
					LOG.info("NonPersonalParty work finished; {} elements processed by topology.", responses.size()); 
				} else {
					LOG.info("eventToExpect null, so will not check topology responses, only DB insertions.");
					List<PartyType> processedElements = requestSender.waitAndGetInDbAllElements(events, maxRetries, interval);
					LOG.info("NonPersonalParty work finished; {} elements processed by topology.", processedElements.size()); 
				}
			}

		} else {
			
			List<Event> events = csvToKafkaParty(csvFilename, bootstrapServer);
			// Wait until topology finish process all events
			LOG.info("{} Party events sent to topology from csvFilename = {}. Checking response to all events...",
					events.size(), csvFilename);
			if (eventToExpect != null) {
				Map<String, Event> responses = requestSender.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, eventToExpect);
				LOG.info("PartyCsvLoader work finished; {} elements processed by topology.", responses.size()); 
			} else {
				LOG.info("eventToExpect null, so will not check topology responses, only DB insertions.");
				List<PartyType> processedElements = requestSender.waitAndGetInDbAllElements(events, maxRetries, interval);
				LOG.info("PartyCsvLoader work finished; {} elements processed by topology.", processedElements.size()); 
			}
			
		}
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
				det.setEmploymentDetails(gson.fromJson(record.get("EmploymentDetails"), PPEmploymentDetails.class));
				det.setCitizenships(gson.fromJson(record.get("Citizenships"), CitizenshipsList.class));
				det.setTaxResidency(gson.fromJson(record.get("TaxResidency"), TaxResidencyList.class));
				det.setEmail(record.get("Email"));
//				det.setTelephone(value);
				String staffIndicator = record.get("StaffIndicator");
				det.setStaffIndicator(("VERDADERO".equalsIgnoreCase(staffIndicator) || "TRUE".equals(staffIndicator) ? true : false));
//				TODO fix csv and then det.setStaff(gson.fromJson(record.get("Staff"), PPStaffType.class));
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
	
	/**
	 * 
	 * @param partyCsvFilename
	 * @param bootstrapServer
	 * @return List of events sent
	 * @throws IOException
	 * @throws SQLException 
	 */
	public List<Event> csvToKafkaParty(String partyCsvFilename, String bootstrapServer) throws IOException, SQLException {
		LOG.info("Loading csvFilename = {}", partyCsvFilename);

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
		requestSender.sendToKafka(bootstrapServer, events);
		
		return events;
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
	 * Configure this object from command-line arguments
	 */
	protected void configFromCommandLineArguments(CommandLineParserCsvLoader cli) {
		CommandLine cmd = cli.getCmd();

		if (cmd.hasOption(cli.getBootstrapserverOpt().getLongOpt())) {
			bootstrapServer = cmd.getOptionValue(cli.getBootstrapserverOpt().getLongOpt());
			LOG.info("bootstrapServer argument received = {}", bootstrapServer);
		} else {
			bootstrapServer = BOOTSTRAPSERVER;
			LOG.info("You could pass -b <bootstrapserver> argument.");
			LOG.info("Using default bootstrapServer = {}", bootstrapServer);
		}

		if (cmd.hasOption(cli.getCsvFileOpt().getLongOpt())) {
			csvFilename = cmd.getOptionValue(cli.getCsvFileOpt().getLongOpt());
			LOG.info("csvFilename argument received = {}", csvFilename);
		} else {
			LOG.info("You could pass -c <csvFilename> argument.");
			csvFilename = null;
			LOG.info("Using csvFilename = {}", csvFilename);
		}

		if (cmd.hasOption("n")) {
			LOG.info("\"-n\" argument received, not checking if response event is correct or incorrect.");
			eventToExpect = null;
		} else {
			LOG.info("You could pass \"-n\" as second argument not to check if response event is correct or incorrect.");
			eventToExpect = PartyEvents.CREATE_PARTY_COMPLETE.getEventName();
		}
	}
}
