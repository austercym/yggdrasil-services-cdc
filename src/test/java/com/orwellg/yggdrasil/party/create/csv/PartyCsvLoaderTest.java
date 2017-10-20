package com.orwellg.yggdrasil.party.create.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;

public class PartyCsvLoaderTest {

	public final static Logger LOG = LogManager.getLogger(PartyCsvLoaderTest.class);

	protected Gson gson = new Gson();
	
	@Before
	public void setUp() throws Exception {
//		man = MariaDbManager.getInstance("db-local.yaml");
	}
	
	@Test
	public void testPartyPersonalCsvLoad() throws Exception {
		// Given CSV file
		// and kafka+zookeeper mocked?
		// When load CSV
		// and generate events
		// Then PartyType objects populated
		// and requests (events) created: "CreateParty" Event with data = Party
		// and send to topic "party.action.event.1"?

		// Given CSV file with Parties
		InputStream csvStream = PartyCsvLoaderTest.class.getClassLoader().getResourceAsStream(PartyCsvLoader.CSV_PARTY_PERSONAL_FILENAME);
		Reader in = new InputStreamReader(csvStream);

		// Load CSV
		PartyCsvLoader partyCsvLoader = new PartyCsvLoader();
		List<PartyType> parties = partyCsvLoader.loadCsvParty(in);

		// Generate events
		String parentKey = "PartyCsvLoaderTest";
		String eventName = PartyEvents.CREATE_PARTY.getEventName();
		List<Event> events = partyCsvLoader.generateEventList(parentKey, eventName, parties);

		// Then Party objects populated
		Assert.assertEquals(99, parties.size());
		Assert.assertEquals("Butt", parties.get(0).getLastName());
		Assert.assertEquals("jbutt@gmail.com", parties.get(0).getIDList().getIDList().getEmail());
		Assert.assertEquals("Darakjy", parties.get(1).getLastName());
		Assert.assertEquals(Boolean.TRUE, parties.get(0).getPersonalDetails().getStaffIndicator());
		for (Iterator<PartyType> iterator = parties.iterator(); iterator.hasNext();) {
			PartyType partyType = (PartyType) iterator.next();
			Assert.assertNotNull(String.format("should have personalDetails: %s", partyType), partyType.getPersonalDetails());
			Assert.assertNotNull(String.format("should have ID_List: %s", partyType), partyType.getIDList());
			LOG.info("Loaded PartyType = {}", partyType);
		}

		// Then requests (events) generated
		Assert.assertEquals(99, events.size());
		{
			Event event1 = events.get(0);
			String eventDataStr = event1.getEvent().getData().toString();
//			PartyType partyType = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
			PartyType partyType = gson.fromJson(eventDataStr, PartyType.class);
			Assert.assertEquals("James", partyType.getFirstName());
		}
		{
			Event event = events.get(98);
			String eventDataStr = event.getEvent().getData().toString();
//			PartyType partyType = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
			PartyType partyType = gson.fromJson(eventDataStr, PartyType.class);
			Assert.assertEquals("Lisha", partyType.getFirstName());
		}
	}

	@Test
	public void testPartyNonPersonalCsvLoad() throws Exception {
		// Given CSV file
		// and kafka+zookeeper mocked?
		// When load CSV
		// and generate events
		// Then PartyType objects populated
		// and requests (events) created: "CreateParty" Event with data = Party
		// and send to topic "party.action.event.1"?

		// Given CSV file with Parties
		InputStream csvStream = PartyCsvLoaderTest.class.getClassLoader().getResourceAsStream(PartyCsvLoader.CSV_PARTY_NONPERSONAL_FILENAME);
		Reader in = new InputStreamReader(csvStream);

		// Load CSV
		PartyCsvLoader partyCsvLoader = new PartyCsvLoader();
		List<PartyType> parties = partyCsvLoader.loadCsvParty(in);

		// Generate events
		String parentKey = "PartyCsvLoaderTest";
		String eventName = PartyEvents.CREATE_PARTY.getEventName();
		List<Event> events = partyCsvLoader.generateEventList(parentKey, eventName, parties);

		// Then Party objects populated
		int expectedNum = 102;
		Assert.assertEquals(expectedNum, parties.size());
		Assert.assertEquals("Tortor Dictum LLP", parties.get(0).getBussinesName());
		Assert.assertEquals("1120", parties.get(0).getNonPersonalDetails().getSICCodes().getActivityCodes().get(0).getCode());
		Assert.assertEquals("Growing of rice", parties.get(0).getNonPersonalDetails().getSICCodes().getActivityCodes().get(0).getDescription());
		Assert.assertEquals("Elementum Foundation", parties.get(1).getBussinesName());
		Assert.assertEquals("1140", parties.get(1).getNonPersonalDetails().getSICCodes().getActivityCodes().get(1).getCode());
		Assert.assertEquals("Growing of sugar cane", parties.get(1).getNonPersonalDetails().getSICCodes().getActivityCodes().get(1).getDescription());
		for (Iterator<PartyType> iterator = parties.iterator(); iterator.hasNext();) {
			PartyType partyType = (PartyType) iterator.next();
			Assert.assertNotNull(String.format("should have nonPersonalDetails: %s", partyType), partyType.getNonPersonalDetails());
			Assert.assertNotNull(String.format("should have ID_List: %s", partyType), partyType.getIDList());
			LOG.info("Loaded PartyType = {}", partyType);
		}

		// Then requests (events) generated
		Assert.assertEquals(expectedNum, events.size());
		{
			Event event1 = events.get(0);
			String eventDataStr = event1.getEvent().getData().toString();
//			PartyType partyType = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
			PartyType partyType = gson.fromJson(eventDataStr, PartyType.class);
			Assert.assertEquals("Tortor Dictum LLP", partyType.getBussinesName());
		}
		{
			Event event = events.get(99);
			String eventDataStr = event.getEvent().getData().toString();
//			PartyType partyType = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
			PartyType partyType = gson.fromJson(eventDataStr, PartyType.class);
			Assert.assertEquals("Ullamcorper Eu Inc.", partyType.getBussinesName());
		}
	}
	
//	@Test
//	public void testCsvToKafkaParty() throws Exception {
//		SubscriberKafkaConfiguration configuration = SubscriberKafkaConfiguration
//				.loadConfiguration(PartyTopology.SUBSCRIBER_LOCAL_YAML);
//		String bootstrapServer = configuration.getBootstrap().getHost();
//		PartyCsvLoader partyCsvLoader = new PartyCsvLoader(MariaDbManager.getInstance().getConnection());
//		partyCsvLoader.csvToKafkaParty(PartyCsvLoader.CSV_PARTY_FILENAME, bootstrapServer);
//	}

}
