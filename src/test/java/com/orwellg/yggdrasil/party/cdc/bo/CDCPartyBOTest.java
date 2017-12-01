package com.orwellg.yggdrasil.party.cdc.bo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.repositories.scylla.impl.PartyRepositoryImpl;
import com.orwellg.umbrella.commons.types.scylla.entities.Party;

public class CDCPartyBOTest {
	
	protected CDCPartyBO cdcPartyBo;
	
	@Mock
	protected PartyRepositoryImpl partyDao;
	
	@Mock
	protected Logger LOG;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	protected Gson gson = new Gson();

	public static final String INSERT_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
			+ "\"Party_ID\": \"1IPAGO\", \"FirstName\": \"James\", \"LastName\": \"Butt\", \"BusinessName\": \"\", \"TradingName\": \"\", "
			+ "\"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"Le\", \"ID_List\": \"{\\\"ID_List\\\":{\\\"Email\\\":\\\"jbutt@gmail.com\\\"}}\","
			+ " \"Addresses\": \"[]\", \"Party_InfoAssets\": \"\\u0000\"}";

	public static final String UPDATE_BEFORE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 1, \"timestamp\": 1511782041, \"event_type\": \"update_before\", "
			+ "\"Party_ID\": \"7893012032906067968IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"\", \"BusinessName\": \"\", "
			+ "\"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", \"Addresses\": \"null\", "
			+ "\"Party_InfoAssets\": \"\\u0000\"}";

	public static final String UPDATE_AFTER_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 2, \"timestamp\": 1511782041, \"event_type\": \"update_after\", "
			+ "\"Party_ID\": \"7893012032906067968IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"UpdatedLastNameForTest\", "
			+ "\"BusinessName\": \"\", \"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", "
			+ "\"Addresses\": \"null\", \"Party_InfoAssets\": \"\\u0000\"}";

	public static final String DELETE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 904, \"event_number\": 1, \"timestamp\": 1511782192, \"event_type\": \"delete\", "
			+ "\"Party_ID\": \"5944926038373957632IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"\", \"BusinessName\": \"\", "
			+ "\"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", \"Addresses\": \"null\", "
			+ "\"Party_InfoAssets\": \"\\u0000\"}";
	
	@Before
	public void setUp() {
		Gson gson = new Gson();
		cdcPartyBo = new CDCPartyBO(gson, partyDao);
		cdcPartyBo.LOG = LOG;
	}

	@Test
	public void testCDCInsert() {
		// Given parsed ChangeRecord from json
		CDCPartyChangeRecord cr = gson.fromJson(INSERT_CDC_JSON, CDCPartyChangeRecord.class);
		assertEquals("insert", cr.getEventType().toString());
		assertEquals("1IPAGO", cr.getPartyID());
		assertEquals("{\"ID_List\":{\"Email\":\"jbutt@gmail.com\"}}", cr.getIDList());

		// When process cr
		Party party = cdcPartyBo.processChangeRecord(cr);

		// Then event data inserted in scylla repository
		verify(partyDao).insert(party);
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcPartyBo.LOG).info("Party inserted in scylla = {} for changeRecord event = {}", party, cr);
	}

	@Test
	public void testCDCUpdate() {
		{
			// When parse json
			CDCPartyChangeRecord cr = gson.fromJson(UPDATE_BEFORE_CDC_JSON, CDCPartyChangeRecord.class);
			// And process cr
			Party party = cdcPartyBo.processChangeRecord(cr);

			// Then CDCPartyChangeRecord parsed
			assertEquals("update_before", cr.getEventType().toString());
			assertEquals("7893012032906067968IPAGO", cr.getPartyID());
			assertEquals("null", cr.getIDList());
			// Then do nothing
			verify(partyDao, never()).insert(party);
			// And logged at info level nothing done including the original ChangeRecord event as it came from maxscale.
			verify(cdcPartyBo.LOG).debug("Nothing done for update_before changeRecord event = {}", cr);
		}

		{
			// When parse json
			CDCPartyChangeRecord cr = gson.fromJson(UPDATE_AFTER_CDC_JSON, CDCPartyChangeRecord.class);
			// And process cr
			Party party = cdcPartyBo.processChangeRecord(cr);

			// Then CDCPartyChangeRecord parsed
			assertEquals("update_after", cr.getEventType().toString());
			assertEquals("7893012032906067968IPAGO", cr.getPartyID());
			assertEquals("null", cr.getIDList());
			// And then event data inserted in scylla repository
			verify(partyDao).update(party);
			// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
			verify(cdcPartyBo.LOG).info("Party updated in scylla = {} for changeRecord event = {}", party, cr);
		}
	}

	@Test
	public void testCDCDelete() {
		// When parse json
		CDCPartyChangeRecord cr = gson.fromJson(DELETE_CDC_JSON, CDCPartyChangeRecord.class);
		// And process cr
		Party party = cdcPartyBo.processChangeRecord(cr);

		// Then CDCPartyChangeRecord parsed
		assertEquals("delete", cr.getEventType().toString());
		assertEquals("5944926038373957632IPAGO", cr.getPartyID());
		assertEquals("null", cr.getIDList());
		// And then event data deleted in scylla repository
		verify(partyDao).delete(party);			
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcPartyBo.LOG).info("Party deleted in scylla = {} for changeRecord event = {}", party, cr);
	}
}
