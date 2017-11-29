package com.orwellg.yggdrasil.party.cdc.bo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;

public class CDCPartyBOTest {
	
	protected CDCPartyBO cdcPartyBo;
	
	@Mock
	PartyRepositoryImpl partyDao;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	protected Gson gson = new Gson();
	
	@Before
	public void setUp() {
		Gson gson = new Gson();
		cdcPartyBo = new CDCPartyBO(gson, partyDao);
	}

	@Test
	public void testCDCInsert() {
		// Given insert CDC event
		String json = 
				"{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
						+ "\"Party_ID\": \"1IPAGO\", \"FirstName\": \"James\", \"LastName\": \"Butt\", \"BusinessName\": \"\", \"TradingName\": \"\", "
						+ "\"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"Le\", \"ID_List\": \"{\\\"ID_List\\\":{\\\"Email\\\":\\\"jbutt@gmail.com\\\"}}\","
						+ " \"Addresses\": \"[]\", \"Party_InfoAssets\": \"\\u0000\"}";

		// When parse json
		CDCPartyChangeRecord cr = gson.fromJson(json, CDCPartyChangeRecord.class);
		// And process cr
		Party party = cdcPartyBo.processChangeRecord(cr);

		// Then CDCPartyChangeRecord parsed
		assertEquals("insert", cr.getEventType().toString());
		assertEquals("1IPAGO", cr.getPartyID());
		assertEquals("{\"ID_List\":{\"Email\":\"jbutt@gmail.com\"}}", cr.getIDList());
		// And then event data inserted in scylla repository
		verify(partyDao).insert(party);
	}

	@Test
	public void testCDCUpdate() {
		{
			// Given update_before CDC event
			String json = 
					"{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 1, \"timestamp\": 1511782041, \"event_type\": \"update_before\", "
							+ "\"Party_ID\": \"7893012032906067968IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"\", \"BusinessName\": \"\", "
							+ "\"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", \"Addresses\": \"null\", "
							+ "\"Party_InfoAssets\": \"\\u0000\"}";

			// When parse json
			CDCPartyChangeRecord cr = gson.fromJson(json, CDCPartyChangeRecord.class);
			// And process cr
			Party party = cdcPartyBo.processChangeRecord(cr);

			// Then CDCPartyChangeRecord parsed
			assertEquals("update_before", cr.getEventType().toString());
			assertEquals("7893012032906067968IPAGO", cr.getPartyID());
			assertEquals("null", cr.getIDList());
			// Then do nothing
			verify(partyDao, never()).insert(party);
		}

		{
			// Given update_after CDC event
			String json = 
					"{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 2, \"timestamp\": 1511782041, \"event_type\": \"update_after\", "
							+ "\"Party_ID\": \"7893012032906067968IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"UpdatedLastNameForTest\", "
							+ "\"BusinessName\": \"\", \"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", "
							+ "\"Addresses\": \"null\", \"Party_InfoAssets\": \"\\u0000\"}";

			// When parse json
			CDCPartyChangeRecord cr = gson.fromJson(json, CDCPartyChangeRecord.class);
			// And process cr
			Party party = cdcPartyBo.processChangeRecord(cr);

			// Then CDCPartyChangeRecord parsed
			assertEquals("update_after", cr.getEventType().toString());
			assertEquals("7893012032906067968IPAGO", cr.getPartyID());
			assertEquals("null", cr.getIDList());
			// And then event data inserted in scylla repository
			verify(partyDao).update(party);
		}
	}

	@Test
	public void testCDCDelete() {
		// Given delete CDC event
		String json = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 904, \"event_number\": 1, \"timestamp\": 1511782192, \"event_type\": \"delete\", "
				+ "\"Party_ID\": \"5944926038373957632IPAGO\", \"FirstName\": \"FirstName\", \"LastName\": \"\", \"BusinessName\": \"\", "
				+ "\"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"\", \"ID_List\": \"null\", \"Addresses\": \"null\", "
				+ "\"Party_InfoAssets\": \"\\u0000\"}";

		// When parse json
		CDCPartyChangeRecord cr = gson.fromJson(json, CDCPartyChangeRecord.class);
		// And process cr
		Party party = cdcPartyBo.processChangeRecord(cr);

		// Then CDCPartyChangeRecord parsed
		assertEquals("delete", cr.getEventType().toString());
		assertEquals("5944926038373957632IPAGO", cr.getPartyID());
		assertEquals("null", cr.getIDList());
		// And then event data inserted in scylla repository
		verify(partyDao).delete(party);
	}
}
