package com.orwellg.yggdrasil.party.cdc.bo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.types.scylla.entities.Party;

public class CDCChangeRecordToEntityMapperTest {

	protected Gson gson = new Gson();
	
	@Test
	public void test() {
		String json = 
				"{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
						+ "\"Party_ID\": \"1IPAGO\", \"FirstName\": \"James\", \"LastName\": \"Butt\", \"BusinessName\": \"\", "
						+ "\"TradingName\": \"\", \"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"Le\", "
						+ "\"ID_List\": \"{\\\"ID_List\\\":{\\\"Email\\\":\\\"jbutt@gmail.com\\\"}}\", \"Addresses\": \"[]\", \"Party_InfoAssets\": \"\\u0000\"}";
		CDCPartyChangeRecord changeRecord = gson.fromJson(json, CDCPartyChangeRecord.class);
		assertEquals("insert", changeRecord.getEventType().toString());
		assertEquals("1IPAGO", changeRecord.getPartyID());
		assertEquals("{\"ID_List\":{\"Email\":\"jbutt@gmail.com\"}}", changeRecord.getIDList());
		
		CDCChangeRecordToEntityMapper mapper = new CDCChangeRecordToEntityMapper();
		Party party = mapper.map(changeRecord, Party.class);
		
		assertEquals("1IPAGO", party.getPartyId());
		assertEquals("James", party.getFirstName());
		assertTrue(StringUtils.isEmpty(party.getBusinessName()));
		assertEquals("{\"ID_List\":{\"Email\":\"jbutt@gmail.com\"}}", party.getIdList());
		assertEquals("[]", party.getAddresses());
		assertEquals("\u0000", party.getPartyInfoAssets());
	}

}
