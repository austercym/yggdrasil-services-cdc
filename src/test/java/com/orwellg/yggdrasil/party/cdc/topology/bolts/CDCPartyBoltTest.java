package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;

public class CDCPartyBoltTest {

	protected CDCPartyBolt bolt = new CDCPartyBolt();

	@Mock
	protected TopologyConfig config;

	@Mock
	protected Tuple tuple;
	protected String insertJson = 
			"{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
					+ "\"Party_ID\": \"1IPAGO\", \"FirstName\": \"James\", \"LastName\": \"Butt\", \"BusinessName\": \"\", \"TradingName\": \"\", "
					+ "\"Title\": 0, \"Salutation\": \"\", \"OtherNames\": \"Le\", \"ID_List\": \"{\\\"ID_List\\\":{\\\"Email\\\":\\\"jbutt@gmail.com\\\"}}\","
					+ " \"Addresses\": \"[]\", \"Party_InfoAssets\": \"\\u0000\"}";

	@Mock
	protected Session ses;
	@Mock
	protected CDCPartyBO cdcPartyBo;
	protected CDCPartyChangeRecord changeRecord = new Gson().fromJson(insertJson, CDCPartyChangeRecord.class);
	@Mock
	protected OutputCollector collector;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	protected CDCPartyBolt cdcPartyBolt = new CDCPartyBolt();

	@Before
	public void setUp() throws Throwable {
		// A real Tuple that arrives KafkaEventProcessBolt looks like this:
		// {topic=com.orwellg.yggdrasil.party.get.request.1, partition=0, offset=0, key=null, value=CjAuMC4xJjIwMTctMTEtMjggMTk6MTI6MDQCJEdldFBhcnR5VG9wb2xvZ3lJVBBHZXRQYXJ0eQIoR2V0UGFydHlUb3BvbG9neVRlc3RURVZFTlQtYjE3NjdjMWYtNDJmYi00Njk5LWE0MjYtZDk0ZTVjOGNiMTkyAlB7IklkIjp7IklkIjoiMjUxNzgwMDEyNjM2MjE1NzA1NklQQUdPIn19AAwxMzAwODAMSVBBR09PAgxJUEFHT08CAgI=, headers=AA==}
//		when(tuple.getValues()).thenReturn(Arrays.asList("", "", "", "", insertJson));
		when(tuple.getValueByField("eventData")).thenReturn(changeRecord);
		
		cdcPartyBolt.session = ses;
		cdcPartyBolt.cdcPartyBo = cdcPartyBo;
		cdcPartyBolt.setCollector(collector);
		
//		when(cdcPartyBo.parseChangeRecordJson(insertJson)).thenReturn(changeRecord);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExecute() throws Exception {
		// Given insert CDC event
		// When execute() with CDCPartyChangeRecord in value 4 of Tuple
		cdcPartyBolt.execute(tuple);
		// Then Party insert requested to CDCPartyBO
//		verify(cdcPartyBolt.cdcPartyBo).parseChangeRecordJson(insertJson);
		verify(cdcPartyBolt.cdcPartyBo).processChangeRecord(changeRecord);
		// And emit with CDCPartyChangeRecord as result in Tuple
//		verify(collector).emit(eq(tuple), any(Values.class));
	}
}
