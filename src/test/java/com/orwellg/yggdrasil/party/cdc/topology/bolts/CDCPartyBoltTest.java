package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBOTest;

public class CDCPartyBoltTest {

	protected CDCPartyBolt bolt = new CDCPartyBolt();

	@Mock
	protected TopologyConfig config;

	@Mock
	protected Tuple insertTuple;
	@Mock
	protected Tuple updateBeforeTuple;
	@Mock
	protected Tuple updateAfterTuple;
	@Mock
	protected Tuple deleteTuple;

	@Mock
	protected Session ses;
	@Mock
	protected CDCPartyBO cdcPartyBo;
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
		// Given insert CDC event tuple
		CDCPartyChangeRecord insertChangeRecord = new Gson().fromJson(CDCPartyBOTest.INSERT_CDC_JSON, CDCPartyChangeRecord.class);
		String key = "key1234";
		when(insertTuple.getValueByField("key")).thenReturn(key);
		String processId = "processId123";
		when(insertTuple.getValueByField("processId")).thenReturn(processId);
		when(insertTuple.getValueByField("eventData")).thenReturn(insertChangeRecord);
		when(insertTuple.getValueByField("eventName")).thenReturn(insertChangeRecord.getEventType().toString());
		
		// When execute() with CDCPartyChangeRecord in eventData of Tuple
		cdcPartyBolt.execute(insertTuple);
		
		// Then Party insert requested to CDCPartyBO
		verify(cdcPartyBolt.cdcPartyBo).processChangeRecord(insertChangeRecord);
		// And emit with CDCPartyChangeRecord as result in Tuple
		verify(collector).emit(insertTuple, new Values(insertTuple.getValueByField("key"), insertTuple.getValueByField("processId"), insertChangeRecord));
		verify(collector).ack(insertTuple);
	}

//	Scenario 5 - Error executing insert/update_after/delete
//	- When insert/update_after/delete CDC event received on CDC topic "com.orwellg.yggdrasil.party.CDC.request.1" and exception occurs
//	- Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale. Nothing published to kafka topic.
//	- And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely. Storm collector emit() and ack() must not be called.
}
