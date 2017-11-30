package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.Logger;
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
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBOTest;

public class CDCPartyBoltTest {

	protected CDCPartyBolt bolt = new CDCPartyBolt();

	protected final CDCPartyChangeRecord INSERT_CHANGE_RECORD = new Gson().fromJson(CDCPartyBOTest.INSERT_CDC_JSON, CDCPartyChangeRecord.class);
	
	@Mock
	protected Tuple insertTuple;

	@Mock
	protected Session ses;
	@Mock
	protected CDCPartyBO cdcPartyBo;
	@Mock
	protected OutputCollector collector;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	protected CDCPartyBolt cdcPartyBolt = new CDCPartyBolt();

	protected CDCPartyBolt cdcPartyBoltErrorScenario = new CDCPartyBolt();
	@Mock
	protected CDCPartyBO cdcPartyBoErrorScenario;
	@Mock
	protected Logger logMock;

	@Before
	public void setUp() throws Throwable {
		cdcPartyBolt.session = ses;
		cdcPartyBolt.cdcPartyBo = cdcPartyBo;
		cdcPartyBolt.setCollector(collector);

		cdcPartyBoltErrorScenario.session = ses;
		cdcPartyBoltErrorScenario.cdcPartyBo = cdcPartyBoErrorScenario;
		cdcPartyBoltErrorScenario.setCollector(collector);
		cdcPartyBoltErrorScenario.LOG = logMock;

		String key = "key1234";
		when(insertTuple.getValueByField("key")).thenReturn(key);
		String processId = "processId123";
		when(insertTuple.getValueByField("processId")).thenReturn(processId);
		when(insertTuple.getValueByField("eventData")).thenReturn(INSERT_CHANGE_RECORD);
		when(insertTuple.getValueByField("eventName")).thenReturn(INSERT_CHANGE_RECORD.getEventType().toString());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExecute() throws Exception {
		// Given insert CDC event tuple

		// When execute() with CDCPartyChangeRecord in eventData of Tuple
		cdcPartyBolt.execute(insertTuple);

		// Then Party insert requested to CDCPartyBO
		verify(cdcPartyBolt.cdcPartyBo).processChangeRecord(INSERT_CHANGE_RECORD);
		// And emit with CDCPartyChangeRecord as result in Tuple
		verify(collector).emit(insertTuple, new Values(insertTuple.getValueByField("key"), insertTuple.getValueByField("processId"), INSERT_CHANGE_RECORD));
		verify(collector).ack(insertTuple);
	}

	@Test
	public void testExecuteErrorScenario() throws Exception {
		//	Scenario 5 - Error executing insert/update_after/delete
		//	- When insert/update_after/delete CDC event received on CDC topic "com.orwellg.yggdrasil.party.CDC.request.1" and exception occurs
		//	- Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale. Nothing published to kafka topic.
		//	- And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely. Storm collector emit() and ack() must not be called.
		
		// Given execution that will raise an exception within the bolt
		RuntimeException simulatedEx = new RuntimeException("simulated error");
		when(cdcPartyBoErrorScenario.processChangeRecord(INSERT_CHANGE_RECORD)).thenThrow(simulatedEx);
		Throwable cause = null;
		
		// When executed
		try {
			cdcPartyBoltErrorScenario.execute(insertTuple);
			fail("Exception should have been raised");
		} catch (Exception e) {
			cause = e.getCause();
		}
		
		// Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale.
		verify(logMock).error(String.format("%sError in Action %s for %s. Message: %s", cdcPartyBoltErrorScenario.logPreffix,
				INSERT_CHANGE_RECORD.getEventType().toString(), INSERT_CHANGE_RECORD, simulatedEx.getMessage()), simulatedEx);
		// And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely.
		assertEquals(simulatedEx, cause);
		// And storm collector emit() and ack() must not be called.
		verify(collector, never()).emit(any());
		verify(collector, never()).ack(any());
	}
}
