package com.orwellg.yggdrasil.contract.cdc.topology.bolts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.orwellg.yggdrasil.contract.cdc.bo.CDCContractBO;
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
import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;
import com.orwellg.yggdrasil.contract.cdc.bo.CDCContractBOTest;

public class CDCContractBoltTest {

	protected final CDCContractChangeRecord INSERT_CHANGE_RECORD = new Gson().fromJson(CDCContractBOTest.INSERT_CDC_JSON, CDCContractChangeRecord.class);
	
	@Mock
	protected Tuple insertTuple;

	@Mock
	protected Session ses;
	@Mock
	protected CDCContractBO cdcContractBo;
	@Mock
	protected OutputCollector collector;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	protected CDCContractBolt cdcContractBolt = new CDCContractBolt();

	protected CDCContractBolt cdcContractBoltErrorScenario = new CDCContractBolt();
	@Mock
	protected CDCContractBO cdcContractBoErrorScenario;
	@Mock
	protected Logger logMock;

	@Before
	public void setUp() throws Throwable {
		cdcContractBolt.session = ses;
		cdcContractBolt.cdcContractBo = cdcContractBo;
		cdcContractBolt.setCollector(collector);

		cdcContractBoltErrorScenario.session = ses;
		cdcContractBoltErrorScenario.cdcContractBo = cdcContractBoErrorScenario;
		cdcContractBoltErrorScenario.setCollector(collector);
		cdcContractBoltErrorScenario.LOG = logMock;

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

		// When execute() with CDCContractChangeRecord in eventData of Tuple
		cdcContractBolt.execute(insertTuple);

		// Then Contract insert requested to CDCContractBO
		verify(cdcContractBolt.cdcContractBo).processChangeRecord(INSERT_CHANGE_RECORD);
		// And emit with CDCContractChangeRecord as result in Tuple
		verify(collector).emit(insertTuple, new Values(insertTuple.getValueByField("key"), insertTuple.getValueByField("processId"), INSERT_CHANGE_RECORD));
		verify(collector).ack(insertTuple);
	}

	@Test
	public void testExecuteErrorScenario() throws Exception {
		//	Scenario 5 - Error executing insert/update_after/delete
		//	- When insert/update_after/delete CDC event received on CDC topic "com.orwellg.yggdrasil.contract.CDC.request.1" and exception occurs
		//	- Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale. Nothing published to kafka topic.
		//	- And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely. Storm collector emit() and ack() must not be called.
		
		// Given execution that will raise an exception within the bolt
		RuntimeException simulatedEx = new RuntimeException("simulated error");
		when(cdcContractBoErrorScenario.processChangeRecord(INSERT_CHANGE_RECORD)).thenThrow(simulatedEx);
		Throwable cause = null;
		
		// When executed
		try {
			cdcContractBoltErrorScenario.execute(insertTuple);
			fail("Exception should have been raised");
		} catch (Exception e) {
			cause = e.getCause();
		}
		
		// Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale.
		verify(logMock).error(String.format("%sError in Action %s for %s. Message: %s", cdcContractBoltErrorScenario.logPreffix,
				INSERT_CHANGE_RECORD.getEventType().toString(), INSERT_CHANGE_RECORD, simulatedEx.getMessage()), simulatedEx);
		// And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely.
		assertEquals(simulatedEx, cause);
		// And storm collector emit() and ack() must not be called.
		verify(collector, never()).emit(any());
		verify(collector, never()).ack(any());
	}
}
