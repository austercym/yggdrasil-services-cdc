package com.orwellg.yggdrasil.contract.cdc.topology.bolts;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.orwellg.yggdrasil.contract.cdc.bo.CDCContractBOTest;
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

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;

public class KafkaChangeRecordProcessBoltTest {
	
	protected KafkaChangeRecordProcessBolt bolt = new KafkaChangeRecordProcessBolt();

	protected final CDCContractChangeRecord INSERT_CHANGE_RECORD = new Gson().fromJson(CDCContractBOTest.INSERT_CDC_JSON, CDCContractChangeRecord.class);

	protected final String SCHEMA_CHANGE_JSON = "{\"namespace\": \"MaxScaleChangeDataSchema.avro\", \"type\": \"record\", \"name\": \"ChangeRecord\","
			+ " \"fields\": ["
			+ "{\"name\": \"domain\", \"type\": \"int\"}, {\"name\": \"server_id\", \"type\": \"int\"}, {\"name\": \"sequence\", \"type\": \"int\"},"
			+ " {\"name\": \"event_number\", \"type\": \"int\"}, {\"name\": \"timestamp\", \"type\": \"int\"}, {\"name\": \"event_type\","
			+ " \"type\": {\"type\": \"enum\", \"name\": \"EVENT_TYPES\", \"symbols\": [\"insert\", \"update_before\", \"update_after\", \"delete\"]}}, "
			+ "{\"name\": \"Contract_ID\", \"type\": \"string\", \"real_type\": \"varchar\", \"length\": 30 }, "
			+ "{\"name\": \"Product_IDs\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"Service_IDs\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"ContractInfoAssets\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"ContractOperations\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"Channels\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"ApplicableLocation\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }, "
			+ "{\"name\": \"Tags\", \"type\": \"string\", \"real_type\": \"longtext\", \"length\": -1 }"
			+ "]}";


	@Mock
	protected Tuple insertTuple;

	@Mock
	protected Tuple schemaChangeTuple;

	@Mock
	protected Tuple errorTuple;
	
	@Mock
	protected OutputCollector collector;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	protected Logger logMock;

	@Before
	public void setUp() throws Throwable {
		bolt.gson = new Gson();
		bolt.setCollector(collector);
		bolt.LOG = logMock;
		
		// CDC json in field 4 of input tuple
		when(insertTuple.getValues()).thenReturn(new Values("", "", "", "", CDCContractBOTest.INSERT_CDC_JSON));

		// CDC schema change json in field 4 of input tuple
		when(schemaChangeTuple.getValues()).thenReturn(new Values("", "", "", "", SCHEMA_CHANGE_JSON));
		
		// CDC json that will cause an error in bolt
		when(errorTuple.getValues()).thenReturn(new Values("", "", "", "", "incorrect json that will raise error"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExecute() throws Exception {
		// Given insert CDC event tuple
		// When execute() with CDCContractChangeRecord in eventData of Tuple
		bolt.execute(insertTuple);

		// Then emit with CDCContractChangeRecord as eventData in Tuple
		CDCContractChangeRecord cr = INSERT_CHANGE_RECORD;
		verify(collector).emit(insertTuple, new Values(bolt.outputTupleKey(cr), bolt.outputTupleProcessId(cr), bolt.outputTupleEventName(cr), cr));
		verify(collector).ack(insertTuple);
	}
	
	@Test
	public void testIgnoreSchemaChangeEvents() throws Exception {
		// Given schema change CDC event tuple
		// When execute() with CDCContractChangeRecord in eventData of Tuple
		bolt.execute(schemaChangeTuple);

		// Then ignore (do not emit, but ack)
		verify(collector, never()).emit(any(String.class), any());
		verify(collector).ack(schemaChangeTuple);
		// And logged at info level schema change CDC has been ignored
		verify(logMock).info(String.format("Ignoring schema change ChangeRecord = %s", schemaChangeTuple));
	}

	@Test
	public void testExecuteErrorScenario() throws Exception {
		//	Scenario 5 - Error executing insert/update_after/delete
		//	- When insert/update_after/delete CDC event received on CDC topic "com.orwellg.yggdrasil.contract.CDC.request.1" and exception occurs
		//	- Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale. Nothing published to kafka topic.
		//	- And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely. Storm collector emit() and ack() must not be called.
		
		// Given execution that will raise an exception within the bolt
		
		// When executed
		Throwable cause = null;
		try {
			bolt.execute(errorTuple);
			fail("Exception should have been raised");
		} catch (Exception e) {
			cause = e.getCause();
		}
		
		// Then logged at error level full stacktrace including the original ChangeRecord event as it came from maxscale.
		verify(logMock).error(String.format("Error decoding ChangeRecord %s. Message: %s", errorTuple, cause.getMessage()), cause);
		// And exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely.
		assertNotNull(cause);
		// And storm collector emit() and ack() must not be called.
		verify(collector, never()).emit(any());
		verify(collector, never()).ack(any());
	}
}
