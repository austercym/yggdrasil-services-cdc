package com.orwellg.yggdrasil.contract.cdc.bo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ContractRepositoryImpl;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ContractRepositoryImpl;
import com.orwellg.umbrella.commons.types.scylla.entities.Contract;

public class CDCContractBOTest {
	
	protected CDCContractBO cdcContractBo;
	
	@Mock
	protected ContractRepositoryImpl contractDao;
	
	@Mock
	protected Logger LOG;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	protected Gson gson = new Gson();

	public static final String INSERT_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
			+ "\"Contract_ID\": \"200000001IPAGO\", "
			+ "\"Product_IDs\": \"{\\\"ProductIDs\\\":[{\\\"ID\\\":\\\"650002IPAGO\\\"},{\\\"ID\\\":\\\"18237641962IPAGO\\\"}]}\", "
			+ "\"Service_IDs\": \"{\\\"ServiceIDs\\\":[{\\\"ID\\\":\\\"18237641962IPAGO\\\"},{\\\"ID\\\":\\\"18237641963IPAGO\\\"}]}\", "
			+ "\"ContractInfoAssets\": \"\\u0000\", "
			+ "\"ContractOperations\": \"{\\\"ContractOperations\\\":{\\\"CanUseATM\\\":\\\"true\\\"}}\", "
			+ "\"Channels\": \"\", "
			+ "\"ApplicableLocation\": \"\", "
			+ "\"Tags\": \"\"}";

	public static final String UPDATE_BEFORE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 1, \"timestamp\": 1511782041, \"event_type\": \"update_before\", "
			+ "\"Contract_ID\": \"224643001IPAGO\", "
			+ "\"Product_IDs\": \"{\\\"ProductIDs\\\":[{\\\"ID\\\":\\\"650002IPAGO\\\"},{\\\"ID\\\":\\\"18237641962IPAGO\\\"}]}\", "
			+ "\"Service_IDs\": \"{\\\"ServiceIDs\\\":[{\\\"ID\\\":\\\"18237641962IPAGO\\\"},{\\\"ID\\\":\\\"18237641963IPAGO\\\"}]}\", "
			+ "\"ContractInfoAssets\": \"\\u0000\", "
			+ "\"ContractOperations\": \"{\\\"ContractOperations\\\":{\\\"CanUseATM\\\":\\\"true\\\"}}\", "
			+ "\"Channels\": \"\", "
			+ "\"ApplicableLocation\": \"\", "
			+ "\"Tags\": \"\"}";

	public static final String UPDATE_AFTER_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 2, \"timestamp\": 1511782041, \"event_type\": \"update_after\", "
			+ "\"Contract_ID\": \"2034532001IPAGO\", "
			+ "\"Product_IDs\": \"{\\\"ProductIDs\\\":[{\\\"ID\\\":\\\"650002IPAGO\\\"},{\\\"ID\\\":\\\"18237641962IPAGO\\\"}]}\", "
			+ "\"Service_IDs\": \"{\\\"ServiceIDs\\\":[{\\\"ID\\\":\\\"18237641962IPAGO\\\"},{\\\"ID\\\":\\\"18237641963IPAGO\\\"}]}\", "
			+ "\"ContractInfoAssets\": \"\\u0000\", "
			+ "\"ContractOperations\": \"{\\\"ContractOperations\\\":{\\\"CanUseATM\\\":\\\"true\\\"}}\", "
			+ "\"Channels\": \"\", "
			+ "\"ApplicableLocation\": \"\", "
			+ "\"Tags\": \"\"}";

	public static final String DELETE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 904, \"event_number\": 1, \"timestamp\": 1511782192, \"event_type\": \"delete\", "
			+ "\"Contract_ID\": \"223423401IPAGO\", "
			+ "\"Product_IDs\": \"{\\\"ProductIDs\\\":[{\\\"ID\\\":\\\"650002IPAGO\\\"},{\\\"ID\\\":\\\"18237641962IPAGO\\\"}]}\", "
			+ "\"Service_IDs\": \"{\\\"ServiceIDs\\\":[{\\\"ID\\\":\\\"18237641962IPAGO\\\"},{\\\"ID\\\":\\\"18237641963IPAGO\\\"}]}\", "
			+ "\"ContractInfoAssets\": \"\\u0000\", "
			+ "\"ContractOperations\": \"{\\\"ContractOperations\\\":{\\\"CanUseATM\\\":\\\"true\\\"}}\", "
			+ "\"Channels\": \"\", "
			+ "\"ApplicableLocation\": \"\", "
			+ "\"Tags\": \"\"}";
	
	@Before
	public void setUp() {
		Gson gson = new Gson();
		cdcContractBo = new CDCContractBO(gson, contractDao);
		cdcContractBo.LOG = LOG;
	}

	@Test
	public void testCDCInsert() {
		// Given parsed ChangeRecord from json
		CDCContractChangeRecord cr = gson.fromJson(INSERT_CDC_JSON, CDCContractChangeRecord.class);
		assertEquals("insert", cr.getEventType().toString());
		assertEquals("200000001IPAGO", cr.getContractID());
		assertEquals("{\"ContractOperations\":{\"CanUseATM\":\"true\"}}", cr.getContractOperations());

		// When process cr
		Contract contract = cdcContractBo.processChangeRecord(cr);

		// Then event data inserted in scylla repository
		verify(contractDao).insert(contract);
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcContractBo.LOG).info("Contract inserted in scylla = {} for changeRecord event = {}", contract, cr);
	}

	@Test
	public void testCDCUpdate() {
		{
			// When parse json
			CDCContractChangeRecord cr = gson.fromJson(UPDATE_BEFORE_CDC_JSON, CDCContractChangeRecord.class);
			// And process cr
			Contract contract = cdcContractBo.processChangeRecord(cr);

			// Then CDCContractChangeRecord parsed
			assertEquals("update_before", cr.getEventType().toString());
			assertEquals("224643001IPAGO", cr.getContractID());
			// Then do nothing
			verify(contractDao, never()).insert(contract);
			// And logged at info level nothing done including the original ChangeRecord event as it came from maxscale.
			verify(cdcContractBo.LOG).debug("Nothing done for update_before changeRecord event = {}", cr);
		}

		{
			// When parse json
			CDCContractChangeRecord cr = gson.fromJson(UPDATE_AFTER_CDC_JSON, CDCContractChangeRecord.class);
			// And process cr
			Contract contract = cdcContractBo.processChangeRecord(cr);

			// Then CDCContractChangeRecord parsed
			assertEquals("update_after", cr.getEventType().toString());
			assertEquals("2034532001IPAGO", cr.getContractID());
			// And then event data inserted in scylla repository
			verify(contractDao).update(contract);
			// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
			verify(cdcContractBo.LOG).info("Contract updated in scylla = {} for changeRecord event = {}", contract, cr);
		}
	}

	@Test
	public void testCDCDelete() {
		// When parse json
		CDCContractChangeRecord cr = gson.fromJson(DELETE_CDC_JSON, CDCContractChangeRecord.class);
		// And process cr
		Contract contract = cdcContractBo.processChangeRecord(cr);

		// Then CDCContractChangeRecord parsed
		assertEquals("delete", cr.getEventType().toString());
		assertEquals("223423401IPAGO", cr.getContractID());
		// And then event data deleted in scylla repository
		verify(contractDao).delete(contract);			
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcContractBo.LOG).info("Contract deleted in scylla = {} for changeRecord event = {}", contract, cr);
	}
}
