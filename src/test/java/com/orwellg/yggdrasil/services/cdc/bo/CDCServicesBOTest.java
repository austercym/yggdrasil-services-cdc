package com.orwellg.yggdrasil.services.cdc.bo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.orwellg.umbrella.avro.types.cdc.CDCServicesChangeRecord;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ServicesRepositoryImpl;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.gson.Gson;
import com.orwellg.umbrella.commons.types.scylla.entities.Services;

public class CDCServicesBOTest {
	
	protected CDCServicesBO cdcServicesBo;
	
	@Mock
	protected ServicesRepositoryImpl servicesDao;
	
	@Mock
	protected Logger LOG;

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();
	
	protected Gson gson = new Gson();

	public static final String INSERT_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 45, \"event_number\": 1, \"timestamp\": 1511543507, \"event_type\": \"insert\", "
			+ "\"Service_ID\": \"Service1\", "
			+ "\"Contract_ID\": \"Contract1\", "
			+ "\"PSHR\": \"PSHR1\", "
			+ "\"ServiceFeatures\": \"ServiceFeatures1\", "
			+ "\"ServiceOperations\": \"ServiceOperations1\", "
			+ "\"Tags\": \"Tags1\"}";

	public static final String UPDATE_BEFORE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 1, \"timestamp\": 1511782041, \"event_type\": \"update_before\", "
			+ "\"Service_ID\": \"Service2\", "
			+ "\"Contract_ID\": \"Contract2\", "
			+ "\"PSHR\": \"PSHR2\", "
			+ "\"ServiceFeatures\": \"ServiceFeatures2\", "
			+ "\"ServiceOperations\": \"ServiceOperations2\", "
			+ "\"Tags\": \"Tags2\"}";

	public static final String UPDATE_AFTER_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 902, \"event_number\": 2, \"timestamp\": 1511782041, \"event_type\": \"update_after\", "
			+ "\"Service_ID\": \"Service3\", "
			+ "\"Contract_ID\": \"Contract3\", "
			+ "\"PSHR\": \"PSHR3\", "
			+ "\"ServiceFeatures\": \"ServiceFeatures3\", "
			+ "\"ServiceOperations\": \"ServiceOperations3\", "
			+ "\"Tags\": \"Tags3\"}";

	public static final String DELETE_CDC_JSON = "{\"domain\": 0, \"server_id\": 1, \"sequence\": 904, \"event_number\": 1, \"timestamp\": 1511782192, \"event_type\": \"delete\", "
			+ "\"Service_ID\": \"Service4\", "
			+ "\"Contract_ID\": \"Contract4\", "
			+ "\"PSHR\": \"PSHR4\", "
			+ "\"ServiceFeatures\": \"ServiceFeatures4\", "
			+ "\"ServiceOperations\": \"ServiceOperations4\", "
			+ "\"Tags\": \"Tags4\"}";
	
	@Before
	public void setUp() {
		Gson gson = new Gson();
		cdcServicesBo = new CDCServicesBO(gson, servicesDao);
		cdcServicesBo.LOG = LOG;
	}

	@Test
	public void testCDCInsert() {
		// Given parsed ChangeRecord from json
		CDCServicesChangeRecord cr = gson.fromJson(INSERT_CDC_JSON, CDCServicesChangeRecord.class);
		assertEquals("insert", cr.getEventType().toString());
		assertEquals("Service1", cr.getServiceID());
		assertEquals("ServiceOperations1", cr.getServiceOperations());

		// When process cr
		Services services = cdcServicesBo.processChangeRecord(cr);

		// Then event data inserted in scylla repository
		verify(servicesDao).insert(services);
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcServicesBo.LOG).info("Services inserted in scylla = {} for changeRecord event = {}", services, cr);
	}

	@Test
	public void testCDCUpdate() {
		{
			// When parse json
			CDCServicesChangeRecord cr = gson.fromJson(UPDATE_BEFORE_CDC_JSON, CDCServicesChangeRecord.class);
			// And process cr
			Services services = cdcServicesBo.processChangeRecord(cr);

			// Then CDCServicesChangeRecord parsed
			assertEquals("update_before", cr.getEventType().toString());
			assertEquals("Service2", cr.getServiceID());
			// Then do nothing
			verify(servicesDao, never()).insert(services);
			// And logged at info level nothing done including the original ChangeRecord event as it came from maxscale.
			verify(cdcServicesBo.LOG).debug("Nothing done for update_before changeRecord event = {}", cr);
		}

		{
			// When parse json
			CDCServicesChangeRecord cr = gson.fromJson(UPDATE_AFTER_CDC_JSON, CDCServicesChangeRecord.class);
			// And process cr
			Services services = cdcServicesBo.processChangeRecord(cr);

			// Then CDCServicesChangeRecord parsed
			assertEquals("update_after", cr.getEventType().toString());
			assertEquals("Service3", cr.getServiceID());
			// And then event data inserted in scylla repository
			verify(servicesDao).update(services);
			// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
			verify(cdcServicesBo.LOG).info("Services updated in scylla = {} for changeRecord event = {}", services, cr);
		}
	}

	@Test
	public void testCDCDelete() {
		// When parse json
		CDCServicesChangeRecord cr = gson.fromJson(DELETE_CDC_JSON, CDCServicesChangeRecord.class);
		// And process cr
		Services services = cdcServicesBo.processChangeRecord(cr);

		// Then CDCServicesChangeRecord parsed
		assertEquals("delete", cr.getEventType().toString());
		assertEquals("Service4", cr.getServiceID());
		// And then event data deleted in scylla repository
		verify(servicesDao).delete(services);			
		// And logged at info level ChangeRecord executed including the original ChangeRecord event as it came from maxscale.
		verify(cdcServicesBo.LOG).info("Services deleted in scylla = {} for changeRecord event = {}", services, cr);
	}
}
