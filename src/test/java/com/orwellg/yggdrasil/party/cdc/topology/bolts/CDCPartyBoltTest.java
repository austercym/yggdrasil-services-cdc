//package com.orwellg.yggdrasil.party.cdc.topology.bolts;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.when;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.junit.MockitoJUnit;
//import org.mockito.junit.MockitoRule;
//
//import com.orwellg.umbrella.avro.types.party.PartyIdType;
//import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
//import com.orwellg.umbrella.avro.types.party.PartyType;
//import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetailType;
//import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetails;
//import com.orwellg.umbrella.commons.config.MariaDBConfig;
//import com.orwellg.umbrella.commons.config.params.MariaDBParams;
//import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
//import com.orwellg.umbrella.commons.repositories.mariadb.impl.PartyDAO;
//import com.orwellg.umbrella.commons.repositories.mariadb.impl.PartyPersonalDetailsDAO;
//import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
//import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
//import com.orwellg.umbrella.commons.types.party.Party;
//import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;
//import com.orwellg.yggdrasil.party.cdc.topology.bolts.PartyCDCBolt;
//import com.orwellg.yggdrasil.party.dao.MariaDbManager;
//
//public class CDCPartyBoltTest {
//
//	// Local idgen not to need zookeeper connection
//	protected UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
//	protected PartyCDCBolt bolt = new PartyCDCBolt();
//	protected static PartyDAO partyDao;
//	protected static PartyPersonalDetailsDAO personalDetailsDao;
//
//	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
//	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
//	private static final String DELIMITER = ";";
//	
//	@Mock
//	protected TopologyConfig config;
//	
//	@Mock
//	protected MariaDBConfig mariaDbConfig;
//
//	@Mock
//	protected MariaDBParams mariaDbParams;
//
//	@Rule
//	public MockitoRule mockitoRule = MockitoJUnit.rule();
//
//	@Before
//	public void setUp() throws Throwable {
//		// Start H2 db server in-memory
//		Connection connection = DriverManager.getConnection(JDBC_CONN);
//		
//		// Create schema
//		H2DbHelper h2 = new H2DbHelper();
//		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
//		
//		TopologyConfigFactory.setTopologyConfig(config);
//
//		when(config.getMariaDBConfig()).thenReturn(mariaDbConfig);
//		when(mariaDbConfig.getMariaDBParams()).thenReturn(mariaDbParams);
////		when(mariaDbParams.getHost()).thenReturn("localhost");
////		when(mariaDbParams.getPort()).thenReturn("3306");
////		when(mariaDbParams.getDbName()).thenReturn("ipagoo");
//		when(mariaDbParams.getUser()).thenReturn("");
//		when(mariaDbParams.getPassword()).thenReturn("");
//
//		when(mariaDbParams.getUrl()).thenReturn(JDBC_CONN);
//		
//		TopologyConfig c2 = TopologyConfigFactory.getTopologyConfig("topo.properties");
//		assertEquals(config, c2);
//		MariaDbManager.getInstance("topo.properties");
//		assertEquals(JDBC_CONN, MariaDbManager.getUrl());
//		
//		partyDao = new PartyDAO(MariaDbManager.getInstance().getConnection());
//		personalDetailsDao = new PartyPersonalDetailsDAO(MariaDbManager.getInstance().getConnection());
//	}
//
//	@After
//	public void tearDownAfterClass() throws Exception {
//		TopologyConfigFactory.resetTopologyConfig();
//	}
//
//	@Test
//	public void testSaveBasicParty() throws Exception {
//		// Given Party (basic)
//		// When saveParty
//		// Then Party in db
//
//		// Given Party (basic)
//		PartyType partyType = new PartyType();
//		String partyIdToCreate = idGen.generateLocalUniqueIDStr();
//		partyType.setId(new PartyIdType(partyIdToCreate));
//		partyType.setFirstName("PartyChunga1");
//		Party p = new Party(partyType);
//
//		// When saveParty
//		bolt.insertParty(p);
//
//		// Then Party in db
//		Party resultParty = partyDao.getById(partyIdToCreate);
//		Assert.assertEquals(p.getParty().getId().getId(), resultParty.getParty().getId().getId());
//		Assert.assertEquals(p.getParty().getFirstName(), resultParty.getParty().getFirstName());
//	}
//
//	@Test
//	public void testSavePersonalParty() throws Exception {
//		// Given Party with PersonalDetails
//		// When saveParty
//		// Then Party in db
//		// and PersonalDetails in db
//
//		// Given Party (with id) with details (with id)
//		PartyType partyType = new PartyType();
//		String partyIdToCreate = idGen.generateLocalUniqueIDStr();
//		partyType.setId(new PartyIdType(partyIdToCreate));
//		partyType.setFirstName("PartyChunga1");
//		Party p = new Party(partyType);
//		// Details
//		PartyPersonalDetailsType detT = new PartyPersonalDetailsType();
//		String detIdToCreate = idGen.generateLocalUniqueIDStr();
//		detT.setId(detIdToCreate);
//		detT.setPartyID(partyIdToCreate);
//		PPEmploymentDetailType empDet = new PPEmploymentDetailType();
//		empDet.setJobTitle("job title");
//		detT.setEmploymentDetails(new PPEmploymentDetails(empDet));
//		p.getParty().setPersonalDetails(detT);
//
//		// When saveParty
//		bolt.insertParty(p);
//
//		// Then Party in db
//		Party resultParty = partyDao.getById(partyIdToCreate);
//		Assert.assertEquals(p.getParty().getId().getId(), resultParty.getParty().getId().getId());
//		Assert.assertEquals(p.getParty().getFirstName(), resultParty.getParty().getFirstName());
//		// and PersonalDetails in db
//		PartyPersonalDetailsType resultDet = personalDetailsDao.getById(detIdToCreate);
//		Assert.assertEquals(detT.getEmploymentDetails(), resultDet.getEmploymentDetails());
//	}
//
////	@Test
////	public void testSaveNonPersonalParty() {
//		// TODO Given Party with NonPersonalDetails
//		// When saveParty
//		// Then Party in db
//		// and NonPersonalDetails in db
////	}
//}
