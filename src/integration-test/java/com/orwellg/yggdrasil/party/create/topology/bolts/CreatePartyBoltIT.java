package com.orwellg.yggdrasil.party.create.topology.bolts;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetailType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetails;
import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdap;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;
import com.orwellg.yggdrasil.party.dao.PartyDAO;
import com.orwellg.yggdrasil.party.dao.PartyPersonalDetailsDAO;

public class CreatePartyBoltIT {

	/**In-process zookeeper instance*/
    protected static TestingServer zkInstance;

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";
    
	protected static CuratorFramework client;
	
	// Local idgen not to need zookeeper connection
	protected UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
	protected CreatePartyBolt bolt = new CreatePartyBolt();
	protected static PartyDAO partyDao;
	protected static PartyPersonalDetailsDAO personalDetailsDao;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Throwable {
		// Start H2 db server in-memory
		Connection connection = DriverManager.getConnection(JDBC_CONN);
		
		// Create schema
		H2DbHelper h2 = new H2DbHelper();
		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
		
		// Starts a ZooKeeper server
//		zkInstance.start().result(Duration.ofSeconds(90));
		zkInstance = new TestingServer(6969);
//		zkInstance.start();

//		zookeeperHosts = zkInstance.getConnectString();

		String zookeeperHost = "127.0.0.1:6969";
		client = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
		client.start();
		
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		zk.printAllProps();

		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.url", JDBC_CONN);
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user", "");
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password", "");
		
		zk.printAllProps();
		
		TopologyConfigWithLdapFactory.resetTopologyConfig();
		TopologyConfigWithLdap config = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig("topo.properties");
		assertEquals(zookeeperHost, config.getZookeeperConnection());
		TopologyConfigFactory.resetTopologyConfig();
		TopologyConfigFactory.getTopologyConfig("topo.properties");
		assertEquals(JDBC_CONN, TopologyConfigFactory.getTopologyConfig().getMariaDBConfig().getMariaDBParams().getUrl());
		
		assertEquals("localhost", config.getMariaDBConfig().getMariaDBParams().getHost());
		assertEquals("3306", config.getMariaDBConfig().getMariaDBParams().getPort());

		partyDao = new PartyDAO(MariaDbManager.getInstance().getConnection());
		personalDetailsDao = new PartyPersonalDetailsDAO(MariaDbManager.getInstance().getConnection());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// Close the curator client
		if (client != null) {
			client.close();
		}
		
        TopologyConfigWithLdapFactory.resetTopologyConfig();
		TopologyConfigFactory.resetTopologyConfig();
		assertEquals(CuratorFrameworkState.STOPPED, client.getState());

        // Stops the ZooKeeper instance and also deletes any data files.
		// This makes sure no state is kept between test cases.
//		zkInstance.destroy().ready(Duration.ofSeconds(90));
		zkInstance.stop();
        zkInstance.close();
	}

	@Test
	public void testSaveBasicParty() throws Exception {
		// Given Party (basic)
		// When saveParty
		// Then Party in db

		// Given Party (basic)
		PartyType partyType = new PartyType();
		String partyIdToCreate = idGen.generateLocalUniqueIDStr();
		partyType.setId(new PartyIdType(partyIdToCreate));
		partyType.setFirstName("PartyChunga1");
		Party p = new Party(partyType);

		// When saveParty
		bolt.insertParty(p);

		// Then Party in db
		Party resultParty = partyDao.getById(partyIdToCreate);
		Assert.assertEquals(p.getParty().getId().getId(), resultParty.getParty().getId().getId());
		Assert.assertEquals(p.getParty().getFirstName(), resultParty.getParty().getFirstName());
	}

	@Test
	public void testSavePersonalParty() throws Exception {
		// Given Party with PersonalDetails
		// When saveParty
		// Then Party in db
		// and PersonalDetails in db

		// Given Party (with id) with details (with id)
		PartyType partyType = new PartyType();
		String partyIdToCreate = idGen.generateLocalUniqueIDStr();
		partyType.setId(new PartyIdType(partyIdToCreate));
		partyType.setFirstName("PartyChunga1");
		Party p = new Party(partyType);
		// Details
		PartyPersonalDetailsType detT = new PartyPersonalDetailsType();
		String detIdToCreate = idGen.generateLocalUniqueIDStr();
		detT.setId(detIdToCreate);
		detT.setPartyID(partyIdToCreate);
		PPEmploymentDetailType empDet = new PPEmploymentDetailType();
		empDet.setJobTitle("job title");
		detT.setEmploymentDetails(new PPEmploymentDetails(empDet));
		p.getParty().setPersonalDetails(detT);

		// When saveParty
		bolt.insertParty(p);

		// Then Party in db
		Party resultParty = partyDao.getById(partyIdToCreate);
		Assert.assertEquals(p.getParty().getId().getId(), resultParty.getParty().getId().getId());
		Assert.assertEquals(p.getParty().getFirstName(), resultParty.getParty().getFirstName());
		// and PersonalDetails in db
		PartyPersonalDetailsType resultDet = personalDetailsDao.getById(detIdToCreate);
		Assert.assertEquals(detT.getEmploymentDetails(), resultDet.getEmploymentDetails());
	}

	@Test
	public void testSaveNonPersonalParty() {
		// TODO Given Party with NonPersonalDetails
		// When saveParty
		// Then Party in db
		// and NonPersonalDetails in db
	}
	
//	@Test
//	public void testExecute() {
//		fail("Not yet implemented");
//	}

}
