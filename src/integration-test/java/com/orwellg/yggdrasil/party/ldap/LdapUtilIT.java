package com.orwellg.yggdrasil.party.ldap;

import static org.junit.Assert.assertEquals;

import javax.naming.directory.DirContext;

import org.apache.curator.test.TestingServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.yggdrasil.party.config.LdapParams;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdap;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;

public class LdapUtilIT {

//	/**In-process zookeeper instance*/
//	private static final ZKInstance zkInstance = ZKFactory.apply()
//			 .withPort(6969)
//			 .create();	
    protected static TestingServer zkInstance;

	private final static Logger LOG = LogManager.getLogger(LdapUtilIT.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Throwable {
		// Starts a ZooKeeper server
//		zkInstance.start().result(Duration.ofSeconds(90));
		zkInstance = new TestingServer(6969);
//		zkInstance.start();

//		zookeeperHosts = zkInstance.getConnectString();
		
		TopologyConfigWithLdapFactory.resetTopologyConfig();
		
		TopologyConfigWithLdap config = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig("topo.properties");
		assertEquals(LdapParams.URL_DEFAULT, config.getLdapConfig().getLdapParams().getUrl());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TopologyConfigWithLdapFactory.getTopologyConfig().close();
		TopologyConfigWithLdapFactory.resetTopologyConfig();

        // Stops the ZooKeeper instance and also deletes any data files.
		// This makes sure no state is kept between test cases.
//		zkInstance.destroy().ready(Duration.ofSeconds(90));
		zkInstance.stop();
        zkInstance.close();
	}

	
	@Test
	public void testAddUserAndGetById() throws Exception {
		TopologyConfigWithLdap config = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig("topo.properties");
		// LdapUtil specific params
		LdapParams ldapParams = config.getLdapConfig().getLdapParams();
		LdapUtil ldapUtil = new LdapUtil(ldapParams);
		// Here:
//		protected LdapParams ldapParams = ComponentFactory.getConfigurationParams().getLdapParams();


		// Unique ID generation for Party
		UniqueIDGenerator idGen = new UniqueIDGenerator();
		String partyIdToCreate = idGen.generateLocalUniqueIDStr();
		// long partyIdToCreate = 2327;
		String username = partyIdToCreate + "@grmblf.com";
		String password = "!78.@$#grr";

		DirContext addedUser = null;

		try {
			addedUser = ldapUtil.addUser(partyIdToCreate, username, password);

			LOG.info("addedUser = {}", addedUser.getNameInNamespace());
			Assert.assertEquals(String.format("cn=%s,%s", partyIdToCreate, LdapParams.USERS_GROUP_DN_DEFAULT),
					addedUser.getNameInNamespace());

			DirContext foundUser = ldapUtil.getUserById(partyIdToCreate);
			LOG.info("foundUser by getUserById = {}", foundUser.getNameInNamespace());
			Assert.assertEquals(String.format("cn=%s,%s", partyIdToCreate, LdapParams.USERS_GROUP_DN_DEFAULT),
					foundUser.getNameInNamespace());
		} finally {
			// Remove user from ldap so that there aren't tons of test users in ldap
			if (addedUser != null) {
				ldapUtil.removeUser(partyIdToCreate);
				LOG.info("removeUser = {}", partyIdToCreate);
			}
		}
	}

	// final static int CLUSTER_ID = 15;
	//
	// @Test
	// public void getClusterIDTest() throws Exception {
	// ZooKeeper zookeeper = zkInstance.getZookeeperConnection();
	// ResourceTestPoolHelper.prepareClusterID(zookeeper, "/some-path", CLUSTER_ID);
	//
	// int id = get(zookeeper, "/some-path");
	// assertThat(id, is(CLUSTER_ID));
	//
	// }
	//
	// final static String CLUSTER_ID_NODE = "/cluster-id";
	// final static int DEFAULT_CLUSTER_ID = 0;
	//
	// /**
	// * Retrieves the numeric cluster ID from the ZooKeeper quorum.
	// *
	// * @param zookeeper
	// * ZooKeeper instance to work with.
	// * @return The cluster ID, if configured in the quorum.
	// * @throws IOException
	// * Thrown when retrieving the ID fails.
	// * @throws NumberFormatException
	// * Thrown when the supposed ID found in the quorum could not be
	// * parsed as an integer.
	// */
	// public static int get(ZooKeeper zookeeper, String znode) throws IOException {
	// try {
	// Stat stat = zookeeper.exists(znode, false);
	// if (stat == null) {
	// ZooKeeperHelper.mkdirp(zookeeper, znode);
	// ZooKeeperHelper.create(zookeeper, znode + CLUSTER_ID_NODE,
	// String.valueOf(DEFAULT_CLUSTER_ID).getBytes());
	// }
	//
	// byte[] id = zookeeper.getData(znode + CLUSTER_ID_NODE, false, null);
	// return Integer.valueOf(new String(id));
	// } catch (KeeperException e) {
	// throw new IOException(String.format("Failed to retrieve the cluster ID from
	// the ZooKeeper quorum. "
	// + "Expected to find it at znode %s.", znode + CLUSTER_ID_NODE), e);
	// } catch (InterruptedException e) {
	// Thread.currentThread().interrupt();
	// throw new IOException(e);
	// }
	// }
}
