package com.orwellg.yggdrasil.party.ldap;

import javax.naming.directory.DirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.yggdrasil.party.config.LdapParams;
import com.orwellg.yggdrasil.party.config.TopologyConfigWihLdap;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;

public class LdapUtilTest {

	// This is java-uniqueid way to access zookeeper:
//	@Rule
//	public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

	private final static Logger LOG = LogManager.getLogger(LdapUtilTest.class);

	/**
	 * Launch local zookeeper instance in test, populate it, read it in test.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testAddUserAndGetById() throws Exception {
		// This is netflix (umbrella) way to access zookeeper:
//		ZookeeperUtils.init("localhost:" + ZooKeeperInstance.DEFAULT_PORT, ConfigurationParams.ZK_PARTY_SEARCH_ROOT);
//		SubscriberKafkaConfiguration subsConfig;
//		subsConfig = SubscriberKafkaConfiguration.loadConfiguration(PartyTopology.SUBSCRIBER_LOCAL_YAML);
//		if (!ConfigurationManager.isConfigurationInstalled()) {
//			ZookeeperUtils.init(subsConfig.getZookeeper().getHost(), ConfigurationParams.ZK_PARTY_STORM_ROOT);
//		}

		// TODO maybe change this zookeeper from java-uniqueid way to netflix (umbrella) way to access to zookeeper:
		// This is java-uniqueid way to access zookeeper:
//		// Write parameter in zookeeper so that later it can be read by
//		// ComponentFactory.getConfigurationParams() in LdapUtil:
//		ZooKeeper zookeeper = zkInstance.getZookeeperConnection();
//		ResourceTestPoolHelper.prepareZkParam(zookeeper, "/com/orwellg/party-storm/ldap.url",
//				"ldap://ec2-35-176-201-54.eu-west-2.compute.amazonaws.com:389");
		// Alternative:
		// ZooKeeperHelper.mkdirp(zookeeper, znode);
		// ZooKeeperHelper.create(zookeeper, znode + CLUSTER_ID_NODE,
		// String.valueOf(DEFAULT_CLUSTER_ID).getBytes());

		// This tries to connect to zookeeper and get
		// "/com/orwellg/party-storm/ldap.url" property among others:
		// Get (and init if they weren't before) party-storm application-wide params. Tries to connect to zookeeper:
		TopologyConfigWihLdap config = TopologyConfigWithLdapFactory.getTopologyConfig();
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
