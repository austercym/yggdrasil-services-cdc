package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.orwellg.umbrella.commons.config.params.MariaDBParams;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;

/**
 * Very simple MariaDB connection manager TODO to be replaced.
 * @author c.friaszapater
 *
 */
public class MariaDbManager {

	public final static Logger LOG = LogManager.getLogger(MariaDbManager.class);

	protected static MariaDbManager instance;

	private static String url;
	private static String user;
	private static String pwd;
	protected Connection con;
	
	/**
	 * Usual getInstance unless specific topologyPropertiesFile is needed.
	 * TopologyConfig.DEFAULT_PROPERTIES_FILE properties file will be used.
	 */
	public static MariaDbManager getInstance() throws SQLException {
		return getInstance(null);
	}
	
	/**
	 * getInstance with specific topologyPropertiesFile (eg: for tests).
	 * @param propertiesFile TopologyConfig properties file. Can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
	 */
	public static MariaDbManager getInstance(String topologyPropertiesFile) throws SQLException {
		if (instance == null) {
			MariaDBParams dbPar = TopologyConfigFactory.getTopologyConfig(topologyPropertiesFile).getMariaDBConfig().getMariaDBParams();
			// jdbc:mysql://host:port/name
			url = String.format("jdbc:mysql://%s:%s/%s", dbPar.getDbName(), dbPar.getHost(), dbPar.getDbName());
			user = dbPar.getUser();
			pwd = dbPar.getPassword();
			
			LOG.info("MariaDbManager.initInstance with {} {} {}", url, user, pwd);
			
			instance = new MariaDbManager();
			
			setUpConnection(url, user, pwd);
		}
			
		return instance;
	}
	
	protected static void setUpConnection(String url, String user, String pwd) throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", pwd);
		instance.con = DriverManager.getConnection(url,connectionProps);
	}

	public Connection getConnection() throws SQLException {
		if (con == null || !con.isValid(1)) {
			setUpConnection(url, user, pwd);
		}
		return con;
	}

}
