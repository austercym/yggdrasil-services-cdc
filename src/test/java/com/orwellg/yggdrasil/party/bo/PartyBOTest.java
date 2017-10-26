package com.orwellg.yggdrasil.party.bo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;

public class PartyBOTest {

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";
	
	protected PartyBO partyBO;

	@Before
	public void setUp() throws Throwable {
		// Start H2 db server in-memory
		Connection connection = DriverManager.getConnection(JDBC_CONN);
		
		// Create schema
		H2DbHelper h2 = new H2DbHelper();
		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
		
		Statement s = connection.createStatement();
		s.executeQuery("SELECT * FROM Party;");
		
		partyBO = new PartyBO(connection);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		PartyType partyType = new PartyType();

		UniqueIDGenerator idGen = new UniqueIDGenerator();
		String partyIdToCreate = idGen.generateLocalUniqueIDStr();

		partyType.setId(new PartyIdType(partyIdToCreate));
		partyType.setFirstName("PartyChunga1");
		
		PartyNonPersonalDetailsType nonPersDet = new PartyNonPersonalDetailsType();
		nonPersDet.setId(idGen.generateLocalUniqueIDStr());
		nonPersDet.setBusinessRegistrationCountry("UK");
		
		partyType.setNonPersonalDetails(nonPersDet);
		
		Party p = new Party(partyType);
		partyBO.insertParty(p);

		Party p2 = partyBO.getById(partyIdToCreate);
		Assert.assertEquals(p.getParty().getId().getId(), p2.getParty().getId().getId());
		Assert.assertEquals(p.getParty().getFirstName(), p2.getParty().getFirstName());
		Assert.assertEquals(nonPersDet, p.getParty().getNonPersonalDetails());
	}

}
