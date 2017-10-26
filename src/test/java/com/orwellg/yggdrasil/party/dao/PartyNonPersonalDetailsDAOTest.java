package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.ActivityCode;
import com.orwellg.umbrella.avro.types.party.ActivityCodeList;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;

public class PartyNonPersonalDetailsDAOTest {

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";
	
	protected PartyNonPersonalDetailsDAO nonPersDetDAO;
	protected PartyDAO partyDAO;
	protected UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
	
	@Before
	public void setUp() throws Exception{
		// Start H2 db server in-memory
		Connection connection = DriverManager.getConnection(JDBC_CONN);
		
		// Create schema
		H2DbHelper h2 = new H2DbHelper();
		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
		
		Statement s = connection.createStatement();
		s.executeQuery("SELECT * FROM Party;");

		partyDAO = new PartyDAO(connection);
		nonPersDetDAO = new PartyNonPersonalDetailsDAO(connection);
	}
	
	@Test
	public void testCreateAndGet() throws Exception {
		String partyId = idGen.generateUniqueIDStr();
		PartyType p = Party.generatePartyTypeValidForSchema();
		p.setId(new PartyIdType(partyId));
		partyDAO.createParty(new Party(p));
		
		// NPP_ID	Party_ID	SICCodes	BusinessRegistrationCountry
		PartyNonPersonalDetailsType det = new PartyNonPersonalDetailsType();
		det.setId(idGen.generateUniqueIDStr());
		det.setPartyID(partyId);
		ActivityCodeList codes = new ActivityCodeList();
		codes.setActivityCodes(Arrays.asList(new ActivityCode("code", "Description")));
		det.setSICCodes(codes);
		det.setBusinessRegistrationCountry("ES");
		
		nonPersDetDAO.create(det);

		PartyNonPersonalDetailsType det2 = nonPersDetDAO.getById(det.getId());
		Assert.assertEquals(det.getId(), det2.getId());
		Assert.assertEquals(det, det2);
	}

	@Test
	public void testGetByPartyId() throws Exception {
		String partyId = idGen.generateUniqueIDStr();
		PartyType p = Party.generatePartyTypeValidForSchema();
		p.setId(new PartyIdType(partyId));
		partyDAO.createParty(new Party(p));
		
		PartyNonPersonalDetailsType det = new PartyNonPersonalDetailsType();
		det.setId(idGen.generateUniqueIDStr());
		det.setPartyID(partyId);
		det.setBusinessRegistrationCountry("ES");
		
		nonPersDetDAO.create(det);

		PartyNonPersonalDetailsType det2 = nonPersDetDAO.getByPartyId(partyId);
		Assert.assertEquals(det.getId(), det2.getId());
		Assert.assertEquals(det, det2);
	}
}
