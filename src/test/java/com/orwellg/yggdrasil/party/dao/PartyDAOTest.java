package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.commons.IDList;
import com.orwellg.umbrella.avro.types.party.ID_List_Record;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.yggdrasil.h2.H2DbHelper;

public class PartyDAOTest {

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";
	
	protected PartyDAO partyDAO;
	
	@Before
	public void setUp() throws Throwable {
		// Start H2 db server in-memory
		Connection connection = DriverManager.getConnection(JDBC_CONN);
		
		// Create schema
		H2DbHelper h2 = new H2DbHelper();
		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
		
		Statement s = connection.createStatement();
		s.executeQuery("SELECT * FROM Party;");

		partyDAO = new PartyDAO(connection);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateParty() throws Exception {
		PartyType partyType = new PartyType();

		UniqueIDGenerator idGen = new UniqueIDGenerator();
		String partyIdToCreate = idGen.generateLocalUniqueIDStr();

		// Party_ID,FirstName,LastName,BusinessName,TradingName,Title,Salutation,OtherNames,ID_List,Addresses,Party_InfoAssets
		partyType.setId(new PartyIdType(partyIdToCreate));
		partyType.setFirstName("PartyChunga1");
		partyType.setLastName("last");
		partyType.setBussinesName("bus");
		partyType.setTradingName("tra");
//		TODO fix DB and then partyType.setTitle("tit");
		partyType.setSalutation("sal");
		partyType.setOtherNames("oth");
		IDList idList = new IDList();
		idList.setEmail("a@b.co");
		idList.setCompanyIdentifier("companyId");
		partyType.setIDList(new ID_List_Record(idList));
//		TODO fix avro and then pt.setAddresses(gson.fromJson(rs.getString("Addresses"), Address_Record.class));
		Party p = new Party(partyType);
		partyDAO.createParty(p);

		Party p2 = partyDAO.getById(partyIdToCreate);
		Assert.assertEquals(p.getParty().getId().getId(), p2.getParty().getId().getId());
		Assert.assertEquals(p.getParty().getFirstName(), p2.getParty().getFirstName());
		Assert.assertEquals(p.getParty(), p2.getParty());
	}

}
