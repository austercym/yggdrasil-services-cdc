package com.orwellg.yggdrasil.party.create.topology.bolts;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;
import com.orwellg.yggdrasil.party.dao.PartyDAO;
import com.orwellg.yggdrasil.party.dao.PartyPersonalDetailsDAO;

public class CreatePartyBoltTest {

	// Local idgen not to need zookeeper connection
	UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
	CreatePartyBolt bolt = new CreatePartyBolt();
	PartyDAO partyDao;
	PartyPersonalDetailsDAO personalDetailsDao;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		partyDao = new PartyDAO(MariaDbManager.getInstance().getConnection());
		personalDetailsDao = new PartyPersonalDetailsDAO(MariaDbManager.getInstance().getConnection());
	}

	@After
	public void tearDown() throws Exception {
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
		detT.setEmploymentDetails("employment details");
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
