package com.orwellg.yggdrasil.party.bo;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;

public class PartyBOTest {

	MariaDbManager man;
	PartyBO partyBO;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		man = MariaDbManager.getInstance("db-local.yaml");
		partyBO = new PartyBO(man.getConnection());
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
