package com.orwellg.yggdrasil.party.dao;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.ActivityCode;
import com.orwellg.umbrella.avro.types.party.ActivityCodeList;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;

public class PartyNonPersonalDetailsDAOTest {

	protected MariaDbManager man;
	protected PartyNonPersonalDetailsDAO nonPersDetDAO;
	protected PartyDAO partyDAO;
	protected UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
	
	@Before
	public void setUp() throws Exception{
		man = MariaDbManager.getInstance("db-local.yaml");
		nonPersDetDAO = new PartyNonPersonalDetailsDAO(man.getConnection());
		partyDAO = new PartyDAO(man.getConnection());
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
