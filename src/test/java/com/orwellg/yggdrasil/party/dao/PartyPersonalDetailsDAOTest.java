package com.orwellg.yggdrasil.party.dao;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;

public class PartyPersonalDetailsDAOTest {

	protected MariaDbManager man;
	protected PartyPersonalDetailsDAO persDetDAO;
	protected PartyDAO partyDAO;
	protected UniqueIDGeneratorLocal idGen = new UniqueIDGeneratorLocal();
	
	@Before
	public void setUp() throws Exception{
		man = MariaDbManager.getInstance("db-local.yaml");
		persDetDAO = new PartyPersonalDetailsDAO(man.getConnection());
		partyDAO = new PartyDAO(man.getConnection());
	}
	
	@Test
	public void testCreateAndGet() throws Exception {
		String partyId = idGen.generateUniqueIDStr();
		PartyType p = Party.generatePartyTypeValidForSchema();
		p.setId(new PartyIdType(partyId));
		partyDAO.createParty(new Party(p));
		
		// PP_ID	Party_ID	EmploymentDetails	Citizenships	TaxResidency	Email	Telephone	StaffIndicator	Staff	Gender	DateOfBirth	Nationality	Tags
		PartyPersonalDetailsType det = new PartyPersonalDetailsType();
		det.setId(idGen.generateUniqueIDStr());
		det.setPartyID(partyId);
		det.setEmploymentDetails("emp det");
		det.setCitizenships("cit");
		det.setTaxResidency("taxRes");
		det.setEmail("a@b.com");
//		det.setTelephone(value);
		det.setStaffIndicator(true);
		det.setStaff("sta");
//		det.setGender("gen");
//		det.setDateOfBirth(System.currentTimeMillis());
//		det.setNationality
//		det.setTags
		
		persDetDAO.create(det);

		PartyPersonalDetailsType det2 = persDetDAO.getById(det.getId());
		Assert.assertEquals(det.getId(), det2.getId());
		Assert.assertEquals(det.getEmploymentDetails(), det2.getEmploymentDetails());
		Assert.assertEquals(det, det2);

		PartyPersonalDetailsType det3 = persDetDAO.getByPartyId(p.getId().getId());
		Assert.assertEquals(det, det3);
	}
}
