package com.orwellg.yggdrasil.party.dao;

import org.junit.Assert;
import org.junit.Test;

import com.orwellg.umbrella.avro.types.commons.IDList;
import com.orwellg.umbrella.avro.types.party.ID_List_Record;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;

public class PartyDAOTest {

	@Test
	public void testCreateParty() throws Exception {
		MariaDbManager man = MariaDbManager.getInstance("db-local.yaml");
		PartyDAO partyDAO = new PartyDAO(man.getConnection());
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
