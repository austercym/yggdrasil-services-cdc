package com.orwellg.yggdrasil.party.cdc.bo;

import org.modelmapper.ModelMapper;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.repositories.scylla.PartyRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.Party;

public class CDCPartyBO {

	protected Gson gson;
	protected PartyRepository partyDao;
	protected ModelMapper modelMapper = new ModelMapper();
	
	public CDCPartyBO(Gson gson, PartyRepository partyDao) {
		this.gson = gson;
		this.partyDao = partyDao;
	}

	public CDCPartyChangeRecord parseChangeRecordJson(String json) {
		CDCPartyChangeRecord cr = gson.fromJson(json, CDCPartyChangeRecord.class);
		return cr;
	}
	
	public Party processChangeRecord(CDCPartyChangeRecord cr) {
		// Map ChangeRecord to Party entity
		Party party = modelMapper.map(cr, Party.class);
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(cr.getEventType())) {
			partyDao.insert(party);
		} else if (EVENT_TYPES.update_after.equals(cr.getEventType())) {
			partyDao.update(party);
		} else if (EVENT_TYPES.delete.equals(cr.getEventType())) {
			partyDao.delete(party);
		}
		
		return party;
	}
}
