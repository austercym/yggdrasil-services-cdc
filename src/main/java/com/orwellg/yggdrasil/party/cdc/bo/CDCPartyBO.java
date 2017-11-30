package com.orwellg.yggdrasil.party.cdc.bo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.repositories.scylla.PartyRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.Party;
import com.orwellg.umbrella.commons.types.utils.mapper.CDCChangeRecordToEntityMapper;

/**
 * Business logic for Party CDC.
 * @author c.friaszapater
 *
 */
public class CDCPartyBO {

	protected Gson gson;
	protected PartyRepository partyDao;
	protected CDCChangeRecordToEntityMapper objectMapper = new CDCChangeRecordToEntityMapper();
	
	protected Logger LOG = LogManager.getLogger(CDCPartyBO.class);
	
	public CDCPartyBO(Gson gson, PartyRepository partyDao) {
		this.gson = gson;
		this.partyDao = partyDao;
	}

	/**
	 * Insert/update/delete entity from PartyRepository, corresponding to the changeRecord. 
	 * @param changeRecord includes entity data.
	 * @return inserted/updated/deleted scylla entity.
	 */
	public Party processChangeRecord(CDCPartyChangeRecord changeRecord) {
		// Map ChangeRecord to Party entity
		Party party = objectMapper.map(changeRecord, Party.class);
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(changeRecord.getEventType())) {
			partyDao.insert(party);
			LOG.info("Party inserted in scylla = {} for changeRecord event = {}", party, changeRecord);
		} else if (EVENT_TYPES.update_after.equals(changeRecord.getEventType())) {
			partyDao.update(party);
			LOG.info("Party updated in scylla = {} for changeRecord event = {}", party, changeRecord);
		} else if (EVENT_TYPES.delete.equals(changeRecord.getEventType())) {
			partyDao.delete(party);
			LOG.info("Party deleted in scylla = {} for changeRecord event = {}", party, changeRecord);
		} else {
			LOG.info("Nothing done for changeRecord event = {}", changeRecord);
			return null;
		}
		return party;
	}
}
