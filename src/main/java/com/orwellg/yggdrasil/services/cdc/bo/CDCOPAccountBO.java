package com.orwellg.yggdrasil.services.cdc.bo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCServicesChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.avro.types.product.ExternalIDServiceType;
import com.orwellg.umbrella.commons.repositories.scylla.OBClientIdAccountRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.openbanking.OBClientIdAccount;
import com.orwellg.umbrella.commons.types.utils.mapper.CDCChangeRecordToEntityMapper;

/**
 * Business logic for Services CDC.
 * @author c.friaszapater
 *
 */
public class CDCOPAccountBO {

	protected Gson gson;
	protected OBClientIdAccountRepository oBClientIdAccountRepository;
	protected CDCChangeRecordToEntityMapper objectMapper = new CDCChangeRecordToEntityMapper();
	
	protected Logger LOG = LogManager.getLogger(CDCOPAccountBO.class);
	
	public CDCOPAccountBO(Gson gson, OBClientIdAccountRepository oBClientIdAccountRepository) {
		this.gson = gson;
		this.oBClientIdAccountRepository = oBClientIdAccountRepository;
	}

	/**
	 * Insert/update/delete entity from ServicesRepository, corresponding to the changeRecord. 
	 * @param changeRecord includes entity data.
	 * @return inserted/updated/deleted scylla entity.
	 */
	public OBClientIdAccount processChangeRecord(CDCServicesChangeRecord changeRecord) {
		
		// Map ChangeRecord to Services entity
		OBClientIdAccount oBClientIdAccount = new OBClientIdAccount();
		
		ExternalIDServiceType externalid = gson.fromJson(changeRecord.getExternalIDs(), ExternalIDServiceType.class);
		
		oBClientIdAccount.setObAccount(externalid.getOBClientID());
		oBClientIdAccount.setInternalAccountId(changeRecord.getServiceID());
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(changeRecord.getEventType())) {
			oBClientIdAccountRepository.create(oBClientIdAccount);
			LOG.info("Services inserted in scylla = {} for changeRecord event = {}", oBClientIdAccount.toString(), changeRecord);
		} else if (EVENT_TYPES.update_after.equals(changeRecord.getEventType())) {
			oBClientIdAccountRepository.update(oBClientIdAccount);
			LOG.info("Services updated in scylla = {} for changeRecord event = {}", oBClientIdAccount.toString(), changeRecord);
		} else if (EVENT_TYPES.delete.equals(changeRecord.getEventType())) {
			oBClientIdAccountRepository.delete(oBClientIdAccount);
			LOG.info("Services deleted in scylla = {} for changeRecord event = {}", oBClientIdAccount.toString(), changeRecord);
		} else if (EVENT_TYPES.update_before.equals(changeRecord.getEventType())) {
			LOG.debug("Nothing done for update_before changeRecord event = {}", changeRecord);
			return null;
		} else {
			LOG.info("Nothing done for changeRecord event = {}", changeRecord);
			return null;
		}
		return oBClientIdAccount;
	}
}
