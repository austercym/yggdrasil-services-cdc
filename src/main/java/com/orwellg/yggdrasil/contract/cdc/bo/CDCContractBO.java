package com.orwellg.yggdrasil.contract.cdc.bo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.repositories.scylla.ContractRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.Contract;
import com.orwellg.umbrella.commons.types.utils.mapper.CDCChangeRecordToEntityMapper;

/**
 * Business logic for Contract CDC.
 * @author c.friaszapater
 *
 */
public class CDCContractBO {

	protected Gson gson;
	protected ContractRepository contractDao;
	protected CDCChangeRecordToEntityMapper objectMapper = new CDCChangeRecordToEntityMapper();
	
	protected Logger LOG = LogManager.getLogger(CDCContractBO.class);
	
	public CDCContractBO(Gson gson, ContractRepository contractDao) {
		this.gson = gson;
		this.contractDao = contractDao;
	}

	/**
	 * Insert/update/delete entity from ContractRepository, corresponding to the changeRecord. 
	 * @param changeRecord includes entity data.
	 * @return inserted/updated/deleted scylla entity.
	 */
	public Contract processChangeRecord(CDCContractChangeRecord changeRecord) {
		// Map ChangeRecord to Contract entity
		Contract contract = objectMapper.map(changeRecord, Contract.class);
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(changeRecord.getEventType())) {
			contractDao.insert(contract);
			LOG.info("Contract inserted in scylla = {} for changeRecord event = {}", contract, changeRecord);
		} else if (EVENT_TYPES.update_after.equals(changeRecord.getEventType())) {
			contractDao.update(contract);
			LOG.info("Contract updated in scylla = {} for changeRecord event = {}", contract, changeRecord);
		} else if (EVENT_TYPES.delete.equals(changeRecord.getEventType())) {
			contractDao.delete(contract);
			LOG.info("Contract deleted in scylla = {} for changeRecord event = {}", contract, changeRecord);
		} else if (EVENT_TYPES.update_before.equals(changeRecord.getEventType())) {
			LOG.debug("Nothing done for update_before changeRecord event = {}", changeRecord);
			return null;
		} else {
			LOG.info("Nothing done for changeRecord event = {}", changeRecord);
			return null;
		}
		return contract;
	}
}
