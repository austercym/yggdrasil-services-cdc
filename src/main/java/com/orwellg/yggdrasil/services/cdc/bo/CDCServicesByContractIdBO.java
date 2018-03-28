package com.orwellg.yggdrasil.services.cdc.bo;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCServicesChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.repositories.scylla.ServicesByContractIdRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.ServicesByContractId;
import com.orwellg.umbrella.commons.types.utils.mapper.CDCChangeRecordToEntityMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Business logic for Services CDC.
 * @author c.friaszapater
 *
 */
public class CDCServicesByContractIdBO {

	protected Gson gson;
	protected ServicesByContractIdRepository servicesByContractIdDao;
	protected CDCChangeRecordToEntityMapper objectMapper = new CDCChangeRecordToEntityMapper();

	protected Logger LOG = LogManager.getLogger(CDCServicesByContractIdBO.class);

	public CDCServicesByContractIdBO(Gson gson, ServicesByContractIdRepository servicesByContractIdDao) {
		this.gson = gson;
		this.servicesByContractIdDao = servicesByContractIdDao;
	}

	/**
	 * Insert/update/delete entity from ServicesByContractIdRepository, corresponding to the changeRecord. 
	 * @param changeRecord includes entity data.
	 * @return inserted/updated/deleted scylla entity.
	 */
	public ServicesByContractId processChangeRecord(CDCServicesChangeRecord changeRecord) {
		
		// Map ChangeRecord to ServicesByContractId entity
		ServicesByContractId servicesByContractId = objectMapper.map(changeRecord, ServicesByContractId.class);
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(changeRecord.getEventType())) {
			servicesByContractIdDao.insert(servicesByContractId);
			LOG.info("ServicesByContractId inserted in scylla = {} for changeRecord event = {}", servicesByContractId, changeRecord);
		} else if (EVENT_TYPES.update_after.equals(changeRecord.getEventType())) {
			servicesByContractIdDao.insert(servicesByContractId);
			LOG.info("ServicesByContractId inserted in scylla = {} for changeRecord event = {}", servicesByContractId, changeRecord);
		} else if (EVENT_TYPES.delete.equals(changeRecord.getEventType())) {
			servicesByContractIdDao.delete(servicesByContractId);
			LOG.info("ServicesByContractId deleted in scylla = {} for changeRecord event = {}", servicesByContractId, changeRecord);
		} else if (EVENT_TYPES.update_before.equals(changeRecord.getEventType())) {
			servicesByContractIdDao.delete(servicesByContractId);
			LOG.info("ServicesByContractId deleted in scylla = {} for changeRecord event = {}", servicesByContractId, changeRecord);
		} else {
			LOG.info("Nothing done for changeRecord event = {}", changeRecord);
			return null;
		}
		return servicesByContractId;
	}
}
