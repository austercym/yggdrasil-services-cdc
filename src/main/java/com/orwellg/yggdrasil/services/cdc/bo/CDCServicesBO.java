package com.orwellg.yggdrasil.services.cdc.bo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCServicesChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.repositories.scylla.ServicesRepository;
import com.orwellg.umbrella.commons.types.scylla.entities.Services;
import com.orwellg.umbrella.commons.types.utils.mapper.CDCChangeRecordToEntityMapper;

/**
 * Business logic for Services CDC.
 * @author c.friaszapater
 *
 */
public class CDCServicesBO {

	protected Gson gson;
	protected ServicesRepository servicesDao;
	protected CDCChangeRecordToEntityMapper objectMapper = new CDCChangeRecordToEntityMapper();
	
	protected Logger LOG = LogManager.getLogger(CDCServicesBO.class);
	
	public CDCServicesBO(Gson gson, ServicesRepository servicesDao) {
		this.gson = gson;
		this.servicesDao = servicesDao;
	}

	/**
	 * Insert/update/delete entity from ServicesRepository, corresponding to the changeRecord. 
	 * @param changeRecord includes entity data.
	 * @return inserted/updated/deleted scylla entity.
	 */
	public Services processChangeRecord(CDCServicesChangeRecord changeRecord) {
		// Map ChangeRecord to Services entity
		Services services = objectMapper.map(changeRecord, Services.class);
	
		// Execute insert/update/delete
		if (EVENT_TYPES.insert.equals(changeRecord.getEventType())) {
			servicesDao.insert(services);
			LOG.info("Services inserted in scylla = {} for changeRecord event = {}", services, changeRecord);
		} else if (EVENT_TYPES.update_after.equals(changeRecord.getEventType())) {
			servicesDao.update(services);
			LOG.info("Services updated in scylla = {} for changeRecord event = {}", services, changeRecord);
		} else if (EVENT_TYPES.delete.equals(changeRecord.getEventType())) {
			servicesDao.delete(services);
			LOG.info("Services deleted in scylla = {} for changeRecord event = {}", services, changeRecord);
		} else if (EVENT_TYPES.update_before.equals(changeRecord.getEventType())) {
			LOG.debug("Nothing done for update_before changeRecord event = {}", changeRecord);
			return null;
		} else {
			LOG.info("Nothing done for changeRecord event = {}", changeRecord);
			return null;
		}
		return services;
	}
}
