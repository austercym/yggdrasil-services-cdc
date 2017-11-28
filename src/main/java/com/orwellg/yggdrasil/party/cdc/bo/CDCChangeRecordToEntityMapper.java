package com.orwellg.yggdrasil.party.cdc.bo;

import org.modelmapper.ModelMapper;

/**
 * Mapper from any CDCChangeRecord to any scylla entity
 * @author c.friaszapater
 *
 */
public class CDCChangeRecordToEntityMapper {

	protected ModelMapper modelMapper = new ModelMapper();
	
	public <T,D> D map(T changeRecord, Class<D> entity) {
		return modelMapper.map(changeRecord, entity);
	}

}
