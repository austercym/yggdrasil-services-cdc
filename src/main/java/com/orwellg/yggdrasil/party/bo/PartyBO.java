package com.orwellg.yggdrasil.party.bo;

import java.sql.Connection;
import java.sql.SQLException;

import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.yggdrasil.party.dao.PartyDAO;
import com.orwellg.yggdrasil.party.dao.PartyNonPersonalDetailsDAO;
import com.orwellg.yggdrasil.party.dao.PartyPersonalDetailsDAO;

/**
 * Business logic associated with Party domain object.
 * @author c.friaszapater
 *
 */
public class PartyBO {

	protected PartyDAO partyDAO = null;
	protected PartyPersonalDetailsDAO personalDetailsDAO = null;
	protected PartyNonPersonalDetailsDAO nonPersonalDetailsDAO = null;

	protected Connection con;

	public PartyBO(Connection con) {
		this.con = con;
		partyDAO = new PartyDAO(con);
		personalDetailsDAO = new PartyPersonalDetailsDAO(con);
		nonPersonalDetailsDAO = new PartyNonPersonalDetailsDAO(con);
	}
	
	/**
	 * Save Party, PersonalDetails if exists, NonPersonalDetails if exists.
	 * @param p
	 * @throws SQLException
	 */
	public void insertParty(Party p) throws SQLException {
		// Save Party
		partyDAO.createParty(p);
		
		// Save PersonalDetails if exists
		PartyPersonalDetailsType persDet = p.getParty().getPersonalDetails();
		if (persDet != null) {
			if (persDet.getPartyID() == null && p.getParty().getId() != null) {
				persDet.setPartyID(p.getParty().getId().getId());
			}
			personalDetailsDAO.create(persDet);
		}

		// Save NonPersonalDetails if exists
		PartyNonPersonalDetailsType nonPersDet = p.getParty().getNonPersonalDetails();
		if (nonPersDet != null) {
			if (nonPersDet.getPartyID() == null && p.getParty().getId() != null) {
				nonPersDet.setPartyID(p.getParty().getId().getId());
			}
			nonPersonalDetailsDAO.create(nonPersDet);
		}
	}

	/**
	 * getById() that gets also Personal/NonPersonal Details if exist.
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public Party getById(String id) throws SQLException {
		Party p = partyDAO.getById(id);
		
		PartyPersonalDetailsType persDet = personalDetailsDAO.getByPartyId(id);
		if (persDet != null) {
			p.getParty().setPersonalDetails(persDet);
		}

		// TODO Get NonPersonalDetails if exists
		PartyNonPersonalDetailsType nonPersDet = nonPersonalDetailsDAO.getByPartyId(id);
		if (nonPersDet != null) {
			p.getParty().setNonPersonalDetails(nonPersDet);
		}
		
		return p;
	}
}
