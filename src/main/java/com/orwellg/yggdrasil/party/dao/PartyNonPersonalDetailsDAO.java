package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.party.ActivityCodeList;
import com.orwellg.umbrella.avro.types.party.PartyNonPersonalDetailsType;

public class PartyNonPersonalDetailsDAO {

	private final static Logger LOG = LogManager.getLogger(PartyNonPersonalDetailsDAO.class);

	protected Connection con;
	
	protected Gson gson = new Gson();

	public PartyNonPersonalDetailsDAO(Connection con) {
		this.con = con;
	}

	public void create(PartyNonPersonalDetailsType p) throws SQLException {
		PreparedStatement st = null;
		try {

			// TODO map all fields:
			String query = "insert into PartyNonPersonalDetails ("
					+ "NPP_ID, Party_ID, SICCodes, BusinessRegistrationCountry"
					+ ") "
					+ " values (?,?,?,?"
					+ ")";
			st = con.prepareStatement(query);
			st.setString(1, p.getId());
			st.setString(2, p.getPartyID());
			st.setString(3, gson.toJson(p.getSICCodes()));
			st.setString(4, p.getBusinessRegistrationCountry());

			st.execute();

			LOG.debug("PartyPersonalDetails inserted PP_ID = {}, Party_ID = {} ", p.getId(), p.getPartyID());

		} finally {
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}
	}

	public PartyNonPersonalDetailsType getById(String id) throws SQLException {
		PartyNonPersonalDetailsType p = null;
		PreparedStatement st = null;
		try {

			String query = "select * from PartyNonPersonalDetails where NPP_ID = ?";
			st = con.prepareStatement(query);
			st.setString(1, id);
			ResultSet rs = st.executeQuery();
			// Single result
			if (rs.next()) {
				p = entityFromResultset(rs);

				LOG.debug("PartyNonPersonalDetails retrieved from db: {}", p);
			}

		} finally {
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}

		return p;
	}

	protected PartyNonPersonalDetailsType entityFromResultset(ResultSet rs) throws SQLException {
		PartyNonPersonalDetailsType p;
		p = new PartyNonPersonalDetailsType();

		// TODO Map all attributes
		p.setId(rs.getString("NPP_ID"));
		p.setPartyID(rs.getString("Party_ID"));
		p.setSICCodes(gson.fromJson(rs.getString("SICCodes"), ActivityCodeList.class));
		p.setBusinessRegistrationCountry(rs.getString("BusinessRegistrationCountry"));
		return p;
	}

	public PartyNonPersonalDetailsType getByPartyId(String partyId) throws SQLException {
		PartyNonPersonalDetailsType p = null;
		PreparedStatement st = null;
		try {

			String query = "select * from PartyNonPersonalDetails where Party_ID = ?";
			st = con.prepareStatement(query);
			st.setString(1, partyId);
			ResultSet rs = st.executeQuery();
			// Single result
			if (rs.next()) {
				p = entityFromResultset(rs);

				LOG.debug("PartyNonPersonalDetails retrieved from db: {}", p);
			}

		} finally {
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}

		return p;
	}
}
