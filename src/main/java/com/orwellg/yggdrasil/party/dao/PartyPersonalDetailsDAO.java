package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;

public class PartyPersonalDetailsDAO {

	private final static Logger LOG = LogManager.getLogger(PartyPersonalDetailsDAO.class);

	protected Connection con;

	public PartyPersonalDetailsDAO(Connection con) {
		this.con = con;
	}

	public void create(PartyPersonalDetailsType p) throws SQLException {
		PreparedStatement st = null;
		try {
			// `PP_ID` varchar(30) NOT NULL,
			// `Party_ID` varchar(30) NOT NULL,
			// `EmploymentDetails` text DEFAULT NULL,
			// `Citizenships` text DEFAULT NULL,
			// `TaxResidency` text DEFAULT NULL,
			// `Email` text DEFAULT NULL,
			// `Telephone` text DEFAULT NULL,
			// `StaffIndicator` tinyint(1) DEFAULT NULL,
			// `Staff` text DEFAULT NULL,
			// `Gender` int(255) DEFAULT NULL,
			// `DateOfBirth` datetime DEFAULT NULL,
			// `Nationality` varchar(255) DEFAULT NULL,
			// `Tags` text DEFAULT 'NULL',

			// TODO map all fields:
			String query = "insert into PartyPersonalDetails ("
					+ "PP_ID, Party_ID, EmploymentDetails, Citizenships, TaxResidency, Email"
//					+ ", Telephone"
					+ ", StaffIndicator, Staff"
//					+ ", Gender"
//					+ ", DateOfBirth"
//					+ ", Nationality, Tags"
					+ ") "
					+ " values (?,?,?,?,?,?,?,?"
//					+ ",?,?,?,?,?"
					+ ")";
			st = con.prepareStatement(query);
			st.setString(1, p.getId());
			st.setString(2, p.getPartyID());
			st.setString(3, p.getEmploymentDetails());
			st.setString(4, p.getCitizenships());
			st.setString(5, p.getTaxResidency());
			st.setString(6, p.getEmail());
//			st.setString(7, p.getTelephone());
			Boolean staffIndicator = p.getStaffIndicator();
			st.setInt(7, (staffIndicator != null && staffIndicator == Boolean.TRUE ? 1 : 0));
			st.setString(8, p.getStaff());
//			TODO fix DB then st.setString(9, p.getGender());
//			TODO fix DB and avro then st.setDate(9, new Date(p.getDateOfBirth()));

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

	public PartyPersonalDetailsType getById(String id) throws SQLException {
		PartyPersonalDetailsType p = null;
		PreparedStatement st = null;
		try {

			String query = "select * from PartyPersonalDetails where PP_ID = ?";
			st = con.prepareStatement(query);
			st.setString(1, id);
			ResultSet rs = st.executeQuery();
			// Single result
			if (rs.next()) {
				p = entityFromResultset(rs);

				LOG.debug("PartyPersonalDetails retrieved from db: {}", p);
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

	protected PartyPersonalDetailsType entityFromResultset(ResultSet rs) throws SQLException {
		PartyPersonalDetailsType p;
		p = new PartyPersonalDetailsType();

		// TODO Map all attributes
		// PP_ID, Party_ID, EmploymentDetails, Citizenships, TaxResidency, Email, Telephone, StaffIndicator, Staff, Gender, DateOfBirth, Nationality, Tags
		p.setId(rs.getString("PP_ID"));
		p.setPartyID(rs.getString("Party_ID"));
		p.setEmploymentDetails(rs.getString("EmploymentDetails"));
		p.setCitizenships(rs.getString("Citizenships"));
		p.setTaxResidency(rs.getString("TaxResidency"));
		p.setEmail(rs.getString("Email"));
//				p.setTelephone(rs.getString("Telephone"));
		p.setStaffIndicator((rs.getInt("StaffIndicator") == 1 ? true : false));
		p.setStaff(rs.getString("Staff"));
		p.setGender(rs.getString("Gender"));
//				Date date = rs.getDate("DateOfBirth");
//				p.setDateOfBirth((date != null ? date.getTime() : null));
//				p.setNationality
//				p.setTags
		return p;
	}

	public PartyPersonalDetailsType getByPartyId(String partyId) throws SQLException {
		PartyPersonalDetailsType p = null;
		PreparedStatement st = null;
		try {

			String query = "select * from PartyPersonalDetails where Party_ID = ?";
			st = con.prepareStatement(query);
			st.setString(1, partyId);
			ResultSet rs = st.executeQuery();
			// Single result
			if (rs.next()) {
				p = entityFromResultset(rs);

				LOG.debug("PartyPersonalDetails retrieved from db: {}", p);
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
