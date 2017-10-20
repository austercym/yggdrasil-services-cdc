package com.orwellg.yggdrasil.party.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.party.ID_List_Record;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.commons.types.party.Party;

public class PartyDAO {

	private final static Logger LOG = LogManager.getLogger(PartyDAO.class);
	
	protected Connection con;
	
	protected Gson gson = new Gson();

	public PartyDAO(Connection con) {
		this.con = con;
	}
	
	public PartyDAO(MariaDbManager man) throws SQLException {
		this.con = man.getConnection();
	}

	public void createParty(Party p) throws SQLException {
		LOG.debug("createParty");

		PreparedStatement st = null;
		try {

//		  `Party_ID` varchar(30) NOT NULL,
//		  `FirstName` varchar(255) DEFAULT NULL,
//		  `LastName` varchar(255) DEFAULT NULL,
//		  `BusinessName` varchar(255) DEFAULT NULL,
//		  `TradingName` varchar(255) DEFAULT NULL,
//		  `Title` int(255) DEFAULT NULL,
//		  `Salutation` varchar(255) DEFAULT NULL,
//		  `OtherNames` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
//		  `ID_List` text,
//		  `Addresses` text,
//		  `Party_InfoAssets` text,
			
			String query = "insert into Party (Party_ID,FirstName,LastName,BusinessName,TradingName"
//					+ ",Title"
					+ ",Salutation,OtherNames,ID_List,Addresses"
//					+ ",Party_InfoAssets"
					+ ") "
					+ "values (?,?,?,?,?,?,?,?,?"
//					+ ",?"
//					+ ",?"
					+ ")";
			st = con.prepareStatement(query);
			String partyId = p.getParty().getId().getId();
			st.setString(1, partyId);
			st.setString(2, p.getParty().getFirstName());
			st.setString(3, p.getParty().getLastName());
			st.setString(4, p.getParty().getBussinesName());
			st.setString(5, p.getParty().getTradingName());
//			TODO fix DB and then st.setString(6, p.getParty().getTitle());
			st.setString(6, p.getParty().getSalutation());
			st.setString(7, p.getParty().getOtherNames());
			st.setString(8, gson.toJson(p.getParty().getIDList()));
			st.setString(9, gson.toJson(p.getParty().getAddresses()));
			// XXX Party_InfoAssets
			st.execute();

			LOG.debug("Party inserted partyId = {}, partyName = {} ", partyId);
			
		} finally {
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}
		LOG.debug("END createParty");
	}

	/**@deprecated*/
	public Party getById(Long id) throws SQLException {
		return getById(id + "IPAGO");
	}
	
	public Party getById(String id) throws SQLException {
		LOG.debug("getById");
		Party p = null;
		PreparedStatement st = null;
		try {

			// Party_ID,FirstName,LastName,BusinessName,TradingName,Title,Salutation,OtherNames,ID_List,Addresses
			String query = "select * from Party where Party_ID = ?";
			st = con.prepareStatement(query);
			st.setString(1, id);
			ResultSet rs = st.executeQuery();
			// Single result
			if (rs.next()) {
				PartyType pt = new PartyType();
				p = new Party(pt);

				pt.setId(new PartyIdType(rs.getString("Party_ID")));
				pt.setFirstName(rs.getString("FirstName"));
				pt.setLastName(rs.getString("LastName"));
				pt.setBussinesName(rs.getString("BusinessName"));
				pt.setTradingName(rs.getString("TradingName"));
//				TODO fix DB and then pt.setTitle(rs.getString("Title"));
				pt.setSalutation(rs.getString("Salutation"));
				pt.setOtherNames(rs.getString("OtherNames"));
				pt.setIDList(gson.fromJson(rs.getString("ID_List"), ID_List_Record.class));
//				TODO fix avro and then pt.setAddresses(gson.fromJson(rs.getString("Addresses"), Address_Record.class));
				// XXX Party_InfoAssets

				LOG.debug("Party retrieved from db: {}", p.getParty());
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

		LOG.debug("END getParty");
		return p;
	}
}
