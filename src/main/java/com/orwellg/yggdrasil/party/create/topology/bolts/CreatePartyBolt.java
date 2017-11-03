package com.orwellg.yggdrasil.party.create.topology.bolts;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.orwellg.umbrella.commons.repositories.mariadb.impl.PartyBO;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;

/**
 * Process PartyEvents.CREATE_PARTY event with Party parameter: Insert Party in
 * relational database.<br/>
 * Input tuple: key, processId, eventName, data (Party)<br/>
 * Emit tuple: key, processId, result (Party)<br/>
 * XXX create subclasses for each Party actions, make this class abstract
 * PartyActionBolt.
 * 
 * @author c.friaszapater
 *
 */
public class CreatePartyBolt extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(CreatePartyBolt.class);

	private String logPreffix;

	protected PartyBO partyBO = null;

	private String topologyPropertiesFile;

	/**
	 * Usual constructor unless specific topologyPropertiesFile is needed.
	 * TopologyConfig.DEFAULT_PROPERTIES_FILE properties file will be used.
	 */
	public CreatePartyBolt() {
		topologyPropertiesFile = null;
	}

	/**
	 * Constructor with specific topologyPropertiesFile (eg: for tests).
	 * @param propertiesFile TopologyConfig properties file. Can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
	 */
	public CreatePartyBolt(String topologyPropertiesFile) {
		this.topologyPropertiesFile = topologyPropertiesFile;
	}
	
	protected String getLogPreffix() {
		return logPreffix;
	}

	@Override
	public void execute(Tuple input) {

		LOG.info("Tuple input = {}", input);

		// Received tuple is: key, processId, eventName, data

		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		String eventName = (String) input.getValueByField("eventName");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		Party p = null;
		try {
			p = (Party) input.getValueByField("eventData");

			LOG.info("{}Action {} starting for party {}.", getLogPreffix(), eventName, p);

			// Insert Party in mariadb relational database
			insertParty(p);

			// Result for next bolt: Party
			Party result = p;

			LOG.info("{}Action {} for party {} finished. Sending the results {}.", getLogPreffix(), eventName, p,
					result);

			getCollector().emit(input, new Values(key, processId, result));
			getCollector().ack(input);

			LOG.info("{}Action {} for party {} finished. Results sent successfully.", getLogPreffix(), eventName, p);
		} catch (Exception e) {
			LOG.error("{}Error in Action {} for party {}. Message: {}", getLogPreffix(), eventName, p, e.getMessage(),
					e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "result"));
	}

	@Override
	public void declareFieldsDefinition() {
	}

	public void insertParty(Party p) throws SQLException {
		// if (partyBO == null) {
		// TODO db connection pool
		MariaDbManager man = MariaDbManager.getInstance(topologyPropertiesFile);
		partyBO = new PartyBO(man.getConnection());
		// }

		partyBO.insertParty(p);
	}

}
