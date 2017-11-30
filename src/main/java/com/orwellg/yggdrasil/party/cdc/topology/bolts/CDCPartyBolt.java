package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.impl.PartyRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;

/**
 * Bolt to process Party CDC actions received by topic.
 * 
 * @author c.friaszapater
 *
 */
public class CDCPartyBolt extends BasicRichBolt {

	private static final long serialVersionUID = 1L;

	protected static final Logger LOG = LogManager.getLogger(CDCPartyBolt.class);

	protected Gson gson;

	protected CDCPartyBO cdcPartyBo;
	protected Session session;

	private String logPreffix;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
		addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
		buildCdcPartyBo();
	}

	private void buildCdcPartyBo() {
		if (session == null || session.isClosed()) {
			ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
			session = ScyllaManager.getInstance(scyllaParams.getNodeList()).getSession(scyllaParams.getKeyspace());
			PartyRepositoryImpl partyDao = new PartyRepositoryImpl(session);
			cdcPartyBo = new CDCPartyBO(gson, partyDao);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * Method that decode the receive event coming from Kafka and send it to the next step in the topology
	 */
	@Override
	public void execute(Tuple input) {		

		LOG.debug("CDC ChangeRecord received: {}. Starting the execution process.", input);

		buildCdcPartyBo();
		
		// Received tuple is: key, processId, eventName, data

		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		String eventName = (String) input.getValueByField("eventName");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		CDCPartyChangeRecord cr = null;
		try {
			cr = (CDCPartyChangeRecord) input.getValueByField("eventData");

			LOG.debug("{}Action {} starting for changeRecord {}.", logPreffix, eventName, cr);

			if (cr.getPartyID() == null /*|| p.getParty().getId().getId() == -1*/) {
				throw new Exception("Party Id null.");
			}

			cdcPartyBo.processChangeRecord(cr);

			CDCPartyChangeRecord result = cr;

			getCollector().emit(input, new Values(key, processId, result));
			getCollector().ack(input);

			LOG.debug("{}Action {} finished, result = {}.", logPreffix, eventName, result);
		} catch (Exception e) {
			LOG.error(String.format("%sError in Action %s for %s. Message: %s", logPreffix, eventName, cr, e.getMessage()), e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}

	protected void sendNextStep(Tuple input, CDCPartyChangeRecord cr) {

		Integer sequence = cr.getSequence();
		Integer eventNumber = cr.getEventNumber();
		Integer timestamp = cr.getTimestamp();
		String eventType = cr.getEventType().toString();
		String elementId = cr.getPartyID();
		String key = sequence + "-" + eventNumber + "-" + timestamp + "-" + eventType + "-" + elementId;

		Map<String, Object> values = new HashMap<>();
		values.put("key", key);
		values.put("message", cr);

		LOG.debug("CDC ChangeRecord processed; key = {}, tuple = {}", key, values);
		send(input, values);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "result"));
	}

	@Override
	public void declareFieldsDefinition() {
	}

}
