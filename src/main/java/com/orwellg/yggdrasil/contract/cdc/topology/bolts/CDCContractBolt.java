package com.orwellg.yggdrasil.contract.cdc.topology.bolts;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.orwellg.yggdrasil.contract.cdc.bo.CDCContractBO;
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
import com.orwellg.umbrella.avro.types.cdc.CDCContractChangeRecord;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ContractRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;

/**
 * Bolt to process Contract CDC actions received by topic.
 * 
 * @author c.friaszapater
 *
 */
public class CDCContractBolt extends BasicRichBolt {

	private static final long serialVersionUID = 1L;

	protected Logger LOG = LogManager.getLogger(CDCContractBolt.class);

	protected Gson gson;

	protected CDCContractBO cdcContractBo;
	protected Session session;

	protected String logPreffix;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
		addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
		buildCdcContractBo();
	}

	private void buildCdcContractBo() {
		if (session == null || session.isClosed()) {
			ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
			session = ScyllaManager.getInstance(scyllaParams.getNodeList()).getSession(scyllaParams.getKeyspace());
			ContractRepositoryImpl contractDao = new ContractRepositoryImpl(session);
			cdcContractBo = new CDCContractBO(gson, contractDao);
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

		buildCdcContractBo();
		
		// Received tuple is: key, processId, eventName, data

		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		String eventName = (String) input.getValueByField("eventName");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		CDCContractChangeRecord cr = null;
		try {
			cr = (CDCContractChangeRecord) input.getValueByField("eventData");

			LOG.debug("{}Action {} starting for changeRecord {}.", logPreffix, eventName, cr);

			if (cr.getContractID() == null /*|| p.getContract().getId().getId() == -1*/) {
				throw new Exception("Contract Id null.");
			}

			cdcContractBo.processChangeRecord(cr);

			CDCContractChangeRecord result = cr;

			getCollector().emit(input, new Values(key, processId, result));
			getCollector().ack(input);

			LOG.debug("{}Action {} finished, result = {}.", logPreffix, eventName, result);
		} catch (Exception e) {
			LOG.error(String.format("%sError in Action %s for %s. Message: %s", logPreffix, eventName, cr, e.getMessage()), e);
			// Exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely.
			throw new RuntimeException(e);
		}
	}

	protected void sendNextStep(Tuple input, CDCContractChangeRecord cr) {

		Integer sequence = cr.getSequence();
		Integer eventNumber = cr.getEventNumber();
		Integer timestamp = cr.getTimestamp();
		String eventType = cr.getEventType().toString();
		String elementId = cr.getContractID();
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
