package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import java.util.Arrays;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.impl.PartyRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;

/**
 * Bolt to process Party CDC actions received by topic.
 * 
 * @author c.friaszapater
 *
 */
public class CDCPartyBolt extends BasicRichBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LogManager.getLogger(CDCPartyBolt.class);

	protected CDCPartyBO cdcPartyBo;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		Gson gson = new Gson();
		ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
		PartyRepositoryImpl partyDao = new PartyRepositoryImpl(scyllaParams.getNodeList(), scyllaParams.getKeyspace());
		cdcPartyBo = new CDCPartyBO(gson, partyDao);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * Method that decode the receive event coming from Kafka and send it to the next step in the topology
	 */
	@Override
	public void execute(Tuple input) {		

		LOG.debug("CDC ChangeRecord received: {}. Starting the decode process.", input);

		try {
			String crJson = getJsonFromChangeRecordTuple(input);
			
			CDCPartyChangeRecord cr = cdcPartyBo.parseChangeRecordJson(crJson);

			cdcPartyBo.processChangeRecord(cr);

			sendNextStep(input, cr);
		} catch (Exception e) {
			LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
			error(e, input);
		}
	}

	public String getJsonFromChangeRecordTuple(Tuple input) {
		return (String) input.getValues().get(4);
	}
	
	protected void sendNextStep(Tuple input, CDCPartyChangeRecord cr) {

		String id = cr.getPartyID();
		Integer eventNumber = cr.getEventNumber();
		Integer sequence = cr.getSequence();
		Integer timestamp = cr.getTimestamp();
		LOG.debug("CDC ChangeRecord decoded; id = {}, eventNumber = {}, sequence = {}, timestamp = {}",
				id, eventNumber, sequence, timestamp);

		Values tuple = new Values(id, eventNumber, sequence, cr);

		getCollector().emit(input, tuple);
		getCollector().ack(input);
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] {"id", "eventNumber", "sequence", "result"}));
	}
}
