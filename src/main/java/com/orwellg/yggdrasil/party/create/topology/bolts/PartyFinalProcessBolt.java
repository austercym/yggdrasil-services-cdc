package com.orwellg.yggdrasil.party.create.topology.bolts;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.yggdrasil.party.create.topology.CreatePartyTopology;

/**
 * Input tuple: key, processId, result (PartyType)<br/>
 * Emit tuple: key, processId, eventName ("PartyCreated"), result
 * (serialized PartyType)<br/>
 * 
 * @author c.friaszapater
 *
 */
public class PartyFinalProcessBolt extends BasicRichBolt {

	private Logger LOG = LogManager.getLogger(PartyFinalProcessBolt.class);

	private String logPreffix;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected Gson gson;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
	}

	@Override
	public void execute(Tuple input) {

		LOG.info("Tuple input = {}", input);

		String key = (String) input.getValueByField(CreatePartyTopology.CREATE_PARTY_COMPONENT_ID+":key");
		String processId = (String) input.getValueByField(CreatePartyTopology.CREATE_PARTY_COMPONENT_ID+":processId");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		LOG.info("{}Party action Finished. Processing the results", logPreffix);

		try {
			// Compose result. CreateParty => eventName = PartyCreated, result = Party (serialized)
			Party party = (Party) input.getValueByField(CreatePartyTopology.CREATE_PARTY_COMPONENT_ID+":result");
//			String result = RawMessageUtils.encodeToString(PartyType.SCHEMA$, party.getParty());
			String result = gson.toJson(party.getParty());

			Party partyLdap = (Party) input.getValueByField(CreatePartyTopology.REGISTER_PARTY_LDAP_COMPONENT_ID+":result");
			
			if (party == null || partyLdap == null || !party.getParty().getId().getId().equals(partyLdap.getParty().getId().getId())) {
				LOG.warn("Party received from {} is {}", CreatePartyTopology.CREATE_PARTY_COMPONENT_ID+":result", party);
				LOG.warn("Party received from {} is {}", CreatePartyTopology.REGISTER_PARTY_LDAP_COMPONENT_ID+":result", partyLdap);
				throw new Exception(String.format("Bolts %s and %s did not provide both correct results.", CreatePartyTopology.CREATE_PARTY_COMPONENT_ID, CreatePartyTopology.REGISTER_PARTY_LDAP_COMPONENT_ID));
			}

			LOG.info("{}Creating event {} to send to Kafka topic.", logPreffix, PartyEvents.CREATE_PARTY_COMPLETE.getEventName());

			Values tuple = new Values(key, processId, PartyEvents.CREATE_PARTY_COMPLETE.getEventName(), result);
			getCollector().emit(input, tuple);
			getCollector().ack(input);

			LOG.info("{}CreateParty Complete event sent to Kafka topic, tuple = {}.", logPreffix, tuple);
		} catch (Exception e) {
			LOG.error("{}Error trying to send CreateParty Complete event. Message: {},", logPreffix, e.getMessage(), e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "eventName", "result"));
	}

	@Override
	public void declareFieldsDefinition() {
	}

}
