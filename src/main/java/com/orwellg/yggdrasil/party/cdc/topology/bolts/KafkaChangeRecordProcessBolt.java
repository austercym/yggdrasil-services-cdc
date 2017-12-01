package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import java.util.Arrays;
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
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.yggdrasil.party.cdc.bo.CDCPartyBO;

public class KafkaChangeRecordProcessBolt extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected Logger LOG = LogManager.getLogger(KafkaChangeRecordProcessBolt.class);
	
	protected Gson gson;
	
	protected CDCPartyBO cdcPartyBo;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Method that decode the receive event coming from Kafka and send it to the next step in the topology
	 */
	@Override
	public void execute(Tuple input) {		
		
		LOG.debug("Event received: {}. Starting the decode process.", input);
				
		try {
			String crJson = getJsonFromChangeRecordTuple(input);

			CDCPartyChangeRecord cr = parseJson(crJson);
			
			if (cr != null) {
				sendNextStep(input, cr);
			} else {
				LOG.info(String.format("Ignoring schema change ChangeRecord = %s", input));
				getCollector().ack(input);
			}
		} catch (Exception e) {
			LOG.error(String.format("Error decoding ChangeRecord %s. Message: %s", input, e.getMessage()), e);
			// Exception must be thrown so that the worker dies and then storm spawns a new worker and retries indefinitely.
			throw new RuntimeException(e);
		}
	}

	protected CDCPartyChangeRecord parseJson(String crJson) {
		if (crJson != null && crJson.startsWith(
				"{\"namespace\": \"MaxScaleChangeDataSchema.avro\", \"type\": \"record\", \"name\": \"ChangeRecord\","+ " \"fields\":")) {
			// Ignore
			return null;
		} else {
			CDCPartyChangeRecord cr = gson.fromJson(crJson, CDCPartyChangeRecord.class);
			return cr;
		}
	}
	
	protected String getJsonFromChangeRecordTuple(Tuple input) {
		return (String) input.getValues().get(4);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * [source,java]
	 * addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData"}));
	 */
	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData"}));
	}
	
	public void sendNextStep(Tuple input, CDCPartyChangeRecord cr) {

		String key = outputTupleKey(cr);
		String processId = outputTupleProcessId(cr);
		String eventName = outputTupleEventName(cr);
		CDCPartyChangeRecord eventData = cr;

		LOG.debug("[Key: {}][ProcessId: {}]: The event was decoded. Send the tuple to the next step in the Topology.",
				key, processId);

		getCollector().emit(input, new Values(key, processId, eventName, eventData));
		getCollector().ack(input);

		LOG.debug("[Key: {}][ProcessId: {}]: Action {} sent for processing in storm, eventData = {}.",
				key, processId, eventName, eventData);

	}

	protected String outputTupleEventName(CDCPartyChangeRecord cr) {
		String eventName = cr.getEventType().toString();
		return eventName;
	}

	protected String outputTupleProcessId(CDCPartyChangeRecord cr) {
		String eventType = cr.getEventType().toString();
		String elementId = cr.getPartyID();
		String processId = eventType + "-" + elementId;
		return processId;
	}

	protected String outputTupleKey(CDCPartyChangeRecord cr) {
		Integer sequence = cr.getSequence();
		Integer eventNumber = cr.getEventNumber();
		Integer timestamp = cr.getTimestamp();
		String eventType = cr.getEventType().toString();
		String elementId = cr.getPartyID();
		String key = sequence + "-" + eventNumber + "-" + timestamp + "-" + eventType + "-" + elementId;
		return key;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "eventName", "eventData"));
	}
}
