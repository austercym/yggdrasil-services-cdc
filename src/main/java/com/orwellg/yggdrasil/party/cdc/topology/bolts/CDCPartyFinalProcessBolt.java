//package com.orwellg.yggdrasil.party.cdc.topology.bolts;
//
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//
//import com.google.gson.Gson;
//import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
//import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
//
///**
// * Input tuple: key, processId, result (PartyType)<br/>
// * Emit tuple: key, processId, eventName ("PartyCreated"), result
// * (serialized PartyType)<br/>
// * 
// * @author c.friaszapater
// *
// */
//public class CDCPartyFinalProcessBolt extends BasicRichBolt {
//
//	private Logger LOG = LogManager.getLogger(CDCPartyFinalProcessBolt.class);
//
//	private String logPreffix;
//
//	protected Gson gson;
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//
//	@Override
//	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
//		super.prepare(stormConf, context, collector);
//		
//		gson = new Gson();
//	}
//	
//	@Override
//	public void execute(Tuple input) {
//
//		LOG.debug("Tuple input = {}", input);
//
//		String key = (String) input.getValueByField("key");
//		String processId = (String) input.getValueByField("processId");
//
//		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);
//
//		LOG.debug("{}Action Finished. Processing the results", logPreffix);
//
//		try {
//			CDCPartyChangeRecord cr = (CDCPartyChangeRecord) input.getValueByField("result");
//
//			// Return ChangeRecord
//			String result = gson.toJson(cr);
//
//			String resultEventName = cr.getEventType().toString();
//			LOG.debug("{}Creating event {} to send to Kafka topic.", logPreffix, resultEventName);
//
//			Values tuple = new Values(key, processId, resultEventName, result);
//			getCollector().emit(input, tuple);
//			getCollector().ack(input);
//
//			LOG.info("{}Complete event sent to Kafka topic, tuple = {}.", logPreffix, tuple);
//		} catch (Exception e) {
//			LOG.error("{}Error trying to send Complete event. Message: {},", logPreffix, e.getMessage(), e);
//			getCollector().reportError(e);
//			getCollector().fail(input);
//		}
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("key", "processId", "eventName", "result"));
//	}
//
//	@Override
//	public void declareFieldsDefinition() {
//	}
//
//}
