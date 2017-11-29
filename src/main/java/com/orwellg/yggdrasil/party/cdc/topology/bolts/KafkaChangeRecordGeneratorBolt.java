package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;

/**
 * = Kafka Event Generator
 * 
 * That is a basic utility bolt. The principal functionality is create a Event and sent it.
 * 
 * The bolt receive a tuple that contains:
 * [square]
 * * key The event key
 * * processId Process Identifier of the process that generate the event
 * * eventName Name of the event that we must generate
 * * result Event data
 * 
 * When this data are receive, the bolt create a event follows the next rules:
 * [square]
 * * The key will be the parent key for the new event
 * * The processId will be the parent process id for the new event
 * * The eventName will be the event name for the new event 
 * * The result will be transformed to json and store it in the event data field of the new event
 * 
 * The new event is store in the message field of the new tuple to send
 * 
 * @author f.deborja
 *
 */
public class KafkaChangeRecordGeneratorBolt extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger LOG = LogManager.getLogger(KafkaChangeRecordGeneratorBolt.class);
	
	private String logPreffix;
	
	/**
	 * {@inheritDoc}
	 * 
	 * This method creates a new event, stores it in a tuple and sends the tuple to the next step in the stom topology.
	 * 
	 */
	@Override
	public void execute(Tuple input) {
		
		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		
		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);
		
		try {
			LOG.info("{}Kafka Event Send. Generating event.", logPreffix);
			
			String crJson = (String) input.getValueByField("result");
			
			Map<String, Object> values = new HashMap<>();
			values.put("key", key);
			values.put("message", crJson);
			
			send(input, values);
			
			LOG.info("{}Kafka Event sended to Kafka topic.", logPreffix);
		} catch (Exception e) {
			LOG.info("{}Error when we try send the result event to Kafka Topic. Message: {},", logPreffix, e.getMessage(), e); 
			error(e, input);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * [source,java]
	 * addFielsDefinition(Arrays.asList(new String[] {"key", "message"}))
	 */
	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
	}
}
