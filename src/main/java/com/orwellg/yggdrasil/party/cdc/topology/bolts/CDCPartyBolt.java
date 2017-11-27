//package com.orwellg.yggdrasil.party.cdc.topology.bolts;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//
//import com.orwellg.umbrella.commons.repositories.scylla.impl.PartyRepositoryImpl;
//import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
//import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
//import com.orwellg.umbrella.commons.types.party.Party;
//import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
//import com.orwellg.yggdrasil.party.get.nosql.dao.PartyNoSqlDao;
//
///**
// * Process PartyEvents.GET_PARTY event with Party parameter: Read Party from
// * noSql database.<br/>
// * Input tuple: key, processId, eventName, data (Party)<br/>
// * Emit tuple: key, processId, result (Party)<br/>
// * 
// * @author c.friaszapater
// *
// */
//public class CDCPartyBolt extends BasicRichBolt {
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//
//	private final static Logger LOG = LogManager.getLogger(CDCPartyBolt.class);
//
//	private String logPreffix;
//
//	private String propertiesFile;
//
//	/**
//	 * Usual constructor unless specific topologyPropertiesFile is needed.
//	 * TopologyConfig.DEFAULT_PROPERTIES_FILE properties file will be used.
//	 */
//	public CDCPartyBolt() {
//		this.propertiesFile = null;
//	}
//
//	/**
//	 * Constructor with specific topologyPropertiesFile (eg: for tests).
//	 * @param propertiesFile TopologyConfig properties file. Can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
//	 */
//	public CDCPartyBolt(String propertiesFile) {
//		this.propertiesFile = propertiesFile;
//	}
//
//	protected String getLogPreffix() {
//		return logPreffix;
//	}
//
//	@Override
//	public void execute(Tuple input) {
//
//		LOG.info("Tuple input = {}", input);
//
//		// Received tuple is: key, processId, eventName, data
//
//		String key = (String) input.getValueByField("key");
//		String processId = (String) input.getValueByField("processId");
//		String eventName = (String) input.getValueByField("eventName");
//
//		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);
//
//		Party p = null;
//		try {
//			p = (Party) input.getValueByField("eventData");
//
//			LOG.info("{}Action {} starting for party {}.", getLogPreffix(), eventName, p);
//
//			if (p.getParty().getId() == null /*|| p.getParty().getId().getId() == -1*/) {
//				throw new Exception("Party Id null.");
//			}
//
//			String partyId = p.getParty().getId().getId();
//			
//			Party result = retrieve(partyId);
//
//			LOG.info("{}Action {} for party {} finished. Sending the results {}.", getLogPreffix(), eventName, p,
//					result);
//
//			getCollector().emit(input, new Values(key, processId, result));
//			getCollector().ack(input);
//
//			LOG.info("{}Action {} for party {} finished. Results sent successfully.", getLogPreffix(), eventName, p);
//		} catch (Exception e) {
//			LOG.error("{}Error in Action {} for party {}. Message: {}", getLogPreffix(), eventName, p, e.getMessage(),
//					e);
//			getCollector().reportError(e);
//			getCollector().fail(input);
//		}
//	}
//
//	protected Party retrieve(String partyId) {
//		// Get scylladb connection
//		ScyllaManager man = ScyllaManager.getInstance(TopologyConfigFactory.getTopologyConfig(propertiesFile).getScyllaConfig().getScyllaParams().getNodeList());
//
//		// Read Party from noSql database
//		PartyRepository partyDAO = new PartyRepositoryImpl(scyllaNodes, scyllaKeyspace);
//		// Result for next bolt: Party
//		Party result = partyDAO.getById(partyId);
//		return result;
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("key", "processId", "result"));
//	}
//
//	@Override
//	public void declareFieldsDefinition() {
//	}
//
//}
