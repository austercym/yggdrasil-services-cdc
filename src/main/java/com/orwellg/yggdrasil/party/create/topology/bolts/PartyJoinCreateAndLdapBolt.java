package com.orwellg.yggdrasil.party.create.topology.bolts;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.execution.ExecutionResultItem;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.utils.factory.UnserializableFactory;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.yggdrasil.party.bo.PartyBO;
import com.orwellg.yggdrasil.party.config.LdapParams;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdap;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;
import com.orwellg.yggdrasil.party.ldap.LdapUtil;

public class PartyJoinCreateAndLdapBolt extends JoinFutureBolt<Party> {

	private static final Logger LOG = LogManager.getLogger(PartyJoinCreateAndLdapBolt.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected PartyBO partyBO = null;
	
//	/**
//	 * Define the name of the success stream
//	 */
//	public static final String EVENT_SUCCESS_STREAM = "party-create-and-ldap-success-stream";
//	/**
//	 * Define the name of the error stream
//	 */
//    public static final String EVENT_ERROR_STREAM   = "party-create-and-ldap-error-stream";

    @Override
    public String getEventSuccessStream() { return DEFAULT_STREAM; }
    @Override
    public String getEventErrorStream() { return DEFAULT_STREAM; }
    
    private String boltId;
    
    protected Gson gson;
    
    public PartyJoinCreateAndLdapBolt(String boltId) {
    		super(boltId);
    		this.boltId = boltId;
	}
    
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
	    	super.prepare(stormConf, context, collector);
	    	gson = new Gson();
    }
    
	@Override
	protected void join(Tuple input, String key, String processId, Party eventData) {
		
		String logPreffix = String.format("[Key: %s][ProcessId: %s] ", key, processId);
		
		LOG.debug("{}Processing key {}, eventData {}", logPreffix, key, eventData);
		
		try {
			MariaDbManager man = MariaDbManager.getInstance();
			partyBO = new PartyBO(man.getConnection());
			// Get (and init if they weren't before) topology application-wide params. Tries to connect to zookeeper:
			TopologyConfigWithLdap topologyConfig = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig();
			// LdapUtil specific params
			LdapParams ldapParams = topologyConfig.getLdapConfig().getLdapParams();
			LdapUtil ldapUtil = new LdapUtil(ldapParams);

			CompletableFuture<ExecutionResultItem> createFut = null;
			CompletableFuture<ExecutionResultItem> ldapFut = null;
			createFut = CompletableFuture.supplyAsync(() -> {
				try {
					partyBO.insertParty(eventData);
					return new ExecutionResultItem(true, Constants.EMPTY, Constants.EMPTY);
				} catch (SQLException e) {
					LOG.error(e.getMessage(), e);
					return new ExecutionResultItem(false, Constants.EMPTY, e.getMessage());
				}
			}, UnserializableFactory.getExecutor(boltId));
			ldapFut = CompletableFuture.supplyAsync(() -> {
				try {
					ldapUtil.registerLdapParty(eventData);
					return new ExecutionResultItem(true, Constants.EMPTY, Constants.EMPTY);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
					return new ExecutionResultItem(false, Constants.EMPTY, e.getMessage());
				}
			}, UnserializableFactory.getExecutor(boltId));
			
			if (createFut != null && ldapFut != null) {
				
				createFut.thenAcceptBothAsync(ldapFut, (createResult, ldapResult) -> {
					
					LOG.debug("{}Starting the join for key {}", logPreffix, key);
					
					String result;
					String stream;
					String eventName;
					if ((createResult == null && ldapResult == null) || (!createResult.getSuccess() || !ldapResult.getSuccess())) {
						stream = getEventErrorStream();
						eventName = PartyEvents.CREATE_PARTY_ERROR.getEventName();
						result = String.format("Futures did not return both correct results: {}, {}.", createResult, ldapResult);
					} else {
						stream = getEventSuccessStream();
						eventName = PartyEvents.CREATE_PARTY_COMPLETE.getEventName();
//						result = RawMessageUtils.encodeToString(PartyType.SCHEMA$, eventData.getParty());
						result = gson.toJson(eventData.getParty());
					}

					LOG.debug("{}Creating event {} to send to Kafka topic.", logPreffix, eventName);
					Map<String, Object> values = new HashMap<>();
					values.put("key", key); 
					values.put("processId", processId);
					values.put("eventName", eventName);
//					values.put("eventData", eventData);
					values.put("result", result);
					
					send(stream, input, values);
//					Values tuple = new Values(key, processId, eventName, result);
//					getCollector().emit(input, tuple);
//					getCollector().ack(input);
					
					LOG.debug("{}End processing the join validator for key {}", logPreffix, key);
				}, UnserializableFactory.getExecutor(boltId));
			}
		} catch (Exception e) {
			LOG.error("{} Error processing the account validation. Message: {},", logPreffix, e.getMessage(), e); 
			error(e, input);
		}
		
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventName", "result"}));
//		addFielsDefinition(EVENT_ERROR_STREAM,   Arrays.asList(new String[] {"key", "processId", "eventName", "result"}));
	}

}
