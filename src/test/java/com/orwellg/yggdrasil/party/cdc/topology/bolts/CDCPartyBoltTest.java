package com.orwellg.yggdrasil.party.cdc.topology.bolts;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.orwellg.umbrella.commons.config.MariaDBConfig;
import com.orwellg.umbrella.commons.config.params.MariaDBParams;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;

public class CDCPartyBoltTest {

	protected CDCPartyBolt bolt = new CDCPartyBolt();

	@Mock
	protected TopologyConfig config;
	
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Before
	public void setUp() throws Throwable {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExecute() throws Exception {
		// When execute() with CDCPartyChangeRecord in value 4 of Tuple
		// Then Party insert requested to CDCPartyBO
		// And emit with CDCPartyChangeRecord as result in Tuple
		
	}
}
