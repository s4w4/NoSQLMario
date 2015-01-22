package la.persistence.database;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import la.common.Agent;
import la.common.Reward;
import la.common.State;
import la.persistence.database.impl.DBRedis;
import la.persistence.importhandler.ImportHandler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;

import org.junit.Test;

import context.ManagerFactory;
public class DBRedisTest {

	private DBRedis dbRedis; 
	
	@Before 
	public void setUp() throws Exception {
		dbRedis = new DBRedis();
		dbRedis.connect();
	}
	@After 
	public void tearDown() throws Exception {
		dbRedis.disconnect(); 
//		dbRedis.flushAll(); 
	}
	
	@Test
	public void testUpdateData() {
		State state = new State(3,3,3);
		List<Reward> rewards = new ArrayList<Reward>();
		rewards.add(new Reward("Reward1", 100));		
		rewards.add(new Reward("Reward2", 200));		
		Agent agent = new Agent(1, rewards);
		int action = 12;
		double value = -13;
		dbRedis.update(state, agent, action, value);
	}
	
	@Test
	public void testSelectData1() {
		dbRedis.flushAll(); 
		State state = new State(2,2,2);
		List<Reward> rewards = new ArrayList<Reward>();
		rewards.add(new Reward("Reward1", 100));		
		rewards.add(new Reward("Reward2", 200));		
		Agent agent = new Agent(1, rewards);
		double[] result = dbRedis.select(state, agent);
		assertTrue(result[0] == 0);
		assertTrue(result[1] == 0);
		assertTrue(result[2] == 0);
		assertTrue(result[3] == 0);
		assertTrue(result[4] == 0);
		assertTrue(result[5] == 0);
		assertTrue(result[6] == 0);
		assertTrue(result[7] == 0);
		assertTrue(result[8] == 0);
		assertTrue(result[9] == 0);
		assertTrue(result[10] == 0);
		assertTrue(result[11] == 0);	
	}
	

	@Test
	public void testSelectData2() {
		State state = new State(2,2,2);
		List<Reward> rewards = new ArrayList<Reward>();
		rewards.add(new Reward("Reward1", 100));		
		rewards.add(new Reward("Reward2", 200));		
		Agent agent = new Agent(1, rewards);
		
		//UPDATE
		int action = 10;
		double value = -13;
		dbRedis.update(state, agent, action, value);
		
		double[] result = dbRedis.select(state, agent);
		assertTrue(result[0] == 0);
		assertTrue(result[1] == 0);
		assertTrue(result[2] == 0);
		assertTrue(result[3] == 0);
		assertTrue(result[4] == 0);
		assertTrue(result[5] == 0);
		assertTrue(result[6] == 0);
		assertTrue(result[7] == 0);
		assertTrue(result[8] == 0);
		assertTrue(result[9] == -13);
		assertTrue(result[10] == 0);
		assertTrue(result[11] == 0);	
	}
	@Test
	public void testGet() {
		
	}
	

}
