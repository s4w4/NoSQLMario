package la.persistence.database.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sun.corba.se.spi.orbutil.fsm.Action;

import la.common.Agent;
import la.common.State;
import redis.clients.jedis.Jedis;

public class DBRedis {
	private Jedis jedis;
	private final int ANZAHL_AKTIONEN = 12;

	public DBRedis() {
		// Connection to Redis server on localhost
		this.jedis = new Jedis("localhost");
		System.out.println("Connection to server sucessfully");
	}

	public void flushAll() {
		jedis.flushAll();
	}

	private static byte[] toByteArray(double value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		return bytes;
	}

	public static double toDoubleArray(byte[] byteArray) {
		int times = Double.SIZE / Byte.SIZE;
		return ByteBuffer.wrap(byteArray, 0, times).getDouble();
	}

	public double[] select(State state, Agent agent) {
		double[] result = new double[ANZAHL_AKTIONEN];

		byte[] key = (state.getMarioState() + "-" + state.getEnvironmentState()
				+ "-" + state.getEnemyState() + ":" + agent).getBytes();
		Map<byte[], byte[]> hashMap = jedis.hgetAll(key);
		if (hashMap.size() == 0) {
			for (int i = 0; i < result.length; i++) {
				hashMap.put(("action:" + (i + 1)).getBytes(), toByteArray(0));
				result[i] = 0;
			}
			//Best Action 
			hashMap.put("best-action".getBytes(), "-".getBytes());
			hashMap.put("best-action-value".getBytes(), toByteArray(0));
			//-----------
			jedis.hmset(key, hashMap);
		} else {
			for (int i = 0; i < result.length; i++)
				result[i] = toDoubleArray(hashMap.get(("action:" + (i + 1))
						.getBytes()));
		}
		return result;
	}

	public boolean update(State state, Agent agent, int action, double value) {
		byte[] key = (state.getMarioState() + "-" + state.getEnvironmentState()
				+ "-" + state.getEnemyState() + ":" + agent).getBytes();
		Map<byte[], byte[]> hashMap = jedis.hgetAll(key);
		String actionStr = "action:" + action;
		hashMap.put(actionStr.getBytes(), toByteArray(value));
		//BestAction
//		System.out.println("value = " + value + " tod " + toDoubleArray(hashMap.get("best-action-value".getBytes())));
		if (hashMap.get("best-action-value".getBytes()) != null && toDoubleArray(hashMap.get("best-action-value".getBytes())) <= value) {
			hashMap.put("best-action".getBytes(), actionStr.getBytes());
			hashMap.put("best-action-value".getBytes(), toByteArray(value));
			System.out.println("YES");
		}
		hashMap.put(key, toByteArray(value));
		//-----------
		jedis.hmset(key, hashMap);
		return true;
	}

	public Map<String, Integer> getPreferredAction(Agent agent) {
		Map<String, Integer> result = new HashMap<String, Integer>(); 
		Set<String> set = jedis.keys("*");
		for (String key : set) {
			Map<String, String> map = jedis.hgetAll(key);
			System.out.println(map.get("best-action"));
			if (!result.containsKey(map.get("best-action"))){
				result.put(map.get("best-action")+"", 1);
			}else{
				result.put(map.get("best-action")+"", result.get(map.get("best-action")));
			}
		}
		return result;
	}


	public void connect() {
		this.jedis.connect();
	}

	public void disconnect() {
		this.jedis.disconnect();
	}

}
