package la.persistence.database.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import la.common.Agent;
import la.common.State;
import redis.clients.jedis.Jedis;

public class DBRedis {
	private Jedis jedis;
	private final int ANZAHL_AKTIONEN = 12; 
	public DBRedis() {
		//Connection to Redis server on localhost
		this.jedis = new Jedis("localhost");
		System.out.println("Connection to server sucessfully");
//		jedis.flushAll();
	}

	private static byte[] toByteArray(double value){
		byte[] bytes = new byte[8]; 
		ByteBuffer.wrap(bytes).putDouble(value); 
		return bytes; 
	}
	
	public static double toDoubleArray(byte[] byteArray){
		int times = Double.SIZE / Byte.SIZE; 
		return ByteBuffer.wrap(byteArray, 0, times).getDouble(); 
	}

	public double[] select(State state, Agent agent) {
		double [] result = new double[ANZAHL_AKTIONEN];
		
		byte[] key = (state.getMarioState()+ "-" + state.getEnvironmentState() + "-"+ state.getEnemyState()+ ":" + agent).getBytes(); 
		Map<byte[], byte[]> hashMap = jedis.hgetAll(key);
		if (hashMap.size() == 0){
			for (int i = 0; i < result.length; i++) {
				hashMap.put(("Action:"+(i+1)).getBytes(), toByteArray(0));
				result[i] = 0; 
			}
			jedis.hmset(key, hashMap);
		}else{
			for(int i = 0 ; i<result.length; i++)
				result[i] = toDoubleArray(hashMap.get(("Action:"+(i+1)).getBytes())); 
		}
		return result; 
	}
	
	public boolean update(State state, Agent agent, int action, double value) {
		byte[] key = (state.getMarioState()+ "-" + state.getEnvironmentState() + "-"+ state.getEnemyState()+ ":" + agent).getBytes(); 
		Map<byte[], byte[]> hashMap = jedis.hgetAll(key);
		hashMap.put(("Action:" + action).getBytes(), toByteArray(value));
		jedis.hmset(key, hashMap); 
		return true; 
	}
	
//	public void getDataByPLZ(String plz) {
//		jedis.connect();
//		Map<String, String> map = jedis.hgetAll(plz);
//		if (map.size() != 0) {
//			String city = map.get("city");
//			int pop = Integer.parseInt(map.get("pop"));
//			float[] loc = new float[2];
//			loc[0] = Float.parseFloat(map.get("loc_x"));
//			loc[1] = Float.parseFloat(map.get("loc_y"));
//			String state = map.get("state");
//			jedis.disconnect();
//			System.out.println("plz " + plz + " state " + state);
//		}
//		// return new Data(plz, city, loc, pop, state);
//	}
	//
	// public Set<String> getPLZByCity(String city) {
	// jedis.connect();
	// Set<String> res = new HashSet<String>();
	// if (jedis.isConnected()) {
	// Set<String> set = jedis.keys("*");
	// for (String key : set) {
	// Map<String, String> map = jedis.hgetAll(key);
	// String cityFromDB = map.get("city");
	// if (city.equals(cityFromDB))
	// res.add(key);
	// }
	// }
	// jedis.disconnect();
	// return res;
	// }



	public void connect() {
		this.jedis.connect();
	}


	public void disconnect() {
		this.jedis.disconnect();
	}

}
