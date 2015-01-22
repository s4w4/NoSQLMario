package la.persistence.database.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;

public class DBRedis {
	private Jedis jedis;

	public DBRedis(String host, int port) {
		this.jedis = new Jedis(host, port);
	}

//	public Data getDataByPLZ(String plz) {
//		jedis.connect();
//		Map<String, String> map = jedis.hgetAll(plz);
//		if (map.size() == 0)
//			return null;
//		String city = map.get("city");
//		int pop = Integer.parseInt(map.get("pop"));
//		float[] loc = new float[2];
//		loc[0] = Float.parseFloat(map.get("loc_x"));
//		loc[1] = Float.parseFloat(map.get("loc_y"));
//		String state = map.get("state");
//		jedis.disconnect();
//		return new Data(plz, city, loc, pop, state);
//	}
//
//	public Set<String> getPLZByCity(String city) {
//		jedis.connect();
//		Set<String> res = new HashSet<String>();
//		if (jedis.isConnected()) {
//			Set<String> set = jedis.keys("*");
//			for (String key : set) {
//				Map<String, String> map = jedis.hgetAll(key);
//				String cityFromDB = map.get("city");
//				if (city.equals(cityFromDB))
//					res.add(key);
//			}
//		}
//		jedis.disconnect();
//		return res;
//	}
}
