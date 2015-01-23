package la.persistence.database.impl;

import la.persistence.database.Database;
import la.common.Reward;
import la.common.Agent;
import la.common.State;
import la.common.Try;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatabaseImpl implements Database {

	private DBRedis dbRedis;
	private StatisticDatabase hbase;

	public DatabaseImpl() {
		this.dbRedis = new DBRedis();
		this.hbase = new StatisticDatabase(); 
	}

	@Override
	public Agent getAgent(List<Reward> rewards) {
			if (rewards == null)
				return null; 
			if (rewards.size() == 0)
				return null; 
			
			Agent result = null;
//			try {
//				result = dbCommunication.getRewardsGroup(rewards);
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
			return result;
	}

	@Override
	public double[] select(State state, Agent rewardsGroup) {
		if (state == null)
			return null; 
		if (rewardsGroup == null)
			return null;
		return dbRedis.select(state, rewardsGroup);
	}

	@Override
	public boolean update(State state, Agent rewardsGroup, int action,
			double value) {
		return dbRedis.update(state, rewardsGroup, action, value);
	}


	@Override
	public void saveAll(List<Try> aTryList, Agent agent) {
		hbase.saveAll(aTryList, agent);
	}

	@Override
	public List<Try> getTries(Agent agent) {
		return hbase.getTries(agent); 
	}

	@Override
	public Agent getLastAgent() {
//		Agent result = null;
//		try {
//			result = dbCommunication.getLastRewardsGroup();
//			//result.setTries(dbCom);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		return result;
//		return new Agent(0, new ArrayList<Reward>()); 
		return null; 
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Integer> getPreferredAction(Agent agent) {
		return dbRedis.getPreferredAction(agent);
	}


}
