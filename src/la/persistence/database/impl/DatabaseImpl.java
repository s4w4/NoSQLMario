package la.persistence.database.impl;

import context.ManagerFactory;
import la.persistence.database.Database;
import la.persistence.entities.RewardsForAction;
import la.common.Reward;
import la.common.Agent;
import la.common.State;
import la.common.Try;
import la.persistence.importhandler.ImportHandler;
import la.persistence.importhandler.impl.DBConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DatabaseImpl implements Database {

	private DBRedis dbRedis;

	public DatabaseImpl() {
		this.dbRedis = new DBRedis();
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
	public void saveAll(List<Try> aTryList, Agent rewardsGroup) {
	}

	@Override
	public List<Try> getTries(Agent rewardsGroup) {
//		return dbCommunication.selectTries(rewardsGroup);
		return null; 
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
		return null; 
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}


}
