package la.persistence.database;

import java.util.List;

import la.common.Reward;
import la.common.Agent;
import la.common.State;
import la.common.Try;

public interface Database {
	
	public Agent getAgent(List<Reward> rewards); 

	public Agent getLastAgent(); 
	
	// REDIS: Fertig
	public double[] select(State state, Agent agent);
	
	// REDIS: Fertig
	boolean update(State state, Agent agent, int action, double value);

	// HBase: ... fast fertig.
	public void saveAll(List<Try> aTryList, Agent agent); 

	// HBase: ...
	public List<Try> getTries(Agent agent); 
	
	
	public void reset();


}
