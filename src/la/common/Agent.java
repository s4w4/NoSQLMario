package la.common;

import java.util.List;

public class Agent {

	private int id; 
	private List<Reward> rewards;
	public Agent(int id, List<Reward> rewards) {
		super();
		this.id = id;
		this.rewards = rewards;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public List<Reward> getRewards() {
		return rewards;
	}

	public Reward getReward(String name) {
		Reward returnReward = null;

		for(Reward reward : rewards) {
			if(reward.getName().equals(name)) {
				returnReward = reward;
			}
		}

		return returnReward;
	}

	public void setRewards(List<Reward> rewards) {
		this.rewards = rewards;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Agent other = (Agent) obj;
		if (id != other.id)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return id + "";
	} 
	
	
	
}
