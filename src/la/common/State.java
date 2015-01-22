package la.common;

import la.application.configManagement.Config;

/**
 * Created by Alex on 29.10.2014.
 */
public class State {
    private static final long MARIO_MUL = Config.MARIO_MUL;
    private static final long ENVIRONMENT_MUL =Config.ENVIRONMENT_MUL;
    private static final long ENEMY_MUL = Config.ENEMY_MUL;

    private long marioState;
    private long environmentState;
    private long enemyState;
    private long stateId; 

    public State(long marioState, long enviromentState, long enemyState){
        int enemyFlag = 0;

        if (enemyState != 0) {
            enemyFlag = 1;
        }

        this.marioState = ((marioState*enemyFlag)+1);
        this.environmentState = enviromentState;
        this.enemyState = enemyState;

        long id = (marioState*MARIO_MUL) + (environmentState*ENVIRONMENT_MUL) + (enemyState*ENEMY_MUL);

        this.setStateId(id);
    }

    public State(long stateId) {
    	this.stateId = stateId; 
	}

	public long getStateId(){
        return stateId; 
    }
	
    public long getMarioState() {
        return marioState;
    }

    public long getEnvironmentState() {
        return environmentState;
    }

    public long getEnemyState() {
        return enemyState;
    }

    public void setMarioState(long marioState) {
        this.marioState = marioState;
    }

    public void setEnvironmentState(long enviromentState) {
        this.environmentState = enviromentState;
    }

    public void setEnemyState(long enemyState) {
        this.enemyState = enemyState;
    }

	public void setStateId(long stateId) {
		this.stateId = stateId;
	}
	
    @Override
    public String toString() {
    	return getStateId()+"";
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (enemyState ^ (enemyState >>> 32));
		result = prime * result
				+ (int) (environmentState ^ (environmentState >>> 32));
		result = prime * result + (int) (marioState ^ (marioState >>> 32));
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
		State other = (State) obj;
		if (enemyState != other.enemyState)
			return false;
		if (environmentState != other.environmentState)
			return false;
		if (marioState != other.marioState)
			return false;
		return true;
	}
    
    

}
