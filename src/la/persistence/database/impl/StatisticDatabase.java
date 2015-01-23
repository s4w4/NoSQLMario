package la.persistence.database.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import la.common.Agent;
import la.common.Reward;
import la.common.State;
import la.common.Try;
import la.persistence.database.Database;

public class StatisticDatabase {
	private final String TABLENAME_TRY = "try";
	private final String COLUMNFAMILY_TRY = "cf";
	private final String COLUMN_WIN = "Win";
	private final String COLUMN_STEPS = "Steps";
	private final String COLUMN_WIN_COUNT = "WinCount";
	private final String COLUMN_DEATH_COUNT = "DeathCount";
	private final String COLUMN_HURT_COUNT = "HurtCount";
	private final String COLUMN_ELAPSED_FRAME_COUNT = "ElapsedFrameCount";
	private final String COLUMN_MOVE_RIGHT_COUNT = "MoveRightCount";
	private final String COLUMN_MOVE_LEFT_COUNT = "MoveLeftCount";
	private final String COLUMN_MOVE_UP_COUNT = "MoveUpCount";
	private final String COLUMN_MOVE_DOWN_COUNT = "MoveDownCount";
	private final String COLUMN_KILL_COUNT = "KillCount";
	private final String COLUMN_REWARDS = "Rewards";

	private HBaseConnection hbase;

	public StatisticDatabase() {
		try {
			this.hbase = new HBaseConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void saveAll(List<Try> aTryList, Agent agent) {
		try {
			for (Try t : aTryList) {
				saveTries(t, agent);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<Try> getTries(Agent agent) {
		List<Try> tries = new ArrayList<Try>();
		try {
			List<String> columns = new ArrayList<String>();
			columns.add(COLUMN_WIN);
			columns.add(COLUMN_REWARDS);
			columns.add(COLUMN_STEPS);
			columns.add(COLUMN_WIN_COUNT);
			columns.add(COLUMN_DEATH_COUNT);
			columns.add(COLUMN_HURT_COUNT);
			columns.add(COLUMN_KILL_COUNT);
			columns.add(COLUMN_ELAPSED_FRAME_COUNT);
			columns.add(COLUMN_MOVE_RIGHT_COUNT);
			columns.add(COLUMN_MOVE_LEFT_COUNT);
			columns.add(COLUMN_MOVE_UP_COUNT);
			columns.add(COLUMN_MOVE_DOWN_COUNT);

			List<List<byte[]>> allResults = hbase.readAllColumn(TABLENAME_TRY,
					agent.getId(), COLUMNFAMILY_TRY, columns);
			for (List<byte[]> resultList : allResults) {
				int win = Bytes.toInt(resultList.get(0));
				double rewards = Bytes.toDouble(resultList.get(1));
				int steps = Bytes.toInt(resultList.get(2));
				int winCount = Bytes.toInt(resultList.get(3));
				int deathCount = Bytes.toInt(resultList.get(4));
				int hurtCount = Bytes.toInt(resultList.get(5));
				int killCount = Bytes.toInt(resultList.get(6));
				int elapsedFrameCount = Bytes.toInt(resultList.get(7));
				int moveRightCount = Bytes.toInt(resultList.get(8));
				int moveLeftCount = Bytes.toInt(resultList.get(9));
				int moveUpCount = Bytes.toInt(resultList.get(10));
				int moveDownCount = Bytes.toInt(resultList.get(11));
				Try t = new Try(win, rewards, steps);
				t.setReward_win_count(winCount);
				t.setReward_death_count(deathCount);
				t.setReward_hurt_count(hurtCount);
				t.setReward_kill_count(killCount);
				t.setReward_elapsed_frame_count(elapsedFrameCount);
				t.setReward_move_right_count(moveRightCount);
				t.setReward_move_left_count(moveLeftCount);
				t.setReward_move_up_count(moveUpCount);
				t.setReward_move_down_count(moveDownCount);
				tries.add(t);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tries;
	}

	
	
	private void saveTries(Try t, Agent agent) throws IOException{
		Map<String, String> values = new HashMap<String, String>();

		String valueWin = t.getWin() + "";
		values.put(COLUMN_WIN, valueWin);

		String valueRewards = t.getRewards() + "";
		values.put(COLUMN_REWARDS, valueRewards);

		String valueSteps = t.getSteps() + "";
		values.put(COLUMN_STEPS, valueSteps);

		String valueWinCount = t.getReward_win_count() + "";
		values.put(COLUMN_WIN_COUNT, valueWinCount);

		String valueDeathCount = t.getReward_death_count() + "";
		values.put(COLUMN_DEATH_COUNT, valueDeathCount);

		String valueHurtCount = t.getReward_hurt_count() + "";
		values.put(COLUMN_HURT_COUNT, valueHurtCount);

		String valueKillCount = t.getReward_kill_count() + "";
		values.put(COLUMN_KILL_COUNT, valueKillCount);

		String valueElapsedFrameCount = t.getReward_elapsed_frame_count() + "";
		values.put(COLUMN_ELAPSED_FRAME_COUNT, valueElapsedFrameCount);

		String valueMoveRight = t.getReward_move_right_count() + "";
		values.put(COLUMN_MOVE_RIGHT_COUNT, valueMoveRight);

		String valueMoveLeft = t.getReward_move_left_count() + "";
		values.put(COLUMN_MOVE_LEFT_COUNT, valueMoveLeft);

		String valueMoveUp = t.getReward_move_up_count() + "";
		values.put(COLUMN_MOVE_UP_COUNT, valueMoveUp);

		String valueMoveDown = t.getReward_move_down_count() + "";
		values.put(COLUMN_MOVE_DOWN_COUNT, valueMoveDown);

		hbase.writeValues(TABLENAME_TRY, agent.getId(),
				COLUMNFAMILY_TRY, values, hbase.getNextTimestamp(TABLENAME_TRY,
						agent.getId(), COLUMNFAMILY_TRY, COLUMN_WIN));
	}

}
