package la.application.exportManagement;

import jxl.CellView;
import jxl.format.UnderlineStyle;
import jxl.write.*;
import jxl.write.Number;
import la.common.Reward;
import la.common.Try;

import java.util.List;

import static la.application.exportManagement.ExportCommon.*;

public class SheetFormat {

    private WritableCellFormat timesBoldUnderline;
    private WritableCellFormat times;

    public SheetFormat() throws WriteException {
        WritableFont timesFont1 = new WritableFont(WritableFont.TIMES, 10);
        times = new WritableCellFormat(timesFont1);
        times.setWrap(true);

        WritableFont timesFont2 = new WritableFont(WritableFont.TIMES, 10, WritableFont.BOLD, false,UnderlineStyle.SINGLE);
        timesBoldUnderline = new WritableCellFormat(timesFont2);
        timesBoldUnderline.setWrap(true);

        CellView cv = new CellView();
        cv.setFormat(times);
        cv.setFormat(timesBoldUnderline);
        cv.setAutosize(true);
    }

    /**********************************************************************************
     *  VALUES
     **********************************************************************************/

    public void createLabelValues(WritableSheet sheet, List<Reward> rewards ) throws WriteException {
        // Die Reward Gruppe
        addCaption(sheet, COLUMN_VALUE_REWARDS_BEGIN, ROW_VALUE_REWARDS_BEGIN, "RewardsGroup");
        // Alle Rewards
        for (int i = COLUMN_VALUE_REWARDS_BEGIN; i < rewards.size(); i++) {
            addCaption(sheet, i + 1, ROW_VALUE_REWARDS_BEGIN, rewards.get(i).getName());
        }
        // Die Tries
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN,    ROW_VALUE_TRIES_BEGIN, "ID");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +1, ROW_VALUE_TRIES_BEGIN, "Win");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +2, ROW_VALUE_TRIES_BEGIN, "Rewards");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +3, ROW_VALUE_TRIES_BEGIN, "Steps");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +4, ROW_VALUE_TRIES_BEGIN, "count [win]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +5, ROW_VALUE_TRIES_BEGIN, "count [death]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +6, ROW_VALUE_TRIES_BEGIN, "count [hurt]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +7, ROW_VALUE_TRIES_BEGIN, "count [kill]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +8, ROW_VALUE_TRIES_BEGIN, "count [elapsed_frame]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +9, ROW_VALUE_TRIES_BEGIN, "count [right]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +10, ROW_VALUE_TRIES_BEGIN, "count [left]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +11, ROW_VALUE_TRIES_BEGIN, "count [up]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +12, ROW_VALUE_TRIES_BEGIN, "count [down]");

        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +14, ROW_VALUE_TRIES_BEGIN, "count * reward [win]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +15, ROW_VALUE_TRIES_BEGIN, "count * reward [death]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +16, ROW_VALUE_TRIES_BEGIN, "count * reward [hurt]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +17, ROW_VALUE_TRIES_BEGIN, "count * reward [kill]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +18, ROW_VALUE_TRIES_BEGIN, "count * reward [elapsed_frame]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +19, ROW_VALUE_TRIES_BEGIN, "count * reward [right]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +20, ROW_VALUE_TRIES_BEGIN, "count * reward [left]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +21, ROW_VALUE_TRIES_BEGIN, "count * reward [up]");
        addCaption(sheet, COLUMN_VALUE_TRIES_BEGIN +22, ROW_VALUE_TRIES_BEGIN, "count * reward [down]");
    }

    public void createContentValues(WritableSheet sheet, int rewardGroupID, List<Reward> rewards, List<Try> tries) throws WriteException {
        addNumber(sheet, COLUMN_VALUE_REWARDS_BEGIN, ROW_VALUE_REWARDS_BEGIN +1, rewardGroupID);
        for (int i = COLUMN_VALUE_REWARDS_BEGIN; i < rewards.size(); i++) {
            addNumber(sheet, i + 1, ROW_VALUE_REWARDS_BEGIN +1, rewards.get(i).getReward());
        }

        for (int i = 0; i < tries.size(); i++) {
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN,     ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getId());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +1,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getWin());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +2,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getRewards());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +3,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getSteps());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +4,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_win_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +5,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_death_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +6,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_hurt_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +7,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_kill_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +8,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_elapsed_frame_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +9,  ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_right_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +10, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_left_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +11, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_up_count());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +12, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_up_count());

            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +14, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_win_count() * rewards.get(0).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +15, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_death_count() * rewards.get(1).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +16, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_hurt_count() * rewards.get(2).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +17, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_kill_count() * rewards.get(3).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +18, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_elapsed_frame_count() * rewards.get(4).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +19, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_right_count() * rewards.get(5).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +20, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_left_count() * rewards.get(6).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +21, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_up_count() * rewards.get(7).getReward());
            addNumber(sheet, COLUMN_VALUE_TRIES_BEGIN +22, ROW_VALUE_TRIES_BEGIN +1+i, tries.get(i).getReward_move_up_count() * rewards.get(8).getReward());
        }
    }

    /**********************************************************************************
     *  Statistics
     **********************************************************************************/

    public void createLabelStatistics(WritableSheet sheet) throws WriteException {
        // Wins
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN, ROW_STATISTIC_WINS_BEGIN, "WINS");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN, ROW_STATISTIC_WINS_BEGIN + 1, "Max [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN + 1, ROW_STATISTIC_WINS_BEGIN + 1, "Min [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN + 2, ROW_STATISTIC_WINS_BEGIN + 1, "Durchschnitt [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN + 3, ROW_STATISTIC_WINS_BEGIN + 1, "Gesamt [Win]");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN + 4, ROW_STATISTIC_WINS_BEGIN + 1, "Prozent [Win]");
        addCaption(sheet, COLUMN_STATISTIC_WINS_BEGIN + 5, ROW_STATISTIC_WINS_BEGIN + 1, "Max in Folge [Win]");
        // Loss
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN, ROW_STATISTIC_LOSS_BEGIN, "LOSS");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN, ROW_STATISTIC_LOSS_BEGIN + 1, "Max [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 1, ROW_STATISTIC_LOSS_BEGIN + 1, "Min [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 2, ROW_STATISTIC_LOSS_BEGIN + 1, "Durchschnitt [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 3, ROW_STATISTIC_LOSS_BEGIN + 1, "Gesamt [loss]");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 4, ROW_STATISTIC_LOSS_BEGIN + 1, "Prozent [loss]");
        addCaption(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 5, ROW_STATISTIC_LOSS_BEGIN + 1, "Max in Folge [loss]");
        // Rewards
        addCaption(sheet, COLUMN_STATISTIC_REWARDS_BEGIN, ROW_STATISTIC_REWARDS_BEGIN, "REWARDS");
        addCaption(sheet, COLUMN_STATISTIC_REWARDS_BEGIN, ROW_STATISTIC_REWARDS_BEGIN + 1, "Max [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_REWARDS_BEGIN + 1, ROW_STATISTIC_REWARDS_BEGIN + 1, "Min [Reward]");
        addCaption(sheet, COLUMN_STATISTIC_REWARDS_BEGIN + 2, ROW_STATISTIC_REWARDS_BEGIN + 1, "Durchschnitt [Reward]");
        // Steps
        addCaption(sheet, COLUMN_STATISTIC_STEPS_BEGIN, ROW_STATISTIC_STEPS_BEGIN, "STEPS");
        addCaption(sheet, COLUMN_STATISTIC_STEPS_BEGIN, ROW_STATISTIC_STEPS_BEGIN + 1, "Max [Step]");
        addCaption(sheet, COLUMN_STATISTIC_STEPS_BEGIN + 1, ROW_STATISTIC_STEPS_BEGIN + 1, "Min [Step]");
        addCaption(sheet, COLUMN_STATISTIC_STEPS_BEGIN + 2, ROW_STATISTIC_STEPS_BEGIN + 1, "Durchschnitt [Step]");

    }
    public void createContentStatistics(WritableSheet sheet, int tryCount) throws WriteException {
        double winMax = 0;
        double winMin = 0;
        double winAverage = 0;
        double winGesamtInt = 0;
        int winInFolge = 0;
        int winMaxInFolge = 0;
        int lossInFolge = 0;
        int lossMaxInFolge = 0;
        double lossMax = 0;
        double lossMin = 0;
        double lossAverage = 0;
        double rewardMax = 0;
        double rewardMin = 0;
        double rewardAverage = 0;
        double stepMax = 0;
        double stepMin = 0;
        double stepAverage = 0;

        boolean firstWin = true;
        boolean firstLoss = true;
        boolean first = true;
        for (int i = ROW_VALUE_TRIES_BEGIN+1; i < ROW_VALUE_TRIES_BEGIN+1+tryCount; i++){
            double winValue = Double.parseDouble(sheet.getCell(COLUMN_VALUE_TRIES_BEGIN+1,i).getContents().replace(",","."));
            double reward = Double.parseDouble(sheet.getCell(COLUMN_VALUE_TRIES_BEGIN+2,i).getContents().replace(",","."));
            int step = Integer.parseInt(sheet.getCell(COLUMN_VALUE_TRIES_BEGIN+3,i).getContents());

            if (winValue == 1){

                winInFolge++;
                winMax = (firstWin) ? reward : Math.max(winMax, reward);
                winMin = (firstWin) ? reward : Math.min(winMin, reward);
                winAverage += reward;

                lossInFolge = 0;
                firstWin = false;
            }else{
                winInFolge = 0;

                lossMax = (firstLoss) ? reward : Math.max(lossMax, reward);
                lossMin = (firstLoss) ? reward : Math.min(lossMin, reward);
                lossAverage += reward;
                lossInFolge++;
                firstLoss = false;
            }

            winGesamtInt += winValue;
            winMaxInFolge = Math.max(winInFolge,winMaxInFolge);

            lossMaxInFolge = Math.max(lossInFolge,lossMaxInFolge);

            rewardMax = (first) ? reward : Math.max(reward,rewardMax);
            rewardMin = (first) ? reward : Math.min(reward,rewardMin);
            rewardAverage += reward;

            stepMax = (first) ? step : Math.max(step,stepMax);
            stepMin = (first) ? step : Math.min(step,stepMin);
            stepAverage += step;
            first = false;
        }
        double winProzent = ((winGesamtInt/tryCount)*100);
        winAverage = winAverage/tryCount;
        double lossGesamtInt = tryCount-winGesamtInt;
        double lossProzent = ((lossGesamtInt/tryCount)*100);
        lossAverage= lossAverage/tryCount;
        rewardAverage = rewardAverage/tryCount;
        stepAverage = stepAverage/tryCount;

        // Wins
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN    , ROW_STATISTIC_WINS_BEGIN + 2, winMax);
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN + 1, ROW_STATISTIC_WINS_BEGIN + 2, winMin);
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN + 2, ROW_STATISTIC_WINS_BEGIN + 2, winAverage);
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN + 3, ROW_STATISTIC_WINS_BEGIN + 2, winGesamtInt);
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN + 4, ROW_STATISTIC_WINS_BEGIN + 2, winProzent);
        addNumber(sheet, COLUMN_STATISTIC_WINS_BEGIN + 5, ROW_STATISTIC_WINS_BEGIN + 2, winMaxInFolge);
        // Loss
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN    , ROW_STATISTIC_LOSS_BEGIN + 2, lossMax);
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 1, ROW_STATISTIC_LOSS_BEGIN + 2, lossMin);
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 2, ROW_STATISTIC_LOSS_BEGIN + 2, lossAverage);
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 3, ROW_STATISTIC_LOSS_BEGIN + 2, lossGesamtInt);
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 4, ROW_STATISTIC_LOSS_BEGIN + 2, lossProzent);
        addNumber(sheet, COLUMN_STATISTIC_LOSS_BEGIN + 5, ROW_STATISTIC_LOSS_BEGIN + 2, lossMaxInFolge);
        // Rewards
        addNumber(sheet, COLUMN_STATISTIC_REWARDS_BEGIN    , ROW_STATISTIC_REWARDS_BEGIN + 2, rewardMax);
        addNumber(sheet, COLUMN_STATISTIC_REWARDS_BEGIN + 1, ROW_STATISTIC_REWARDS_BEGIN + 2, rewardMin);
        addNumber(sheet, COLUMN_STATISTIC_REWARDS_BEGIN + 2, ROW_STATISTIC_REWARDS_BEGIN + 2, rewardAverage);
        // Steps
        addNumber(sheet, COLUMN_STATISTIC_STEPS_BEGIN    , ROW_STATISTIC_STEPS_BEGIN + 2, stepMax);
        addNumber(sheet, COLUMN_STATISTIC_STEPS_BEGIN + 1, ROW_STATISTIC_STEPS_BEGIN + 2, stepMin);
        addNumber(sheet, COLUMN_STATISTIC_STEPS_BEGIN + 2, ROW_STATISTIC_STEPS_BEGIN + 2, stepAverage);

    }

    /**********************************************************************************
     *  Private Methoden
     **********************************************************************************/


    private void addCaption(WritableSheet sheet, int column, int row, String s)
            throws  WriteException {
        Label label;
        label = new Label(column, row, s, timesBoldUnderline);
        sheet.addCell(label);
    }

    private void addNumber(WritableSheet sheet, int column, int row,
                           Integer integer) throws WriteException {
        Number number;
        number = new Number(column, row, integer, times);
        sheet.addCell(number);
    }
    private void addNumber(WritableSheet sheet, int column, int row,
                           double rewards) throws WriteException  {
        Number number;
        number = new Number(column, row, rewards, times);
        sheet.addCell(number);
    }

    private void addLabel(WritableSheet sheet, int column, int row, String s)
            throws WriteException {
        Label label;
        label = new Label(column, row, s, times);
        sheet.addCell(label);
    }
}