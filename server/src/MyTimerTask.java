
import java.util.Date;
import java.util.TimerTask;

public class MyTimerTask extends TimerTask {
    private PostgreConnection db;
    private MDTestBench_bot bot;

    MyTimerTask(PostgreConnection db, MDTestBench_bot bot) {
        this.db = db;
        this.bot = bot;
    }

    @Override
    public void run() {
        System.out.println("\nTimerTask проснулся " + new Date());
        bot.checkUpdateDB();
    }
}