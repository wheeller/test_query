import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.bots.DefaultBotOptions;
import org.telegram.telegrambots.meta.ApiContext;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.util.Timer;
import java.util.TimerTask;

class RunBot {
    private PostgreConnection db;
    MDTestBench_bot bot;

    RunBot(PostgreConnection db, String bot_name, String bot_token, String proxy_host, int proxy_port, String proxy_user, String proxy_pass) {
        this.db = db;
        try {
            // Create the Authenticator that will return auth's parameters for proxy authentication
/*            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(PROXY_USER, PROXY_PASSWORD.toCharArray());
                }
            });*/

            // Initialize Api Context
            ApiContextInitializer.init();

            // Instantiate Telegram Bots API
            TelegramBotsApi botsApi = new TelegramBotsApi();

            // Set up Http proxy
            DefaultBotOptions botOptions = ApiContext.getInstance(DefaultBotOptions.class);

            botOptions.setProxyHost(proxy_host);
            botOptions.setProxyPort(proxy_port);
            // Select proxy type: [HTTP|SOCKS4|SOCKS5] (default: NO_PROXY)
            botOptions.setProxyType(DefaultBotOptions.ProxyType.SOCKS5);

            // Register your newly created AbilityBot
            bot = new MDTestBench_bot(bot_token, bot_name, botOptions, db);

            botsApi.registerBot(bot);

        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    void runTimer() {
        // Далее крутимся в планировщике, отвлекаясь на обновления бота
        TimerTask timerTask = new MyTimerTask(db, bot);

        // стартуем TimerTask как поток
        Timer timer = new Timer(false);

        // будем запускать каждые 60 секунд
        timer.scheduleAtFixedRate(timerTask, 0, 60 * 1000);
        System.out.println("TimerTask started");
    }
}