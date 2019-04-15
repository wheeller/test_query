import java.io.*;
import java.util.Properties;

public class Server {
    public static void main(String[] args) {

        String db_url;
        String db_user;
        String db_pass;

        String bot_name;
        String bot_token;

        String proxy_host;
        int proxy_port;
        String proxy_user;
        String proxy_pass;

        String conf_path = null;

        Options CurOpts = new Options();

        // parse arguments
        CurOpts.Parse(args);
//        CurOpts.PrintOpts();

        for (OptElement x : CurOpts.optsList) {
            if (x.flag.equals("-c")) {
                conf_path = x.opt;
            }
        }
/*
        try {
            System.out.println("pwd = " + new File(".").getCanonicalPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
*/
        System.out.println("Running server...");

        FileInputStream fis;
        Properties property = new Properties();

        try {
            fis = new FileInputStream(conf_path);
            property.load(fis);

            db_url = property.getProperty("db.url");
            db_user = property.getProperty("db.user");
            db_pass = property.getProperty("db.pass");

            bot_name = property.getProperty("bot.name");
            bot_token = property.getProperty("bot.token");

            proxy_host = property.getProperty("proxy.host");
            proxy_port = Integer.parseInt(property.getProperty("proxy.port"));

            proxy_user = property.getProperty("proxy.user");
            proxy_pass = property.getProperty("proxy.pass");


            System.out.println("url = " + db_url
                    + ", user = " + db_user
                    + ", pass = " + db_pass
                    + ", proxy_host = " + proxy_host
                    + ", proxy_port = " + proxy_port);

        } catch (IOException e) {
            System.err.println("Конфигурационный файл отсуствует!");
            return;
        } catch (NumberFormatException e) {
            System.err.println("Ошибка числового формата параметра port в конфиге");
            return;
        }

        // Коннектимся к БД
        PostgreConnection db = new PostgreConnection(db_url, db_user, db_pass);
        db.start();
        db.connect();

        // Запуск Телеграммного бота
        RunBot runBot = new RunBot(db, bot_name, bot_token, proxy_host, proxy_port, proxy_user, proxy_pass);
        runBot.runTimer();
        System.out.println("Bot started");
    }
}
