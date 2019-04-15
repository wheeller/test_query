import org.telegram.telegrambots.bots.*;
import org.telegram.telegrambots.meta.api.objects.*;
import org.telegram.telegrambots.meta.api.methods.send.*;
import org.telegram.telegrambots.meta.exceptions.*;
import org.telegram.abilitybots.api.bot.*;

import org.telegram.telegrambots.meta.logging.BotLogger;

import java.util.ArrayList;
import java.util.List;

import org.telegram.telegrambots.meta.api.objects.replykeyboard.*;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.*;

//public class MDTestBench_bot extends TelegramLongPollingBot {
public class MDTestBench_bot extends AbilityBot {
    private String botToken;
    private String botUsername;
    private PostgreConnection db;

    private final static String start_text = "Вас приветствует бот управления доступом на тестовый стенд";
    private final static String def_msg = "Выберите действие";
//    private final static String def_msg_queue = "Вы в очереди. Выберите действие";
    private final static String bad_value = "Введено некорректное значение";
    private final static int max_work_time = 2880;
    private int request = 0; // идентификатор запроса: 1 - удаление из очереди, 0 - нет реакции

    private final static String user_state_not_exist = "not_exist";
    private final static String user_state_empty = "empty";
    private final static String user_state_queue = "queue";
    private final static String user_state_work = "work";
    private final static String user_state_error = "error";

    MDTestBench_bot(String botToken, String botUsername, DefaultBotOptions botOptions, PostgreConnection db) {
        super(botToken, botUsername, botOptions);
        this.botToken = botToken;
        this.botUsername = botUsername;
        this.db = db;
    }

    public int creatorId() {
        return 0;
    }

    @Override
    public void onUpdateReceived(Update update) {

        System.out.println("\n" + update.toString()); // инфа об обновлении

        long id_telegram = update.getMessage().getFrom().getId();
        long chat_id = update.getMessage().getChatId();
        int id_user = db.getIdUser(id_telegram);
        int minutes;
        int remain_minutes;

        String user_state; // not_exist ,empty , queue, work
        user_state = db.getUserState(id_user);

        if (user_state.equals(user_state_not_exist)) {
            // добавить пользователя в базу
            sendMsg("Вас нет в базе данных, добавляю", chat_id, user_state);
            String user_name = update.getMessage().getFrom().getFirstName() + " " + update.getMessage().getFrom().getLastName();
            db.add_user(user_name, id_telegram);
            user_state = user_state_empty;
        }

        if (user_state.equals(user_state_error)) {
            System.out.println("Switch user_state error");
            return;
        }

        // если в обновлении пришло текстовое сообщение
        if (update.hasMessage() && update.getMessage().hasText()) {
            String message_text = update.getMessage().getText();
            System.out.println(message_text);

            switch (message_text) {
                case "/start":
                    sendMsg(start_text, chat_id, user_state);
                    break;
                case "Запрос стенда":
                    if (!user_state.equals(user_state_empty)) break;
                    askBenchQuery(chat_id);
                    break;
                case "Оставшееся время":
                    if (user_state.equals(user_state_empty)) break;
                    remain_minutes = db.getTimeRemain(id_user, "id_user");
                    if (remain_minutes > 0) {
                        sendMsg("Осталось " + remain_minutes + " минут", chat_id, user_state);
                    } else { // ошибка
                        System.out.println("case \"Оставшееся время\" - ошибка, id_user = " + id_user + ": remain_minutes = " + remain_minutes);
                    }
                    break;
                case "Продлить":
                    if (user_state.equals(user_state_empty)) break;
                    askBenchQuery(chat_id);
                    break;
                case "Освободить стенд":
                    if (user_state.equals(user_state_empty)) break;
                    request = 1;
                    askYesNo(id_telegram);
                    break;
                case "Список стендов":
                    sendMsg(db.getBenchInfo(), chat_id, user_state);
                    break;
                case "Показать очередь":
                    sendMsg(db.getQueryInfo(), chat_id, user_state);
                    break;
                case "Да":
                    if (request == 1) { // удалене из очереди
                        request = 0;
                        if (db.freeQuery(id_user)) {
                            checkUpdateDB();
                            user_state = "empty";
                            sendMsg("Ваш запрос удален", chat_id, user_state);
                        }
                        else System.out.println("Ошибка удаления из очереди");
                    } else System.out.println("Игнорируем 'Да' request = " + request);
                    break;
                case "Нет":
                    request = 0;
                    sendMsg(def_msg, chat_id, user_state);
                    break;
                default:
                    // новая/продление очереди в минутах
                    if (message_text.contains("минут")) {
                        try {
                            // проверка ввода на корректность
                            minutes = new Integer(message_text.toLowerCase().split("минут")[0].replace(" ", ""));
                            if (!checkMinutesValue(minutes)) {
                                sendMsg(bad_value, chat_id, user_state);
                                break;
                            }

                            // новая заявка
                            if (user_state.equals(user_state_empty)) {
                                if (db.setNewQuery(id_user, minutes)){
                                    sendMsg("Ваш запрос добавлен", chat_id, user_state);
                                    checkUpdateDB();
                                }
                                else throw new RuntimeException("Ошибка setNewQuery");
                            }

                            // пользователь в работе
                            if (user_state.equals(user_state_work)) {
                                remain_minutes = db.getTimeRemain(id_user, "id_user"); // до продления
                                if (checkMinutesValue(remain_minutes + minutes)) {
                                    if (db.delayWorkQuery(id_user, minutes)) {
                                        remain_minutes = db.getTimeRemain(id_user, "id_user");
                                        sendMsg("Осталось " + remain_minutes + " минут", chat_id, user_state);
                                        sendBroadcastMsg(db.getUserName(id_user, "id_user") + " продлил работу на " + minutes + " минут");
                                        break;
                                    } else
                                        throw new RuntimeException("Ошибка delayWorkQuery");
                                } else {
                                    sendMsg("Сумарно запрошенное время " + (remain_minutes + minutes) +
                                            " за границей 0 - " + max_work_time, chat_id, user_state);
                                    break;
                                }
                            }
                        } catch (NumberFormatException e) {
                            sendMsg(bad_value, chat_id, user_state);
                            e.printStackTrace();
                            return;
                        } catch (RuntimeException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                    // показываем менюшку
                    user_state = db.getUserState(id_user);
                    sendMsg(def_msg, chat_id, user_state);
                    break;
            }
        }
    }

    private boolean checkMinutesValue(int minutes) {
        // проверка на корректность
        if (minutes < 1 || minutes > max_work_time) {
            System.out.println("Некорректное значение запрашиваемого времени minutes " + minutes);
            return false;
        }
        return true;
    }

    private synchronized void sendMsg(String msg, long chat_id, String user_state) {
        System.out.println("Сообщение для " + db.getUserName(db.getIdUser(chat_id), "id_user") + ": " + msg);
        SendMessage message = new SendMessage() // Create a message object object
                .enableMarkdown(true)
                .setChatId(chat_id)
                .setText(msg);
        // Выбор клавиатуры
        switch (user_state) {
            case user_state_empty:
                setButtonsStartMenu(message);
                break;
            case user_state_queue:
                setButtonsQueueMenu(message);
                break;
            case user_state_work:
                setButtonsWorkMenu(message);
                break;
            default:
                // не устанавливаем клавиатуру
                break;
        }
        try {
            execute(message); // Sending our message object to user
        } catch (TelegramApiException e) {
            BotLogger.error("Could not send message", "send_msg", e);
            e.printStackTrace();
        }
    }

    @Override
    public String getBotUsername() {
        // Return bot username
        // If bot username is @MyAmazingBot, it must return 'MyAmazingBot'
        return botUsername;
    }

    @Override
    public String getBotToken() {
        // Return bot token from BotFather
        return botToken;
    }

    // Клавиатура начального экрана
    private synchronized void setButtonsStartMenu(SendMessage sendMessage) {
        // Создаем клавиуатуру
        ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
        sendMessage.setReplyMarkup(replyKeyboardMarkup);
        replyKeyboardMarkup.setSelective(true);
        replyKeyboardMarkup.setResizeKeyboard(true);
        replyKeyboardMarkup.setOneTimeKeyboard(false);

        // Создаем список строк клавиатуры
        List<KeyboardRow> keyboard = new ArrayList<>();

        // Первая строчка клавиатуры
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        // Добавляем кнопки в первую строчку клавиатуры
        keyboardFirstRow.add(new KeyboardButton("Запрос стенда"));
        keyboardFirstRow.add(new KeyboardButton("Список стендов"));
        keyboardFirstRow.add(new KeyboardButton("Показать очередь"));

        // Добавляем все строчки клавиатуры в список
        keyboard.add(keyboardFirstRow);

        // и устанваливаем этот список нашей клавиатуре
        replyKeyboardMarkup.setKeyboard(keyboard);
    }

    // Клавиатура очередника
    private synchronized void setButtonsQueueMenu(SendMessage sendMessage) {
        ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
        sendMessage.setReplyMarkup(replyKeyboardMarkup);
        replyKeyboardMarkup.setSelective(true);
        replyKeyboardMarkup.setResizeKeyboard(true);
        replyKeyboardMarkup.setOneTimeKeyboard(false);
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        keyboardFirstRow.add(new KeyboardButton("Освободить стенд"));
        keyboardFirstRow.add(new KeyboardButton("Список стендов"));
        keyboardFirstRow.add(new KeyboardButton("Показать очередь"));
        keyboard.add(keyboardFirstRow);
        replyKeyboardMarkup.setKeyboard(keyboard);
    }

    // Клавиатура для тех, кто работает
    private synchronized void setButtonsWorkMenu(SendMessage sendMessage) {
        // Создаем клавиуатуру
        ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
        sendMessage.setReplyMarkup(replyKeyboardMarkup);
        replyKeyboardMarkup.setSelective(true);
        replyKeyboardMarkup.setResizeKeyboard(true);
        replyKeyboardMarkup.setOneTimeKeyboard(false);
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        keyboardFirstRow.add(new KeyboardButton("Оставшееся время"));
        keyboardFirstRow.add(new KeyboardButton("Продлить"));
        keyboardFirstRow.add(new KeyboardButton("Освободить стенд"));
        KeyboardRow keyboardSecondRow = new KeyboardRow();
        keyboardSecondRow.add(new KeyboardButton("Список стендов"));
        keyboardSecondRow.add(new KeyboardButton("Показать очередь"));
        keyboard.add(keyboardFirstRow);
        keyboard.add(keyboardSecondRow);
        replyKeyboardMarkup.setKeyboard(keyboard);
    }

    // Создаем клавиуатуру для запроса очереди
    private synchronized void setButtonsQueryTime(SendMessage sendMessage) {
        ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
        sendMessage.setReplyMarkup(replyKeyboardMarkup);
        replyKeyboardMarkup.setSelective(true);
        replyKeyboardMarkup.setResizeKeyboard(true);
        replyKeyboardMarkup.setOneTimeKeyboard(false);

        // Создаем список строк клавиатуры
        List<KeyboardRow> keyboard = new ArrayList<>();

        // Первая строчка клавиатуры
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        // Добавляем кнопки в первую строчку клавиатуры
        keyboardFirstRow.add(new KeyboardButton("10 минут"));
        keyboardFirstRow.add(new KeyboardButton("20 минут"));
        keyboardFirstRow.add(new KeyboardButton("30 минут"));

        // Вторая строчка клавиатуры
        KeyboardRow keyboardSecondRow = new KeyboardRow();
        // Добавляем кнопки во вторую строчку клавиатуры
        keyboardSecondRow.add(new KeyboardButton("60 минут"));
        keyboardSecondRow.add(new KeyboardButton("120 минут"));
        keyboardSecondRow.add(new KeyboardButton("назад"));

        // Добавляем все строчки клавиатуры в список
        keyboard.add(keyboardFirstRow);
        keyboard.add(keyboardSecondRow);

        // и устанваливаем этот список нашей клавиатуре
        replyKeyboardMarkup.setKeyboard(keyboard);
    }

    // Создаем клавиуатуру для запроса подтверждения
    private synchronized void setButtonsYesNo(SendMessage sendMessage) {
        ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
        sendMessage.setReplyMarkup(replyKeyboardMarkup);
        replyKeyboardMarkup.setSelective(true);
        replyKeyboardMarkup.setResizeKeyboard(true);
        replyKeyboardMarkup.setOneTimeKeyboard(false);
        List<KeyboardRow> keyboard = new ArrayList<>();
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        keyboardFirstRow.add(new KeyboardButton("Да"));
        keyboardFirstRow.add(new KeyboardButton("Нет"));
        keyboard.add(keyboardFirstRow);
        replyKeyboardMarkup.setKeyboard(keyboard);
    }

    // запрос подтверждения
    private void askYesNo(long chat_id) {
        SendMessage message = new SendMessage()
                .enableMarkdown(true)
                .setChatId(chat_id)
                .setText("Подтвердите действие");
        // Кастомная клавиатура
        setButtonsYesNo(message);
        try {
            execute(message); // Sending our message object to user
        } catch (TelegramApiException e) {
            BotLogger.error("Could not send message", "send_msg", e);
            e.printStackTrace();
        }
    }

    // Запрос очереди, выбор времени (продолжительность)
    private synchronized void askBenchQuery(long chat_id) {
        SendMessage message = new SendMessage()
                .enableMarkdown(true)
                .setChatId(chat_id)
                .setText("Выберете желаемое время или отправьте сообщение вида 'N минут'");
        // Кастомная клавиатура
        setButtonsQueryTime(message);
        try {
            execute(message); // Sending our message object to user
        } catch (TelegramApiException e) {
            BotLogger.error("Could not send message", "send_msg", e);
            e.printStackTrace();
        }
    }

    private synchronized void sendBroadcastMsg(String msg) {
        long[] arr_id_telegram = db.getIdTelegram();
        for (long id_telegram : arr_id_telegram) {
            SendMessage message = new SendMessage()
                    .enableMarkdown(true)
                    .setChatId(id_telegram)
                    .setText(msg);
            try {
                execute(message); // Sending our message object to user
            } catch (TelegramApiRequestException e) {
                System.out.println("sendBroadcastMsg: Не могу отправить сообщение id_telegram = " + id_telegram);
                e.printStackTrace();
            } catch (TelegramApiException e) {
                BotLogger.error("Could not send message", "send_msg", e);
                e.printStackTrace();
            }
        }
    }

    void checkUpdateDB() {
        try {
            // удаление выполненных запросов
            int id_user;
            ArrayList<Integer> arrayList_id_user;

            arrayList_id_user = db.freeExpiredQuery();
            if (arrayList_id_user != null)
                arrayList_id_user.forEach(val -> sendMsg("Время работы по Вашей заявке истекло!", db.getIdTelegram(val), user_state_empty));


            arrayList_id_user = db.checkFiveMinRemain();
            if (arrayList_id_user != null)
                arrayList_id_user.forEach(val -> sendMsg("Осталось 5 минут до завершения работы", db.getIdTelegram(val), user_state_queue));

            // новые запросы на свободные сервера
            int[] ret;
            int id_server;
            do {
                ret = db.setQueryToServer();
                id_user = ret[0];
                id_server = ret[1];
                if (id_user > 0) {
                    sendBroadcastMsg(db.getUserName(id_user, "id_user") + " занял " + db.getHostname(id_server) +
                            " на " + db.getTimeRemain(id_user, "id_user") + " минут");
                }
            } while (id_user > 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}