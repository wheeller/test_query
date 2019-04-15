import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class PostgreConnection extends Thread {

    //  Database credentials
    private String url;
    private String user;
    private String pass;

    private Connection connection;

    PostgreConnection() {
        setName("PostgresConnection");
        connection = null;
        System.out.println("Testing connection to PostgreSQL JDBC");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }
        System.out.println("PostgreSQL JDBC Driver successfully connected");
    }

    PostgreConnection(String url, String user, String pass) {
        this.url = url;
        this.user = user;
        this.pass = pass;

        setName("PostgresConnection");
        connection = null;
        System.out.println("Testing connection to PostgreSQL JDBC");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }
        System.out.println("PostgreSQL JDBC Driver successfully connected");
    }

    @Override
    public void run() {
    }

    /**
     * Connect to the PostgreSQL database
     * return a Connection object
     */
    void connect() throws RuntimeException {
        try {
            connection = DriverManager.getConnection(url, user, pass);
        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return;
        }
        if (connection != null) System.out.println("You successfully connected to database now");
        else {
            System.out.println("Failed to make connection to database");
            throw new RuntimeException("Fatal error");
        }
    }

    // добавить пользователя в базу
    // return id_user
    long add_user(String user_name, long id_telegram) {
        String SQL = "INSERT INTO users (user_name, id_telegram) VALUES(?,?)";
        long id = 0;
        if (connection == null) connect();
        try (PreparedStatement pstmt = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, user_name);
            pstmt.setLong(2, id_telegram);
            int affectedRows = pstmt.executeUpdate();
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                        System.out.println("User added to DB");
                    }
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return id;
    }


    // получить информацию по стенду
    String getBenchInfo() {
        String info = "";
        int id_query;
        int minutes;
        String SQL = "SELECT * FROM servers ORDER BY id_server";
        if (connection == null) connect();

        try (PreparedStatement pstmt = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                // hostname: user_name, remain_minutes
                info += rs.getString("hostname") + ": ";
                id_query = rs.getInt("id_query");
                // сервер в работе
                if (id_query > 0) {
                    minutes = getTimeRemain(id_query, "id_query");
                    info += getUserName(id_query, "id_query") + ", осталось " + minutes + " минут\n";
                } else info += "свободен\n";
            }
            if (info.isEmpty()) info = "Пусто";
            System.out.print(info);
            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return info;
    }

    String getHostname(int id_server) {
        String hostname = "";
        if (connection == null) connect();
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM servers " +
                    "WHERE id_server = " + id_server);
            if (result.next()) {
                hostname = result.getString("hostname");
            }
            if (hostname.isEmpty()) {
                System.out.println("Сервер id_server = " + id_server + " не найден");
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return hostname;
    }

    // получить информацию по очереди
    String getQueryInfo() {
        String info = "";
        String SQL = "SELECT * FROM query ORDER BY id_query";
        if (connection == null) connect();

        try (PreparedStatement pstmt = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String userName = getUserName(rs.getInt("id_user"), "id_user");
                String status = rs.getString("status");
                int queryTime = rs.getInt("query_time");

                if (status.equals("work")) {
                    info += userName + ", в работе, осталось " +
                            getTimeRemain(rs.getInt("id_query"), "id_query") + " минут\n";
                } else {
                    info += userName + ", новая на " + queryTime + " минут, ожидает " +
                            getTimeWait(rs.getInt("id_query")) + " минут\n";
                }
            }
            if (info.isEmpty()) info = "Заявок нет";
            System.out.print(info);
            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return null;
        }
        return info;
    }

    // получить имя пользователя по ключу и его значению
    String getUserName(long val, String key) {
        String SQL;
        String userName = "";
        if (connection == null) connect();
        switch (key) {
            case "id_user":
            case "id_telegram":
                SQL = "SELECT user_name FROM users WHERE " + key + " = " + val;
                try (Statement pstmt = connection.createStatement()) {
                    ResultSet rs = pstmt.executeQuery(SQL);
                    if (rs.next()) userName = rs.getString("user_name");

                    rs.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
                break;

            case "id_query":
                SQL = "SELECT query.id_query, users.user_name\n" +
                        "FROM query LEFT OUTER JOIN users ON (query.id_user = users.id_user)\n" +
                        "WHERE " + key + " = " + val;
                try (Statement pstmt = connection.createStatement()) {
                    ResultSet rs = pstmt.executeQuery(SQL);
                    if (rs.next()) userName = rs.getString("user_name");
                    rs.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
                break;

            default:
                try {
                    throw new SQLException("getUserName: запрос " + key + " не реализован");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return userName;
    }

    // получить id пользователя из БД по id телеграма
    int getIdUser(long id_telegram) {
        String SQL = "SELECT id_user FROM users WHERE id_telegram = ?";
        if (connection == null) connect();
        try (PreparedStatement pstmt = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setLong(1, id_telegram);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return -1;
    }

    String getUserState(int id_user) {
        if (id_user < 0) return "not_exist";
        String user_state;
        String status = "";

        if (connection == null) connect();
        // читаем заказанное время из базы
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE id_user = " + id_user);
            if (result.next()) {
                status = result.getString("status");
            }
            switch (status) {
                case "new":
                    user_state = "queue";
                    break;
                case "work":
                    user_state = "work";
                    break;
                default:
                    user_state = "empty";
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return "error";
        }
        return user_state;
    }

    // получить id телеграма из БД по id пользователя
    long getIdTelegram(int id_user) {
        String SQL = "SELECT id_telegram FROM users WHERE id_user = ?";
        if (connection == null) connect();
        try (PreparedStatement pstmt = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setInt(1, id_user);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("id_telegram");
            }
            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return -1;
    }

    // получить массив всех id_telegram
    long[] getIdTelegram() {
        ArrayList<Long> al_id_telegram = new ArrayList<>();
        if (connection == null) connect();
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id_telegram FROM users");
            while (rs.next()) {
                al_id_telegram.add(rs.getLong("id_telegram"));
            }
            rs.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return new long[]{-1};
        }
        int i = 0;
        long[] arr_id_telegram = new long[al_id_telegram.size()];
        for (long elem : al_id_telegram) {
            arr_id_telegram[i] = elem;
            System.out.println("id_telegram = " + elem);
            i++;
        }
        return arr_id_telegram;
    }

    // Добавить запрос в очередь
    boolean setNewQuery(int id_user, int minutes) {
        if (connection == null) connect();
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO query (id_user, query_time, status, create_date) VALUES(?,?,'new',?)")) {
            pstmt.setInt(1, id_user);
            pstmt.setInt(2, minutes);
            java.util.Date date = new java.util.Date();
            Timestamp curTime = new Timestamp(date.getTime());
            pstmt.setTimestamp(3, curTime);
            int rows = pstmt.executeUpdate();
            System.out.println("setNewQuery: Добавлено строк:" + rows);
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    // Продлить запрос в работе, новый запрос можно только удалить
    boolean delayWorkQuery(int id_user, int minutes) {
        int query_time = -1;
        int id_query = -1;
        if (connection == null) connect();
        // читаем заказанное время из базы
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query " +
                    "WHERE status = 'work' AND id_user = " + id_user);
            if (result.next()) {
                id_query = result.getInt("id_query");
                query_time = result.getInt("query_time");
            }
            if (id_query < 0 || query_time < 0) {
                System.out.println("delayWorkQuery: Нет заданий в работе от id_user = " + id_user);
                return false;
            } else
                System.out.println("delayWorkQuery: Найден запрос в работе: id_user = " + id_user + ", query_time = " + query_time);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        // продлеваем
        try (PreparedStatement pstmt = connection.prepareStatement("UPDATE query SET query_time = ? " +
                "WHERE id_query = ? AND status = 'work'")) {
            query_time += minutes;
            pstmt.setInt(1, query_time);
            pstmt.setInt(2, id_query);
            int rows = pstmt.executeUpdate();
            System.out.println("delayWorkQuery: Добавлено строк:" + rows);
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    // Установка нового запроса из очереди на свободный сервер
    int[] setQueryToServer() {
        int id_query = -1;
        int id_user = -1;
        int query_time = -1;
        int id_server = -1;
//        String hostname = null;
        if (connection == null) connect();
        // Ищем свободный сервер
        String SQL = "SELECT * FROM servers WHERE status = 'free'";
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(SQL);
            if (rs.next()) {
                id_server = rs.getInt("id_server");
//                hostname = rs.getString("hostname");
                rs.close();
            }
            if (id_server < 0) {
                System.out.println("setQueryToServer: Нет свободных серверов");
                return new int[]{-1, -1};
            } else System.out.println("setQueryToServer: Найден свободный сервер, id_server = " + id_server);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return new int[]{-1, -1};
        }
        // читаем новую запись в очереди с младшим id
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE status = 'new' ORDER BY id_query LIMIT 1");
            if (result.next()) {
                id_query = result.getInt("id_query");
                id_user = result.getInt("id_user");
                query_time = result.getInt("query_time");
            }
            if (id_query < 0 || id_user < 0 || query_time < 0) {
                System.out.println("setQueryToServer:Нет новых заданий");
                return new int[]{-1, -1};
            } else
                System.out.println("setQueryToServer: Найден новый запрос: id_query = " + id_query + "; id_user = " + id_user + "; query_time = " + query_time);
        } catch (SQLException e) {
            e.printStackTrace();
            return new int[]{-1, -1};
        }
        // добавляем новый запрос на сервер (обновляем строку)
        try (PreparedStatement pstmt = connection.prepareStatement("UPDATE servers SET status = ?, id_query = ? WHERE id_server = ?")) {
            pstmt.setString(1, "busy");
            pstmt.setInt(2, id_query);
            pstmt.setInt(3, id_server);
            int rows = pstmt.executeUpdate();
            if (rows > 0) System.out.println("setQueryToServer: Таблица servers: обновлено строк" + rows);
            else {
                System.out.println("setQueryToServer: Таблица servers НЕ обновлена");
                return new int[]{-1, -1};
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return new int[]{-1, -1};
        }
        // Меняем статус принятой заявки, вбиваем время начала работы
        try (PreparedStatement pstmt = connection.prepareStatement("UPDATE query SET status = 'work', begin_date = ?::timestamp WHERE id_query = ?")) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = new java.util.Date();
            pstmt.setString(1, dateFormat.format(date)); // Используем локальное время сервера
            pstmt.setInt(2, id_query);
            int rows = pstmt.executeUpdate();
            if (rows > 0) System.out.println("setQueryToServer: Таблица query: обновлено строк " + rows);
            else {
                System.out.println("setQueryToServer: Таблица query НЕ обновлена");
                return new int[]{-1, -1};
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return new int[]{-1, -1};
        }
        return new int[]{id_user, id_server};
    }

    // Ищем истекшие запросы в работе, перемещаем в историю, освобождаем сервер
    // return ArrayList of id_user из БД
    ArrayList<Integer> freeExpiredQuery() {
        ArrayList<Integer> arrayList_id_user = new ArrayList<>();
        int id_query = -1;
        int id_user = -1;
        int query_time = -1;
        int id_server = -1;
        Timestamp begin_date = null;

        if (connection == null) connect();

        // ищем заявку в работе
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE status = 'work' " +
                    "ORDER BY id_query");
            while (result.next()) {
                id_query = result.getInt("id_query");
                id_user = result.getInt("id_user");
                query_time = result.getInt("query_time");
                begin_date = result.getTimestamp("begin_date");

                if (id_query < 0 || id_user < 0 || query_time < 0) {
                    System.out.println("freeExpiredQuery: Нет заданий в работе");
                    return null;
                } else
                    System.out.println("freeExpiredQuery: Найден запрос в работе: id_query = " + id_query +
                            ", id_user = " + id_user + ", query_time = " + query_time + ", begin_date = " + begin_date.toString());

                // проверяем заявку, завершена или нет
                java.util.Date date = new java.util.Date();
                Timestamp curTime = new Timestamp(date.getTime());
                long work_minutes = (curTime.getTime() - begin_date.getTime()) / (60 * 1000); // милисекунды переводим в минуты
                System.out.println("freeExpiredQuery: в работе " + work_minutes + " минут");

                // если завершена
                if ((int) work_minutes >= query_time) {
                    if (cleanQueryDB(id_query, id_user, id_server, begin_date, "work")) arrayList_id_user.add(id_user);
                } else
                    System.out.println("freeExpiredQuery: заявка id_query = " + id_query + " не требует завершения, work_minutes = " + work_minutes);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return arrayList_id_user;
    }

    // Удаляем запрос пользователя, перемещаем в его в историю, освобождаем сервер
    boolean freeQuery(int id_user) {
        int id_query = -1;
        int query_time = -1;
        int id_server = -1;
        Timestamp begin_date = null;
        String status = "";
        if (connection == null) connect();
        // ищем заявку в работе
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE id_user = " + id_user);
            if (result.next()) {
                id_query = result.getInt("id_query");
                query_time = result.getInt("query_time");
                begin_date = result.getTimestamp("begin_date");
                status = result.getString("status");
            }
            if (id_query < 0) {
                System.out.println("freeQuery: Задание от id_user = " + id_user + " не найдено");
                return false;
            } else
                System.out.println("freeQuery: Найден запрос: id_query = " + id_query + ", id_user = " + id_user +
                        ", query_time = " + query_time + "status = " + status);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        // очистка задания
        return cleanQueryDB(id_query, id_user, id_server, begin_date, status);
    }

    boolean cleanQueryDB(int id_query, int id_user, int id_server, Timestamp begin_date, String... query_status_arg) {
        // очистка задания
        // читаем данные сервера
        String query_status;
        if (query_status_arg.length > 0) {
            query_status = query_status_arg[0];
        } else query_status = "";

        // если запрос в работе
        if (query_status.equals("work")) {
            try (Statement statement = connection.createStatement()) {
                ResultSet result = statement.executeQuery("SELECT * FROM servers WHERE id_query = " + id_query);
                if (result.next()) {
                    id_server = result.getInt("id_server");
                }
                if (id_server < 0) {
                    System.out.println("freeQuery: Ошибка, не найден сервер, выполнивший заявку id_query = " + id_query);
                    return false;
                } else
                    System.out.println("freeQuery: Найден сервер: id_server = " + id_server);
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
            // устанавливаем состояние сервера - свободен
            try (PreparedStatement pstmt = connection.prepareStatement("UPDATE servers SET status = 'free', id_query = NULL WHERE id_query = ?")) {
                pstmt.setInt(1, id_query);
                int rows = pstmt.executeUpdate();
                if (rows > 0) System.out.println("freeQuery: Таблица servers: обновлено строк " + rows);
                else {
                    System.out.println("freeQuery: Таблица servers НЕ обновлена");
                    return false;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                return false;
            }
            // заносим в историю
            try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO history (id_user, id_server, begin_ts, end_ts) VALUES(?,?,?,?)")) {
                pstmt.setInt(1, id_user);
                pstmt.setInt(2, id_server);
                pstmt.setTimestamp(3, begin_date);
                java.util.Date date = new java.util.Date();
                Timestamp curTime = new Timestamp(date.getTime());
                pstmt.setTimestamp(4, curTime);
                int rows = pstmt.executeUpdate();
                if (rows > 0) System.out.println("freeQuery: Таблица history: добавлено строк " + rows);
                else {
                    System.out.println("freeQuery: Таблица history НЕ обновлена");
                    return false;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        // удаляем запрос из очереди
        try (PreparedStatement pstmt = connection.prepareStatement("DELETE FROM query WHERE id_query = ?")) {
            pstmt.setInt(1, id_query);
            int rows = pstmt.executeUpdate();
            if (rows > 0) System.out.println("freeQuery: Таблица query: удалено строк " + rows);
            else {
                System.out.println("freeQuery: Таблица query НЕ обновлена");
                return false;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    int getTimeRemain(int val, String column) {
        int query_time = -1;
        Timestamp begin_date = null;
        String status = "";
        // ищем заявку в работе
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE " + column + " = " + val);
            if (result.next()) {
                query_time = result.getInt("query_time");
                begin_date = result.getTimestamp("begin_date");
                status = result.getString("status");
            }
            System.out.println("getTimeRemain: запрос " + column + " = " + val +
                    "; ответ: query_time = " + query_time + ", begin_date = " + begin_date + ", status = " + status);
            // запрос не вернул результат
            if (query_time < 0) {
                throw new SQLException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

        // Для нового запроса возвращаем запрошенное время
        if (status.equals("new")) return query_time;

        if (begin_date == null) {
            System.out.println("getTimeRemain: begin_date == null!!!");
            return -1;
        }

        // проверяем заявку, завершена или нет
        java.util.Date date = new java.util.Date();
        Timestamp curTime = new Timestamp(date.getTime());
        int remain_minutes = query_time - (int) ((curTime.getTime() - begin_date.getTime()) / (60 * 1000)); // милисекунды переводим в минуты
        System.out.println("getTimeRemain: Запрос " + column + " = " + val + " - осталось " + remain_minutes + " минут");
        return remain_minutes;
    }

    int getTimeWait(int id_query) {
        Timestamp create_date = null;
        // ищем заявку в работе
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE id_query = " + id_query);
            if (result.next()) {
                create_date = result.getTimestamp("create_date");
            }
            if (create_date == null) {
                System.out.println("getTimeWait: нет запроса в работе от id_query = " + id_query);
                return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        // проверяем заявку, завершена или нет
        java.util.Date date = new java.util.Date();
        Timestamp curTime = new Timestamp(date.getTime());
        int wait_minutes = (int) ((curTime.getTime() - create_date.getTime()) / (60 * 1000)); // милисекунды переводим в минуты
        System.out.println("getTimeWait: Запрос id_query = " + id_query + " в очереди" + wait_minutes + " минут");
        return wait_minutes;
    }

    // return ArrayList of <id_user>
    ArrayList<Integer> checkFiveMinRemain() {
        int id_query;
        int id_user;
        int query_time;
        Timestamp begin_date;
        ArrayList<Integer> arrayList_id_user = new ArrayList<>();

        if (connection == null) connect();

        // ищем заявку в работе
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM query WHERE status = 'work' ");
            while (result.next()) {
                id_query = result.getInt("id_query");
                id_user = result.getInt("id_user");
                query_time = result.getInt("query_time");
                begin_date = result.getTimestamp("begin_date");

                if (id_query < 0 || id_user < 0 || query_time < 0) {
                    System.out.println("checkFiveMinRemain: Нет заданий в работе");
                    return null;
                } else
                    System.out.println("checkFiveMinRemain: Найден запрос в работе: id_query = " + id_query + ", id_user = " + id_user +
                            ", query_time = " + query_time + ", begin_date = " + begin_date.toString());

                int time_remain = getTimeRemain(id_query, "id_query");
                if (time_remain == 5) {
                    arrayList_id_user.add(id_user);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
//      arrayList_id_user.forEach(val -> System.out.print(val + "; ")); // DEBUG
        if (arrayList_id_user.isEmpty()) return null;
        else {
            return arrayList_id_user;
        }
    }
}