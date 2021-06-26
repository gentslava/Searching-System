import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Crawler {
    private Connection conn; // соединение с БД в локальном файле
    private boolean debug;
    private int location;
    private List<String> filter_words = new ArrayList<String>();

    /* Конструктор инициализации паука с параметрами БД */
    protected Crawler(String fileName, boolean debug) throws SQLException {
        this.debug = debug;
        if (debug) System.out.println("Конструктор");

        String db_URL = "jdbc:sqlite:" + fileName; // формируемая строка для подключения к локальному файлу

        this.conn = DriverManager.getConnection(db_URL);

        this.conn.setAutoCommit(true); // включить режим автоматической фиксации (commit) изменений БД

        filter_words.addAll(Arrays.asList(new String[]{"в", "без", "до", "для", "за", "через", "над", "по", "из", "у", "около", "под", "о", "про", "на", "к", "перед", "при", "с", "между"}));
    }

    protected Crawler() {};

    /* Метод финализации работы с БД */
    protected void finalize() throws SQLException {
        conn.close(); // закрыть соединение
    }

    /* Удаление таблиц в БД */
    protected void dropDB() throws SQLException {
        if (debug) System.out.println("Удаление таблиц");

        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request;
        // Удалить таблицу wordList из БД
        request = "DROP TABLE IF EXISTS wordList;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу URLList из БД
        request = "DROP TABLE IF EXISTS URLList;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу wordLocation из БД
        request = "DROP TABLE IF EXISTS wordLocation;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу linkBetweenURL из БД
        request = "DROP TABLE IF EXISTS linkBetweenURL;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу linkWord из БД
        request = "DROP TABLE IF EXISTS linkWord;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
    }

    /* Инициализация таблиц в БД */
    protected void createIndexTables() throws SQLException {
        if (debug) System.out.println("Создание таблиц");

        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request;

        // 1. Таблица wordList -----------------------------------------------------
        // Создание таблицы wordList в БД
        request = "CREATE TABLE IF NOT EXISTS wordList(rowId INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT NOT NULL, isFiltred BOOLEAN DEFAULT FALSE);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос

        // 2. Таблица URLList -----------------------------------------------------
        // Создание таблицы URLList в БД
        request = "CREATE TABLE IF NOT EXISTS URLList(rowId INTEGER PRIMARY KEY AUTOINCREMENT, URL TEXT NOT NULL, isIndexed BOOLEAN DEFAULT FALSE);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос

        // 3. Таблица wordLocation -----------------------------------------------------
        // Создание таблицы wordLocation в БД
        request = "CREATE TABLE IF NOT EXISTS wordLocation(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_wordId INTEGER NOT NULL, fk_URLId INTEGER NOT NULL, location INTEGER NOT NULL);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос

        // 4. Таблица linkBetweenURL -----------------------------------------------------
        // Создание таблицы linkBetweenURL в БД
        request = "CREATE TABLE IF NOT EXISTS linkBetweenURL(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_FromURL_Id INTEGER NOT NULL, fk_ToURL_Id INTEGER NOT NULL);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос

        // 5. Таблица linkWord -----------------------------------------------------
        // Создание таблицы linkWord в БД
        request = "CREATE TABLE IF NOT EXISTS linkWord(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_wordId INTEGER NOT NULL, fk_linkId INTEGER NOT NULL);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
    }

    /* Вспомогательный метод для получения идентификатора, добавления и обновления записи */
    private int SelectInsertUpdate(String table, String field, String value, int num, boolean createNew, boolean updateVal) throws Exception {
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request;
        if (field.equals("LAST_INSERT_ROWID()")) { // получение Id последнего добавленного элемента
            request = "SELECT LAST_INSERT_ROWID();";
            if (debug) System.out.println("\t\t\t" + request);
            ResultSet resultRow = statement.executeQuery(request);
            if (resultRow.next()) return resultRow.getInt("LAST_INSERT_ROWID()");
        }
        if (!createNew && !updateVal) { // запрос Id элемента, отвечающего условиям
            request = "SELECT rowId FROM " + table + " WHERE ";
            String fields[] = field.split(", ");
            String values[] = value.split(", ");
            for (int i = 0; i < num; i++) {
                request += fields[i] + " = " + values[i];
                if (i + 1 != num) request += " AND ";
            }
            request += ";";
            if (debug) System.out.println("\t\t\t" + request);
            ResultSet resultRow = statement.executeQuery(request);
            if (resultRow.next()) return resultRow.getInt("rowId");
        } else if (!updateVal) { // занесение нового элемента в таблицу
            request = "INSERT INTO " + table + " (" + field + ") VALUES (" + value + ");";
            if (debug) System.out.println("\t\t\t" + request);
            statement.execute(request);
            return SelectInsertUpdate("", "LAST_INSERT_ROWID()", "", 0, false, false);
        } else { // обновления значений полей определенного элемента
            request = "UPDATE " + table + " SET ";
            String fields[] = field.split(", ");
            String values[] = value.split(", ");
            int i;
            for (i = 0; i < num; i++) {
                request += fields[i] + " = " + values[i];
                if (i + 1 != num) request += ", ";
            }
            request += " WHERE " + fields[i] + " = " + values[i] + ";";
            if (debug) System.out.println("\t\t\t" + request);
            statement.execute(request);
        }
        return -1;
    }

    /* Проиндексирован ли URL */
    private boolean isIndexed(String URL) throws Exception {
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request = "SELECT isIndexed FROM URLList WHERE URL = '" + URL + "';";
        if (debug) System.out.println("\t\t\t" + request);
        ResultSet resultRow = statement.executeQuery(request);
        if (resultRow.next() && resultRow.getBoolean("isIndexed")) return true;
        else return false;
    }

    /* Занесение слов в таблицы wordList и wordLocation */
    private void addWord(int URLId, String word) throws Exception {
        if (word.equals("")) return;
        int wordId = SelectInsertUpdate("wordList", "word", "'" + word + "'", 1, false, false);
        if (wordId == -1) {
            wordId = SelectInsertUpdate("wordList", "word, isFiltred", "'" + word + "', " + filter_words.contains(word), 1, true, false);
        }
        SelectInsertUpdate("wordLocation", "fk_wordId, fk_URLId, location", wordId + ", " + URLId + ", " + location, 2, true, false);
        location++;
    }

    /* Занесение ссылки с одной страницы на другую и текста в таблицы linkBetweenURL и linkWord */
    private void addLinkRef(int URLFromId, int URLToId, String[] linkText) throws Exception {
        if (linkText == null) // если текст ссылки пустой
            SelectInsertUpdate("linkBetweenURL", "fk_FromURL_Id, fk_ToURL_Id", URLFromId + ", " + URLToId, 2, true, false);
        else {
            int linkBetweenId = SelectInsertUpdate("linkBetweenURL", "fk_FromURL_Id, fk_ToURL_Id", URLFromId + ", " + URLToId, 2, true, false);
            for (String word : linkText) {
                if (word.length() == 0) continue;
                int wordId = SelectInsertUpdate("wordList", "word", "'" + word + "'", 1, false, false);
                SelectInsertUpdate("linkWord", "fk_wordId, fk_linkId", wordId + ", " + linkBetweenId, 2, true, false);
            }
        }
    }

    /* Вспомогательный метод для формирования ссылки на следующую страницу */
    private String generateLink(String URL, String nextUrl) {
        nextUrl = nextUrl.replace('\\', '/');
        if (nextUrl.startsWith("http") && nextUrl.length() > 6) { // абсолютная ссылка начинается с http или https
            URL = "";
        } else if (nextUrl.startsWith(".")) { // относительная ссылка, для перемещения по каталогам
            URL = URL.substring(0, URL.lastIndexOf("/"));
            while (nextUrl.contains("/") && nextUrl.substring(0, nextUrl.indexOf("/")).equals("..")) { // перемещение на каталог вверх
                URL = URL.substring(0, URL.lastIndexOf("/") + 1);
                nextUrl = nextUrl.substring(nextUrl.indexOf("/") + 1);
            }
            if (nextUrl.startsWith(".")) { // текущий каталог
                nextUrl = nextUrl.substring(nextUrl.indexOf("/"));
            }
        } else if (nextUrl.startsWith("//")) { // ссылка относительно протокола текущей страницы
            URL = URL.substring(0, URL.indexOf("//"));
        } else if (nextUrl.startsWith("/")) { // ссылка относительно домена текущей страницы
            if (URL.indexOf("/", URL.indexOf("//") + 2) != -1)
                URL = URL.substring(0, URL.indexOf("//") + 2) + URL.substring(URL.indexOf("//") + 2, URL.indexOf("/", URL.indexOf("//") + 2));
        } else { // невалидная ссылка
            URL = "";
            nextUrl = "";
        }
        nextUrl = URL + nextUrl;
        while (nextUrl.endsWith("/")) nextUrl = nextUrl.substring(0, nextUrl.length() - 1); // удаление "/" на конце ссылки
        if (nextUrl.contains("://www.")) nextUrl = nextUrl.replace("://www.", "://"); // удаление "www" из ссылки
        return nextUrl;
    }

    /* Разбиение текста на слова */
    private String[] separateWords(String text) {
        if (text.length() == 0) return null; // если страница не содержит текст
        String[] replace_symbols = {",", ":", "\"", "\\.", "\\{", "}", "—", "-", "\n", "\\(", "\\)", "/", "«", "»", "!", "\\?"}; // исключаемые символы пунктуации из текста страницы
        for (String replace_symbol : replace_symbols) text = text.replaceAll(replace_symbol, " ");
        String[] words = text.split(" ");
        return words;
    }

    /* Очистка HTML-кода от тегов */
    protected String getTextOnly(Element html_doc) {
        String html = html_doc.outerHtml().replaceAll("<", " <"); // подстановка пробелов перед открытием и закрытием тегов каждого элемента, чтобы избежать слияния слов
        html_doc = Jsoup.parse(html);
        String html_text = html_doc.text();
        while (html_text.contains("  ")) html_text = html_text.replaceAll("  ", " "); // замена множественных пробелов
        return html_text.replace('\'', '`');
    }

    protected Document getDocument(String URL) throws IOException {
        Document html_doc;
        if (debug) System.out.println("\t\tПопытка открыть " + URL);
        // блок контроля исключений при запросе содержимого страницы
        try {
            html_doc = Jsoup.connect(URL).get(); // получить HTML-код страницы
            if (debug) System.out.print("\t\tWEB файл ");
        } catch (java.net.MalformedURLException e) { // если не удалось, страница может быть локальным файлом
            if (debug) System.out.print("\t\tЛокальный файл ");
            String fileName = URL.substring(7);
            if (debug) System.out.println(fileName);
            File input = new File(fileName);
            html_doc = Jsoup.parse(input, "UTF-8");
        } catch (Exception e) {
            // обработка исключений при ошибке запроса содержимого страницы
            System.out.println("\t\tОшибка. " + URL);
            System.out.print(e);
            return null;
        }
        return html_doc;
    }

    /* Очистка страницы от комментариев и тегов noindex */
    protected void removeComments(Node node) {
        for (int i = 0; i < node.childNodeSize();) {
            Node child = node.childNode(i);
            if (child.nodeName().equals("#comment")) {
                if (child.outerHtml().equals("<!--noindex-->")) // удаление тега noindex Яндекса
                    while (!child.outerHtml().equals("<!--/noindex-->")) {
                        child.remove();
                        child = node.childNode(i);
                    }
                child.remove();
            }
            else {
                removeComments(child);
                i++;
            }
        }
    }

    /* Индексирование страницы */
    private List<String> addToIndex(String URL) throws Exception {
        if (debug) System.out.println("\t\tИндексирование страницы");
        URL = URL.replace('\\', '/');
        List<String> nextUrlSet = null;

        // Проверка, проиндексирована ли страница
        if (!isIndexed(URL)) {
            // Изменить состояние текущей страницы на индексации на "проиндексировано"
            int URLId = SelectInsertUpdate("URLList", "URL", "'" + URL + "'", 1, false, false);
            if (URLId != -1) SelectInsertUpdate("URLList", "isIndexed, URL", "TRUE, '" + URL + "'", 1, false, true);
            else URLId = SelectInsertUpdate("URLList", "URL, isIndexed", "'" + URL + "', TRUE", 1, true, false);

            // Запросить HTML-код
            Document html_doc = getDocument(URL);

            if (html_doc != null) {
                if (debug) System.out.println("открыт " + URL);
                // создать множество (ArrayList) очередных адресов (уникальных - не повторяющихся)
                nextUrlSet = new ArrayList<String>();

                // Найти и удалить на странице блоки со скриптами, стилями оформления, meta-тегами и ссылками на внешние ресурсы ('script', 'style', 'meta', 'link')
                html_doc.select("script, style, meta, link").remove();
                removeComments(html_doc);

                String text = getTextOnly(html_doc);
                String words[] = separateWords(text.toLowerCase());
                location = 0;
                for (String word : words) addWord(URLId, word);
                // Разобрать HTML-код на составляющие
                // Получить все теги <a>
                Elements links = html_doc.getElementsByTag("a");
                for (Element tagA : links) { // обработать каждый тег <a>
                    String nextUrl = tagA.attr("href"); // получить содержимое аттрибута "href"
                    // Проверка соответствия ссылок требованиям
                    nextUrl = generateLink(URL, nextUrl);
                    if (nextUrl.length() == 0) {
                        if (debug) System.out.println("\t\t\tссылка не валидная - пропустить " + nextUrl);
                    } else if (SelectInsertUpdate("URLList", "URL", "'" + nextUrl + "'", 1, false, false) == -1) {
                        if (debug) System.out.println("\t\t\tссылка валидная - добавить " + nextUrl);
                        nextUrlSet.add(nextUrl); // добавить в множество очередных ссылок nextUrlSet
                        int nextUrlId = SelectInsertUpdate("URLList", "URL", "'" + nextUrl + "'", 1, true, false);
                        // Добавление связи ссылок и их текста ссылки в таблицы linkBetweenURL и linkWord
                        String link_text = getTextOnly(tagA);
                        String[] link_words = separateWords(link_text.toLowerCase());
                        addLinkRef(URLId, nextUrlId, link_words); // добавить информацию о ссылке в БД - addLinkRef(URLId, nextUrlId, link_text)
                    }
                } // конец цикла обработки тега <a>
            }
            if (debug) System.out.println("\t\tСтраница проиндексирована");
        }
        return nextUrlSet;
    }

    /* Метод сбора данных.
     * Начиная с заданного списка страниц, выполняет поиск в ширину
     * до заданной глубины, индексируя все встречающиеся по пути страницы */
    protected void crawl(Map<String, List<String>> parentUrls, int maxDepth) throws Exception {
        if (debug) System.out.println("Начало обхода всех страниц");

        // для каждого уровня глубины currDepth до максимального maxDepth
        for (int currDepth = 0; currDepth < maxDepth; currDepth++) {
            if (debug) System.out.println("\t== Глубина " + (currDepth + 1) + " ==");
            Map<String, List<String>> nextParentUrls = new HashMap<String, List<String>>();

            for (int i = 0; i < parentUrls.size(); i++) {
                String parentUrl = (String) parentUrls.keySet().toArray()[i];
                int N = parentUrls.get(parentUrl) == null ? 0 : parentUrls.get(parentUrl).size(); // количество элементов, которые предстоит обойти в списке URLList

                // обход всех URL на теущей глубине
                for (int j = 0; j < N; j++) {
                    List<String> URLList = parentUrls.get(parentUrl);
                    String URL = URLList.get(j); // получить URL-адрес из списка

                    Date date = new Date();
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
//                    if (debug)
                        System.out.println("\t" + (j + 1) + "/" + URLList.size() + " - " + formatter.format(date) + " - Индексируем страницу " + URL);

                    nextParentUrls.put(URL, addToIndex(URL));
                    // конец обработки одной ссылки URL
                }
            }
            // заменить содержимое parentUrls на nextParentUrls
            parentUrls = nextParentUrls;

            // конец обхода ссылкок parentUrls на текущей глубине
        }
        if (debug) System.out.println("\tВсе страницы проиндексированы");
    }
}