import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Searcher {
    private Connection conn; // соединение с БД в локальном файле
    private double d = 0.85; // коэффициент затухания

    /* Конструктор инициализации паука с параметрами БД */
    protected Searcher(String fileName) throws SQLException {
        String db_url = "jdbc:sqlite:" + fileName; // формируемая строка для подключения к локальному файлу

        this.conn = DriverManager.getConnection(db_url);

        this.conn.setAutoCommit(false); // выключить режим автоматической фиксации (commit) изменений БД
    }

    /* Метод финализации работы с БД */
    protected void finalize() throws SQLException {
        conn.close(); // закрыть соединение
    }

    /* Разбиение строки на слова */
    private String[] separateWords(String text) {
        text = text.toLowerCase(); // Привести текст запроса к нижнему регистру
        String[] words = text.split(" ");
        return words;
    }

    /* Получение идентификаторов для каждого слова в queryString */
    private List<Integer> getWordsIds(String[] words, Statement statement) throws Exception {
        List<Integer> rowIdList = new ArrayList<Integer>(); // список для хранения результата
        boolean is_words = false;

        for (String word : words) {
            String request = "SELECT rowId, isFiltred FROM wordList WHERE word = \"" + word + "\";"; // Сформировать SQL-запрос для получения rowId слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            ResultSet resultRow = statement.executeQuery(request); // Выполнить SQL-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowId

            while (resultRow.next()) {
                // Если слово было найдено
                if (resultRow.getBoolean("isFiltred")) continue; // Пропуск фильтруемых слов
                int word_rowId = resultRow.getInt("rowId");
                rowIdList.add(word_rowId); // поместить rowId в список результата
                System.out.println(word + " " + word_rowId);
                is_words = true;
            }
        }
        if (!is_words) throw new Exception("Ни одно слово не было найдено"); // в случае, если слова не найдены, приостановить работу (генерация исключения)

        return rowIdList; // вернуть список идентификаторов
    }

    /* Функция приводит значение ранга к диапазону 0...1, где 0-"хуже", 1 - "лучше".
            :param scores:  словарь идентификаторов и рангов
            :param smallIsBetter: режим чтения входного ранга. лучше – меньший или больший */
    private Map normalizeScores(Map<Integer, Double> scores, boolean smallIsBetter) {
        if (smallIsBetter) { // Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
            double vsmall = 0.000001; // малая величина, вместо деления 0 и на 0
            double min = Collections.min(scores.values());
            for (int key : scores.keySet()) {
                double val = scores.get(key);
                double newVal = Math.max(min, vsmall) / Math.max(val, vsmall);
                scores.put(key, newVal);
            }
        } else { // Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ
            double max = Collections.max(scores.values());
            for (int key : scores.keySet()) {
                double val = scores.get(key);
                double newVal = val / max;
                scores.put(key, newVal);
            }
        }
        return scores;
    }

    /* Метрика по PageRank */
    private Map pagerankScore() throws SQLException {
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса

        // получение ссылок для расчета PageRank всех страниц
        String request = "SELECT rowId FROM URLList;";
        ResultSet resultRow = statement.executeQuery(request);
        List<Integer> URLs = new ArrayList<Integer>();
        while (resultRow.next()) {
            URLs.add(resultRow.getInt("rowId"));
        }

        request = "SELECT fk_FromURL_Id, fk_ToURL_Id FROM linkBetweenURL;";
        resultRow = statement.executeQuery(request);
        Map<Integer, List<Integer>> linkBetween = new HashMap<Integer, List<Integer>>();
        Map<Integer, Double> pageRank = new HashMap<Integer, Double>();
        for (int URLId : URLs) {
            linkBetween.put(URLId, new ArrayList<Integer>());
            pageRank.put(URLId, 1.0);
        }
        while (resultRow.next()) {
            int FromURL_Id = resultRow.getInt("fk_FromURL_Id"), ToURL_Id = resultRow.getInt("fk_ToURL_Id");
            linkBetween.get(FromURL_Id).add(ToURL_Id);
        }

        for (int i = 0; i < 20; i++) {
            Map<Integer, Double> pageRank_new = new HashMap<Integer, Double>();
            for (int URLId : URLs) {
                pageRank_new.put(URLId, 0.0);
            }
            for (int FromURL_Id : URLs) {
                List<Integer> neighbours = linkBetween.get(FromURL_Id);
                for (int ToURL_Id : neighbours) {
                    pageRank_new.put(ToURL_Id, pageRank_new.get(ToURL_Id) + pageRank.get(FromURL_Id)/neighbours.size());
                }
            }
            for (int URLId : URLs) {
                pageRank.put(URLId, (1 - d) + d * pageRank_new.get(URLId));
            }
        }

        pageRank = normalizeScores(pageRank, false); // передать словарь счетчиков в функцию нормализации, режим "чем больше, тем лучше")
        return pageRank;
    }

    /* Метрика по содержимому Расстояние между искомыми словами */
    private Map distanceScore(List<Integer> URLs, List<Integer[]> wordLocation) {
        Map<Integer, Double> minDistanceDict = new HashMap<Integer, Double>();
        for (int URLId : URLs) {
            int minDistance = Integer.MAX_VALUE;
            for (Integer[] location1 : wordLocation) {
                if (URLId == location1[0]) {
                    int location_prev = location1[2];
                    for (Integer[] location2 : wordLocation) {
                        if (URLId == location2[0]) {
                            if (location_prev != -1 && (location1[2] != location2[2])) {
                                minDistance = Math.min(Math.abs(location2[2] - location_prev), minDistance);
                            }
                        }
                    }
                }
            }
            minDistanceDict.put(URLId, (double) minDistance);
        }
        minDistanceDict = normalizeScores(minDistanceDict, true); // передать словарь счетчиков в функцию нормализации, режим "чем меньше, тем лучше")
        return minDistanceDict;
    }

    /* Метрика по содержимому Расположение относительно начала документа */
    private Map locationScore(List<Integer> URLs, List<Integer[]> wordLocation) {
        Map<Integer, Double> locationsDict = new HashMap<Integer, Double>();
        for (int URLId : URLs) {
            int minLocation = Integer.MAX_VALUE;
            for (Integer[] location : wordLocation) {
                if (URLId == location[0]) {
                    minLocation = Math.min(location[2], minLocation);
                }
            }
            locationsDict.put(URLId, (double) minLocation);
        }
        locationsDict = normalizeScores(locationsDict, true); // передать словарь счетчиков в функцию нормализации, режим "чем меньше, тем лучше")
        return locationsDict;
    }

    /* Метрика по содержимому Частота слов */
    private Map frequencyScore(List<Integer> URLs, List<Integer[]> wordLocation) {
        Map<Integer, Double> countsDict = new HashMap<Integer, Double>();
        for (int URLId : URLs) {
            Map<Integer, Integer> countWords = new HashMap<Integer, Integer>();
            for (Integer[] location : wordLocation) {
                if (URLId == location[0]) {
                    if (!countWords.containsKey(location[1])) countWords.put(location[1], 1);
                    else countWords.put(location[1], countWords.get(location[1]) + 1);
                }
            }
            int sum = 0;
            for (int count : countWords.values()) sum = (sum + count) * count;
            countsDict.put(URLId, (double) sum);
        }
        countsDict = normalizeScores(countsDict, false); // передать словарь счетчиков в функцию нормализации, режим "чем больше, тем лучше")
        return countsDict;
    }

    /* На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке */
    private List<Integer> getSortedList(List<Integer> URLs, List<Integer[]> wordLocation) throws SQLException {
        Map<Integer, Double> frequency = frequencyScore(URLs, wordLocation);
        System.out.println("\t\t\tЧастота слов: " + frequency);

        Map<Integer, Double> location = locationScore(URLs, wordLocation);
        System.out.println("\t\t\tРасположение в документе: " + location);

        Map<Integer, Double> distance = distanceScore(URLs, wordLocation);
        System.out.println("\t\t\tРасстояние между словами: " + distance);

        Map<Integer, Double> pagerank = pagerankScore();
        System.out.println("\t\t\tPageRank: " + pagerank);

        List<Double> weights = new ArrayList<Double>();
        weights.add(1.0); weights.add(1.0); weights.add(1.0); weights.add(1.0);
        int wcount = 0;
        for (double weight : weights) if (weight != 0.0) wcount++;
        System.out.println("\t\t\tВеса: " + weights);

        System.out.println("\t\tРанги с учетом весов:");
        Map<Integer, Double> result = new HashMap<Integer, Double>();
        for (int URLId : URLs) {
            double res = weights.get(0) * frequency.get(URLId) + weights.get(1) * location.get(URLId) + weights.get(2) * distance.get(URLId) + weights.get(3) * pagerank.get(URLId);
            res /= wcount;
            result.put(URLId, res);
            System.out.println("\t\t" + URLId + " - " + res);
        }
        List<Double> sorted = new ArrayList<Double>(result.values());
        Collections.sort(sorted);
        Collections.reverse(sorted);
        URLs.clear();
        for (double val : sorted) {
            for (Map.Entry<Integer, Double> URL : result.entrySet()) {
                if (URL.getValue() == val) {
                    URLs.add(URL.getKey());
                }
            }
        }
        System.out.println("\t" + URLs);
        return URLs;
    }

    /* получение текстового поля URL-адреса по указанному URLId */
    private String getURLName(int URLId) throws SQLException {
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request = "SELECT URL FROM URLList WHERE rowId = \"" + URLId + "\";";
        ResultSet resultRow = statement.executeQuery(request);
        if (!resultRow.next()) return null;
        else return resultRow.getString("URL");
    }

    /* Генерация HTML страницы поисковика */
    private void generateHTML(String queryString, List<Integer> URLs, String[] wordsSearch) throws Exception {
        Crawler crawler = new Crawler();
        // генерация цветов для выделения каждого поискового слова
        int length = wordsSearch.length;
        int step = 0xff / ((int) Math.ceil((double) length / 3));
        String[] colors = new String[length];
        for (int i = 0; i < length; i++) {
            int color;
            boolean rand = (int) Math.random() * 2 == 0;
            if (i % 3 == 0) { // основной цветовой канал - красный
                color = (((int) Math.ceil((double) length / 3)) - (i / 3)) * step * 0x10000 + (rand ? 0xF000 : 0xF0);
            } else if (i % 3 == 1) { // основной цветовой канал - зеленый
                color = (((int) Math.ceil((double) length / 3)) - (i / 3)) * step * 0x100 + (rand ? 0xF0 : 0xF00000);
            } else { // основной цветовой канал - синий
                color = (((int) Math.ceil((double) length / 3)) - (i / 3)) * step * 0x1 + (rand ? 0xF00000 : 0xF000);
            }
            String background = String.format("%06X", color);
            colors[i] = background;
        }

        int page_count = URLs.size() / 10 + 1;
        String[] body = new String[page_count];
        for (int i = 0; i < page_count; i++) body[i] = "";

        int link_count = 0;
        // выделение цветом ключевых слов
        for (int URl : URLs) {
            String URLName = getURLName(URl);

            Document html_doc = crawler.getDocument(URLName);
            String title = html_doc.getElementsByTag("title").first().html();
            html_doc.select("script, style, head").remove();
            crawler.removeComments(html_doc);
            String text = crawler.getTextOnly(html_doc);
            text = text.replace(" ,", ",");
            text = text.replace(" .", ".");

            String[] replace_symbols = {",", ":", "\"", "\\.", "\\{", "}", "—", "-", "\n", "\\(", "\\)", "/", "«", "»", "!", "\\?", " "}; // исключаемые символы пунктуации из текста страницы
            String text_for_separate = text;
            for (String replace_symbol : replace_symbols)
                text_for_separate = text_for_separate.replaceAll(replace_symbol, " ");
            Set<String> words = new HashSet<String>(Arrays.asList(text_for_separate.split(" ")));

            // выделение ключевых слов
            for (String word : words) {
                int i = -1;
                for (String wordSearch : wordsSearch) {
                    i++;
                    if (wordSearch.equalsIgnoreCase(word)) {
                        for (String symbol1 : replace_symbols)
                            for (String symbol2 : replace_symbols) {
                                String background = colors[i];
                                String new_word = "<span class=\"keyword\" style=\"background: #" + background + ";\">" + word + "</span>";
                                text = text.replaceAll(symbol1 + word + symbol2, symbol1 + new_word + symbol2);
                                if (text.lastIndexOf(word) + word.length() == text.length() && text.substring(text.lastIndexOf(word) - 1, text.lastIndexOf(word)).equals(symbol1))
                                    text = text.substring(0, text.lastIndexOf(word)) + new_word;
                            }
                        break;
                    }
                }
            }

            body[link_count / 10] += "\t\t<div>\n\t\t\t<p class=\"link\"><a href=\">" + URLName + "\">" + URLName + "</a></p>\n\t\t\t<p class=\"name\"><a href=\"" + URLName + "\">" + title + "</a></p>\n";
            body[link_count / 10] += "\t\t\t<p class=\"text\">" + text + "</p>\n";
            body[link_count / 10] += "\t\t</div>\n";
            link_count++;
        }
        for (int cur_page = 1; cur_page <= page_count; cur_page++) {
            String filename = queryString + " - SPG Search";
            String HTML = "<html>\n\t<head>\n\t\t<meta charset=\"utf-8\">\n\t\t<link rel=\"stylesheet\" href=\"./style.css\">\n\t\t<title>" + queryString + " - SPG Search</title>\n\t</head>\n\t<body>\n";
            HTML += "\t\t<div class=\"header\">\n\t\t\t<h1>" + queryString + " - Поиск в SPG Search</h1>\n\t\t\t<div class=\"logo\"><img src=\"./logo.png\"></div>\n\t\t</div>\n";
            HTML += body[cur_page - 1];

            // генерация ссылок для перемещения по поисковой выдачи
            if (page_count > 1) {
                HTML += "\t\t<div class=\"prev_next\">";
                if (cur_page != 1) HTML += "<a href=\"" + filename + "-" + (cur_page - 1) + ".html" + "\" class=\"prev\">← Предыдущая страница</a>";
                else HTML += "<a href=\"#\" class=\"prev\" style=\"visibility: hidden;\">← Предыдущая страница</a>";
                HTML += "<span class=\"page_counter\">" + cur_page + " / " + page_count + "</span>";
                if (cur_page != page_count) HTML += "<a href=\"" + filename + "-" + (cur_page + 1) + ".html" + "\" class=\"next\">Следующая страница →</a>";
                else HTML += "<a href=\"#\" class=\"next\" style=\"visibility: hidden;\">Следующая страница →</a>";
                HTML += "</div>\n";
                filename += "-" + cur_page;
            }

            HTML += "\t</body>\n</html>";
            filename += ".html";
            String folder = "Site";
            File dir = new File(folder);
            dir.mkdir();
            PrintStream out = new PrintStream(new FileOutputStream(folder + "\\" + filename));
            out.print(HTML);
            System.out.println("Файл \"" + filename + "\" был записан");
        }
    }

    /* все сочетания позиций всех слов поискового запроса
     *   Поиск комбинаций из всех искомых слов в проиндексированных URL-адресах
     * :param queryString: поисковый запрос пользователя
     * :return: 1) список вхождений формата (URLId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
     */
    protected void search(String queryString) throws Exception {
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса

        String[] words = separateWords(queryString);
        List<Integer> wordIds = getWordsIds(words, statement);
        System.out.println(wordIds);

        // Получение наборов критериев и URL, подходящих под запрос, для дальнейшего ранжирования
        List<Integer> URLs = new ArrayList<Integer>();
        List<Integer[]> wordLocation = new ArrayList<Integer[]>();
        System.out.println("\tКритерии ранжирования:");
        for (int wordId : wordIds) {
            // Получить расположения слов на страницах
            String request = "SELECT fk_URLId, location FROM wordLocation WHERE fk_wordId = \"" + wordId + "\";";
            ResultSet resultRow = statement.executeQuery(request);
            while (resultRow.next()) {
                // Если слово было найдено и rowId получен
                Integer[] location = new Integer[3];
                location[0] = resultRow.getInt("fk_URLId");
                location[1] = wordId;
                location[2] = resultRow.getInt("location");
                wordLocation.add(location); // поместить rowId в список результата
                System.out.println("\tРасположение на странице " + location[0] + "(URLId) " + location[1] + "(wordId) " + location[2] + "(location)");
                if (!URLs.contains(location[0])) URLs.add(location[0]); // поместить Id ранжируемой страницы в список
            }

            // Получить ссылки, по которым ведут слова
            request = "SELECT fk_linkId FROM linkWord WHERE fk_wordId = \"" + wordId + "\";";
            resultRow = statement.executeQuery(request);
            while (resultRow.next()) {
                // Если слово было найдено запомнить ссылку, по которой ведет
                Statement statementURL = this.conn.createStatement();
                int linkId = resultRow.getInt("fk_linkId");
                String requestURL = "SELECT fk_FromURL_Id, fk_ToURL_Id FROM linkBetweenURL WHERE rowId = \"" + linkId + "\";";
                ResultSet resultRowURL = statementURL.executeQuery(requestURL);
                int URLId = resultRowURL.getInt("fk_FromURL_Id");
                int toURLId = resultRowURL.getInt("fk_ToURL_Id");
                System.out.println("\tСсылка на странице " + URLId + "(URLId) " + wordId + "(wordId) " + toURLId + "(ToURL_Id)");
//                if (!URLs.contains(toURLId)) URLs.add(toURLId); // поместить Id ранжируемой страницы в список
            }
        }
        System.out.println("\t\tРанжируемые страницы:");
        for (int URL : URLs) {
            System.out.println("\t\t" + URL);
        }

        URLs = getSortedList(URLs, wordLocation);
        generateHTML(queryString, URLs, words);
        return;
    }
}
