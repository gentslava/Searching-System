import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        boolean searching_or_indexing = false;
        if (searching_or_indexing) {
            List<String> pageList = new ArrayList<String>();
            pageList.add("https://lenta.ru");
            Map<String, List<String>> parentUrls = new HashMap<String, List<String>>();
            parentUrls.put("", pageList);
            try {
                Crawler my_crawler = new Crawler("lr3-depth2.db", false);
                my_crawler.dropDB();
                my_crawler.createIndexTables();
                my_crawler.crawl(parentUrls, 3);
                my_crawler.finalize();
                Date date = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                System.out.println(formatter.format(date) + " - Индексация завершена");
            } catch (Exception e) {
                Date date = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                System.out.println(formatter.format(date) + " - Индексация завершена с ошибкой:");
                e.printStackTrace();
            }
        } else {
            try {
                Searcher mySearcher = new Searcher("lr3-depth2.db");
                String mySearchQuery = "Новосибирск карантин";
                mySearcher.search(mySearchQuery);
                mySearcher.finalize();
            } catch (Exception e) {
                System.out.println("Поиск завершен с ошибкой:");
                e.printStackTrace();
            }
        }
    }
}
