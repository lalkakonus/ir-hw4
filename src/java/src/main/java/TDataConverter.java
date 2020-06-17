import Statistics.StatisticConfig;
import Statistics.TSerp;
import javafx.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;


class TDataConverter {

    static Pattern pattern = Pattern.compile("(^(https?\\.?|www\\.?|://){1,2}|/$)");

    // URL -> URL ID
    private HashMap<String, String> URLNumeration;
    // URL ID- > HOST ID
    private HashMap<String, String> HostNumeration;
    // QUERY -> QUERY ID
    private HashMap<String, Pair<String, Float>> QueryNumeration;
    // QUERY ID -> Set<DOC IDs>
    private HashMap<String, HashSet<String>> QueryDocCorrespondence;


    TDataConverter(Path urlNumerationFilepath, Path queryNumerationFilepath,
                   Path hostNumerationFilepath, Path queryDocCorrFilepath) {
        Function<String, String> dummyFunction = (String value) -> value;
        URLNumeration = LoadItemNumeration(urlNumerationFilepath, TDataConverter::NormalizeURL);
        HostNumeration = LoadItemNumeration(hostNumerationFilepath, dummyFunction);
        QueryNumeration = LoadQueries(queryNumerationFilepath);
        QueryDocCorrespondence = LoadQueryDocCorrespondence(queryDocCorrFilepath);
    }

    private static HashMap<String, String> LoadItemNumeration(Path filepath, Function<String, String> transformation) {
        HashMap<String, String> map = new HashMap<>();
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(filepath);

            String[] out = IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n");
            for (int i = 1; i< out.length; i++) {
                String[] parts = out[i].split("\t");
                String key = transformation.apply(parts[1]);
                String value = parts[0];
                map.put(key, value);
            }
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    private static HashMap<String, Pair<String, Float>> LoadQueries(Path filepath) {
        HashMap<String, Pair<String, Float>> map = new HashMap<>();
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(filepath);

            String[] out = IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n");
            for (int i = 1; i< out.length; i++) {
                String[] parts = out[i].split("\t");
                String key = parts[1];
                Pair<String, Float> value = new Pair<>(parts[0], Float.parseFloat(parts[2]));
                map.put(key, value);
            }
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    private static HashMap<String, HashSet<String>> LoadQueryDocCorrespondence(Path filepath) {
        HashMap<String, HashSet<String>> map = new HashMap<>();
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(filepath);
            String[] out = IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n");
            for (int i = 1; i < out.length; i++) {
                String[] parts = out[i].split("\t");
                String key = parts[0];
                HashSet<String> value = new HashSet<>(Arrays.asList(parts[1].split(",")));
                map.put(key, value);
            }
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    TSerp Convert(String input) throws IllegalArgumentException {
        // Parse query ID
        String[] resolve = input.split("@");
        Pair<String, Float> query = QueryNumeration.getOrDefault(resolve[0], new Pair<>(StatisticConfig.ABSENT, 1.0f));
//        String queryID = QueryNumeration.getOrDefault(resolve[0], StatisticConfig.ABSENT);

        // Parse region ID
        resolve = resolve[1].split("\t");
        String regionID = resolve[0];

        // Parse shown links ID
        List<String> rawShownLinks = Arrays.asList(resolve[1].split(","));
        List<TSerp.TShownLinks> shownLinks = new ArrayList<>();
        for (String url : rawShownLinks) {
            String normalizedURL = NormalizeURL(url);

            String urlID;
            String hostID;
            boolean queryCorrespondent;

            if (URLNumeration.containsKey(normalizedURL)) {
                urlID = URLNumeration.get(normalizedURL);
                hostID = HostNumeration.get(urlID);
            } else {
                urlID = StatisticConfig.ABSENT;
                hostID = StatisticConfig.ABSENT;
            }
            if (QueryDocCorrespondence.containsKey(query.getKey())) {
                queryCorrespondent = QueryDocCorrespondence.get(query.getKey()).contains(urlID);
            } else {
                queryCorrespondent = false;
            }
            shownLinks.add(new TSerp.TShownLinks(urlID, hostID, queryCorrespondent));
        }

        // Parse clicked links
        List<TSerp.TClickedLink> clickedLinks = new ArrayList<>();
        if (resolve.length > 2) {
            // Parse URL ID and position
            for (String url : resolve[2].split(",")) {
                if (url.length() == 0) {
                    continue;
                }
                int position = rawShownLinks.indexOf(url);
                if (position < 0) {
                    System.out.println(input);
                    throw new IllegalArgumentException("Clicked URL not find in shown URLs.");
                }
                TSerp.TClickedLink clickedLink = new TSerp.TClickedLink(shownLinks.get(position));
                clickedLink.setPosition(position);
                clickedLinks.add(clickedLink);
            }

            // Parse timestamp
            String [] rawTimestampList = resolve[3].split(",");
            for (int i = 0; i < rawTimestampList.length; ++i) {
                Long timestamp = Long.parseLong(rawTimestampList[i]);
                clickedLinks.get(i).setTimestamp(timestamp);
            }

            // Sorting order is increasing by time
            clickedLinks.sort(Comparator.comparing(link -> link.Timestamp));
        }

        return new TSerp(query, regionID, shownLinks, clickedLinks);
    }

    private static String NormalizeURL(String URL) {
        return pattern.matcher(URL).replaceAll("");
    }
}