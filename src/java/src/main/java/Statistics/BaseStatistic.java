package Statistics;

import javafx.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static Statistics.StatisticConfig.EMPTY;


public class BaseStatistic {
    protected String KeyPrefix;

    static final TSerp.TBaseLink EMPTY_LINK = new TSerp.TBaseLink();

    private static final Boolean HOST = true;
    private static final Boolean NOT_HOST = false;

    protected void write(String queryID, TSerp.TBaseLink link, String keyType,
                         Float first, Float second, Map<String, String> hashMap) {
        Set<String> querySet = new HashSet<>();
        Set<Pair<String, Boolean>> urlSet = new HashSet<>();

        switch (keyType.charAt(0)) {
            case 'E': {
                querySet.add(EMPTY);
                break;
            } case 'Q': {
                querySet.add(queryID);
                break;
            } case 'B': {
                querySet.add(EMPTY);
                querySet.add(queryID);
            }
        }

        switch (keyType.charAt(1)) {
            case 'E': {
                urlSet.add(new Pair<>(EMPTY, NOT_HOST));
                break;
            } case 'U': {
                urlSet.add(new Pair<>(link.UrlID, NOT_HOST));
                urlSet.add(new Pair<>(link.HostID, HOST));
                break;
            } case 'B': {
                urlSet.add(new Pair<>(EMPTY, NOT_HOST));
                urlSet.add(new Pair<>(link.UrlID, NOT_HOST));
                urlSet.add(new Pair<>(link.HostID, HOST));
            }
        }

        for (String query: querySet) {
            for (Pair<String, Boolean> url: urlSet) {

                String localUrlID = url.getKey();
                Boolean isHost = url.getValue();
                if (query.equals(StatisticConfig.ABSENT) || localUrlID.equals(StatisticConfig.ABSENT)) {
                    continue;
                }
                if (!query.equals(EMPTY) && !localUrlID.equals(EMPTY) && !link.QueryCorrespondent) {
                    continue;
                }
                String key = CompositeKey.getKey(query, localUrlID, isHost, KeyPrefix);

                float one = first;
                float two = second;
                if (hashMap.containsKey(key)) {
                    String [] value = hashMap.get(key).split(":");
                    one += Float.parseFloat(value[0]);
                    two += Float.parseFloat(value[1]);
                }
                hashMap.put(key, one + ":" + two);
            }
        }
    }

    public void map(TSerp serp, Map<String, String> hashMap) throws IOException, InterruptedException {}

    public void reduce(Text key, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {}
}
