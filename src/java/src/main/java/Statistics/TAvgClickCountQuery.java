package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Среднее число кликов для запроса
public class TAvgClickCountQuery extends BaseStatistic {

    public TAvgClickCountQuery() {
        KeyPrefix = StatisticConfig.AVG_CLICK_COUNT_QUERY_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        Float value = serp.QuerySimilarity * serp.ClickedLinks.size();
        write(serp.QueryID, EMPTY_LINK, "QE", value, 1f, hashMap);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sumClicked = 0;
        float showCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sumClicked += Float.parseFloat(parts[0]);
            showCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) sumClicked / showCnt)));
    }
}