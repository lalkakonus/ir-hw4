package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Среднее время работы с запросом
public class TAvgWorkTimeQuery extends BaseStatistic {

    public TAvgWorkTimeQuery(){
        KeyPrefix = StatisticConfig.AVG_WORK_TIME_QUERY_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        int clickedListSize = serp.ClickedLinks.size();
        if (clickedListSize > 1) {
            long diff = serp.ClickedLinks.get(clickedListSize - 1).Timestamp - serp.ClickedLinks.get(0).Timestamp;
            write(serp.QueryID, EMPTY_LINK, "QE", serp.QuerySimilarity * (int) diff, 1f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sumTime = 0;
        float showCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sumTime += Float.parseFloat(parts[0]);
            showCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) sumTime / showCnt)));
    }
}
