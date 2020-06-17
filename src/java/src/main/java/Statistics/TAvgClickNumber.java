package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Средний номер клика в документ
public class TAvgClickNumber extends BaseStatistic {

    public TAvgClickNumber(){
        KeyPrefix = StatisticConfig.AVG_CLICK_NUMBER_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        int i = 0;
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * i, 1f, hashMap);
            ++i;
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sum = 0;
        float cnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sum += Float.parseFloat(parts[0]);
            cnt += Float.parseFloat(parts[1]);
        }

        context.write(key, new Text(Float.toString((float) sum / cnt)));
    }
}
