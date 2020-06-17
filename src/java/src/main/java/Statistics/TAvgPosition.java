package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Средняя позиция документа в выдачах
public class TAvgPosition extends BaseStatistic {

    public TAvgPosition(){
        KeyPrefix = StatisticConfig.AVG_POSITION_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (int i = 0; i < serp.ShownLinks.size(); ++i) {
            write(serp.QueryID, serp.ShownLinks.get(i), "BU", serp.QuerySimilarity * i, 1f, hashMap);
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
