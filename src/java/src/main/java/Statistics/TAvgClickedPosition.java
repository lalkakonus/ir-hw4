package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Средняя позиция кликнутого документа в выдаче
public class TAvgClickedPosition extends BaseStatistic {

    public TAvgClickedPosition(){
        KeyPrefix = StatisticConfig.AVG_CLICK_POSITION_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * clickedLink.Position, 1f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sum = 0;
        float cnt = 0;
        for (Text value: values) {
            String [] parts = value.toString().split(":");
            sum += Float.parseFloat(parts[0]);
            cnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) sum / cnt)));
    }
}
