package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Число кликов на документ по всем выдачам, в которых он встречался
public class TClickDoc extends BaseStatistic {

    public TClickDoc() {
        KeyPrefix = StatisticConfig.CLICK_DOC_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * 1, 0f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sum = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sum += Float.parseFloat(parts[0]);
        }
        context.write(key, new Text(Float.toString(sum)));
    }
}
