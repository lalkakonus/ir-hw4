package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Средний с конца номер клика в документ
public class TAvgInverseClickNumber extends BaseStatistic {

    public TAvgInverseClickNumber(){
        KeyPrefix = StatisticConfig.AVG_INVERSE_CLICK_NUMBER_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        int i = serp.ClickedLinks.size() - 1;
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "QE", serp.QuerySimilarity * i, 1f, hashMap);
            --i;
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
