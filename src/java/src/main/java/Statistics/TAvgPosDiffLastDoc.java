package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Среднее число позиций до предыдущего кликнутого документа
public class TAvgPosDiffLastDoc extends BaseStatistic {

    public TAvgPosDiffLastDoc(){
        KeyPrefix = StatisticConfig.AVG_POS_DIFF_LAST_DOC;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (serp.ClickedLinks.size() > 1) {
            int lastPosition = serp.ClickedLinks.get(0).Position;
            for (int i = 1; i < serp.ClickedLinks.size(); i++) {
                TSerp.TClickedLink clickedLink = serp.ClickedLinks.get(i);
                Integer posDiff = Math.abs(lastPosition - clickedLink.Position);
                write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * posDiff, 1f, hashMap);
                lastPosition = clickedLink.Position;
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sum_diff = 0;
        float clicked_cnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sum_diff += Float.parseFloat(parts[0]);
            clicked_cnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) sum_diff / clicked_cnt)));
    }
}
