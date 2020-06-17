package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Среднее число позиций до следующего кликнутого документа
public class TAvgPosDiffNextDoc extends BaseStatistic {

    public TAvgPosDiffNextDoc(){
        KeyPrefix = StatisticConfig.AVG_POS_DIFF_NEXT_DOC;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (serp.ClickedLinks.size() > 1) {
            int lastElementPosition = serp.ClickedLinks.size() - 1;
            int penultElementPosition = lastElementPosition - 2;

            int lastPosition = serp.ClickedLinks.get(lastElementPosition).Position;
            for (int i = penultElementPosition; i > 0; i--) {
                TSerp.TClickedLink clickedLink = serp.ClickedLinks.get(i);
                Integer posDiff =Math.abs(lastPosition - clickedLink.Position);
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
        context.write(key, new Text(Float.toString(sum_diff / clicked_cnt)));
    }
}