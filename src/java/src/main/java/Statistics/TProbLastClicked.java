package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Вероятность быть последним кликнутым документом
public class TProbLastClicked extends BaseStatistic {

    public TProbLastClicked(){
        KeyPrefix = StatisticConfig.PROB_LAST_CLICKED_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) throws IOException, InterruptedException {
        if (!serp.ClickedLinks.isEmpty()) {
            for (TSerp.TClickedLink clickedLink: serp.ClickedLinks) {
                write(serp.QueryID, clickedLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
            }
            int size = serp.ClickedLinks.size();
            write(serp.QueryID, serp.ClickedLinks.get(size - 1), "BU", serp.QuerySimilarity * 1, 0f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float lastClickedCnt = 0;
        float clickedCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            lastClickedCnt += Float.parseFloat(parts[0]);
            clickedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) lastClickedCnt / clickedCnt)));
    }
}
