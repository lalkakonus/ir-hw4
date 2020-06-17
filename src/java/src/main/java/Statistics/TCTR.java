package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

// Отношение числа кликов по документу к числу показов (CTR)
public class TCTR extends BaseStatistic {

    public TCTR(){
        KeyPrefix = StatisticConfig.CTR_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * 1, 0f, hashMap);
        }
        for (TSerp.TShownLinks shownLink : serp.ShownLinks) {
            write(serp.QueryID, shownLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float viewedCnt = 0;
        float clickedCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            clickedCnt += Float.parseFloat(parts[0]);
            viewedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) clickedCnt / viewedCnt)));
    }
}
