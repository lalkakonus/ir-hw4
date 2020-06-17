package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

// Вероятность клика на документ, находящиеся в выдаче на позицию ниже
public class TProbDownClick extends BaseStatistic {

    public TProbDownClick(){
        KeyPrefix = StatisticConfig.PROB_DOWN_CLICK_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (TSerp.TShownLinks shownLink : serp.ShownLinks) {
            write(serp.QueryID, shownLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
        }
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            int idx = clickedLink.Position;
            if (idx > 0) {
                write(serp.QueryID, serp.ShownLinks.get(idx - 1), "BU", serp.QuerySimilarity * 1, 0f, hashMap);
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float clickedLower = 0;
        float viewedCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            clickedLower += Float.parseFloat(parts[0]);
            viewedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) clickedLower / viewedCnt)));
    }
}
