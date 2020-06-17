package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Вероятность того, что по документу кликнули два раза подряд
public class TProbDoubleClick extends BaseStatistic {

    public TProbDoubleClick(){
        KeyPrefix = StatisticConfig.PROB_DOUBLE_CLICK_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (!serp.ClickedLinks.isEmpty()) {
            for (int i = 0; i < serp.ClickedLinks.size() - 1; ++i) {
                write(serp.QueryID, serp.ClickedLinks.get(i), "BU", 0f, serp.QuerySimilarity * 1, hashMap);
                if (serp.ClickedLinks.get(i).UrlID.equals(serp.ClickedLinks.get(i + 1).UrlID)) {
                    write(serp.QueryID, serp.ClickedLinks.get(i), "BU", serp.QuerySimilarity * 1, 0f, hashMap);
                }
            }
            write(serp.QueryID, serp.ClickedLinks.get(serp.ClickedLinks.size() - 1), "BU", 0f, serp.QuerySimilarity * 1, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float doubleClickedCnt = 0;
        float clickedCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            doubleClickedCnt += Float.parseFloat(parts[0]);
            clickedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) doubleClickedCnt / clickedCnt)));
    }
}
