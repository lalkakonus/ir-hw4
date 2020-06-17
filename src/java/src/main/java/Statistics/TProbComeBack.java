package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Вероятность того, что к документу вернулись после клика по одной из нижерасположенных ссылок
public class TProbComeBack extends BaseStatistic {

    public TProbComeBack(){
        KeyPrefix = StatisticConfig.PROB_COME_BACK_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        int maxClickedPos = -1;
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            write(serp.QueryID, clickedLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
            if (clickedLink.Position < maxClickedPos) {
                write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * 1, 0f, hashMap);
            }
            maxClickedPos = Math.max(maxClickedPos, clickedLink.Position);
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
