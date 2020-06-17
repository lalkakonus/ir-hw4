package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Map;


// Вероятность того, что после клика на документ, пользователь кликал по документам расположенным выше него
public class TProbGoBack extends BaseStatistic {

    public TProbGoBack(){
        KeyPrefix = StatisticConfig.PROB_GO_BACK_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (!serp.ClickedLinks.isEmpty()) {
            ListIterator<TSerp.TClickedLink> it = serp.ClickedLinks.listIterator(serp.ClickedLinks.size());
            int minPos = serp.ClickedLinks.get(serp.ClickedLinks.size() - 1).Position;
            while (it.hasPrevious()) {
                TSerp.TClickedLink clickedLink = it.previous();
                write(serp.QueryID, clickedLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
                if (clickedLink.Position > minPos) {
                    write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * 1, 0f, hashMap);
                }
                minPos = Math.min(minPos, clickedLink.Position);
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float otherClickedCnt = 0;
        float clickedCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            otherClickedCnt += Float.parseFloat(parts[0]);
            clickedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) otherClickedCnt / clickedCnt)));
    }
}