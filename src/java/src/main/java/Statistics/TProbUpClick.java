package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

// Вероятность клика на документ, находящийся в выдаче по запросу на позицию выше текущего
public class TProbUpClick extends BaseStatistic {

    public TProbUpClick(){
        KeyPrefix = StatisticConfig.PROB_UP_CLICK_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) throws IOException, InterruptedException {
        for (TSerp.TShownLinks shownLink : serp.ShownLinks) {
            write(serp.QueryID, shownLink, "BU", 0f, serp.QuerySimilarity * 1, hashMap);
        }
        int size = serp.ShownLinks.size();
        for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
            int position = clickedLink.Position;
            if (position < size - 1) {
                write(serp.QueryID, serp.ShownLinks.get(position + 1), "BU", serp.QuerySimilarity * 1, 0f, hashMap);
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float clickedUpper = 0;
        float viewedCnt = 0;
        for (Text value: values) {
            String [] parts = value.toString().split(":");
            clickedUpper += Float.parseFloat(parts[0]);
            viewedCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) clickedUpper / viewedCnt)));
    }
}
