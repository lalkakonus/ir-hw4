package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


// Среднее число документов, стоящих в выдаче по запросу перед документом, которые были кликнуты перед документом.
public class TAvgNumClickedBefore extends BaseStatistic {
    public TAvgNumClickedBefore(){
        KeyPrefix = StatisticConfig.AVG_NUMBER_CLICKED_AFTER_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (!serp.ClickedLinks.isEmpty()) {
            TreeSet<Integer> positions = new TreeSet<>();
            for (TSerp.TClickedLink clickedLink : serp.ClickedLinks) {
                positions.add(clickedLink.Position);
                int lessCnt = positions.headSet(clickedLink.Position).size();
                write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * lessCnt, 1f, hashMap);
            }
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
