package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


// Среднее число документов, стоящих в выдаче по запросу перед документом, которые были кликнуты после документа
public class TAvgNumClickedAfter extends BaseStatistic {

    public TAvgNumClickedAfter(){
        KeyPrefix = StatisticConfig.AVG_NUMBER_CLICKED_BEFORE_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (!serp.ClickedLinks.isEmpty()) {
            TreeSet<Integer> positions = new TreeSet<>();
            ListIterator<TSerp.TClickedLink> iterator = serp.ClickedLinks.listIterator(serp.ClickedLinks.size());
            while (iterator.hasPrevious()) {
                TSerp.TClickedLink clickedLink = iterator.previous();
                positions.add(clickedLink.Position);
                Integer value = positions.size() - positions.headSet(clickedLink.Position).size();
                write(serp.QueryID, clickedLink, "BU", serp.QuerySimilarity * value, 1f, hashMap);
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
