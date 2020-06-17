package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


// Cредняя позиция первого кликнутого документа
public class TAvgFirstPositionQuery extends BaseStatistic {

    public TAvgFirstPositionQuery(){
        KeyPrefix = StatisticConfig.AVG_FIRST_POSITION_QUERY_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        if (!serp.ClickedLinks.isEmpty()) {
            Float value = serp.QuerySimilarity * serp.ClickedLinks.get(0).Position;
            write(serp.QueryID, EMPTY_LINK, "QE", value, 1f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sumPos = 0;
        float showWithClickCnt = 0;
        for (Text value : values) {
            String [] parts = value.toString().split(":");
            sumPos += Float.parseFloat(parts[0]);
            showWithClickCnt += Float.parseFloat(parts[1]);
        }
        context.write(key, new Text(Float.toString((float) sumPos / showWithClickCnt)));
    }
}
