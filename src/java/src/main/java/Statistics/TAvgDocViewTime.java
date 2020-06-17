package Statistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.Math;
import java.util.Map;


// Среднее время просмотра документа
public class TAvgDocViewTime extends BaseStatistic {

    public TAvgDocViewTime(){
        KeyPrefix = StatisticConfig.AVG_VIEW_TIME_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (int i = 0; i < (serp.ClickedLinks.size() - 1); ++i) {
            long dwellTime = serp.ClickedLinks.get(i + 1).Timestamp - serp.ClickedLinks.get(i).Timestamp;
            write(serp.QueryID, serp.ClickedLinks.get(i), "BU", serp.QuerySimilarity * (int) dwellTime,
                    1f, hashMap);
        }
    }

    @Override
    public void reduce(Text word, Iterable<Text> nums, Reducer.Context context) throws IOException, InterruptedException {
        long sum_time = 0;
        float clicked_cnt = 0;
        for (Text value : nums) {
            String [] parts = value.toString().split(":");
            sum_time += Math.log(1 + Float.parseFloat(parts[0]));
            clicked_cnt += Float.parseFloat(parts[1]);
        }
        context.write(word, new Text(Float.toString(sum_time / clicked_cnt)));
    }
}
