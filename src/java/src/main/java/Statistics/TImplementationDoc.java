package Statistics;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;


// Число показов документа по всем выдачам, в которых он встречался
public class TImplementationDoc extends BaseStatistic {

    public TImplementationDoc() {
        KeyPrefix = StatisticConfig.IMPLEMENTATION_DOC_KEY;
    }

    @Override
    public void map(TSerp serp, Map<String, String> hashMap) {
        for (TSerp.TShownLinks shownLink : serp.ShownLinks) {
            write(serp.QueryID, shownLink, "QU", serp.QuerySimilarity * 1, 0f, hashMap);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        float sum = 0;
        for (Text value: values) {
            String [] parts = value.toString().split(":");
            sum += Float.parseFloat(parts[0]);
        }
        context.write(key, new Text(Float.toString(sum)));
    }
}