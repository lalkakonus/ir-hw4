import Statistics.BaseStatistic;
import Statistics.CompositeKey;
import Statistics.TSerp;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class StatisticsProcessJob extends Configured implements Tool {
    public static class StatisticsProcessMapper extends Mapper<LongWritable, Text, Text, Text> {

        TDataConverter converter;
        Map<String, String> hashMap = new HashMap<>();

        @Override
        protected void setup(Context context) {
            converter = new TDataConverter(Config.URL_MAP_FILEPATH, Config.QUERY_MAP_FILEPATH,
                                           Config.HOST_MAP_FILEPATH, Config.QUERY_DOC_CORR_MAP_FILEPATH);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                TSerp serp = converter.Convert(value.toString());
                for (BaseStatistic statisticInstance : Config.StatisticMap.values()) {
                    statisticInstance.map(serp, hashMap);
                }
            } catch (IllegalArgumentException err) {
                err.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static class StatisticsProcessReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String statisticType = CompositeKey.getKeyType(key.toString());
            BaseStatistic statisticInstance = Config.StatisticMap.get(statisticType);
            if (statisticInstance == null) {
                System.out.println("Reducer not found for class " + statisticType);
                return;
            }
            statisticInstance.reduce(key, values, context);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(StatisticsProcessJob.class);
        job.setJobName(StatisticsProcessJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(StatisticsProcessMapper.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(StatisticsProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (!fs.exists(Config.QUERY_MAP_FILEPATH) || !fs.exists(Config.URL_MAP_FILEPATH) ||
                !fs.exists(Config.HOST_MAP_FILEPATH)) {
            throw new Exception("Config file absent");
        }
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new StatisticsProcessJob(), args);
        System.exit(ret);
    }
}
