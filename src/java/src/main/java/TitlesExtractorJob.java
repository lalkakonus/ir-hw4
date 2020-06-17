import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.min;


public class TitlesExtractorJob extends Configured implements Tool {
    public static class TitlesExtractorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] docInfo = value.toString().split("\t");
            int docID = Integer.parseInt(docInfo[0]);
            if (docInfo.length < 2) {
                context.write(new IntWritable(docID), new Text(""));
                return;
            }
            String title = docInfo[1];
            if (title.length() == 0) {
                List<String> splitted_text = Arrays.asList(docInfo[2].split(" "));
                title = String.join(" ", splitted_text.subList(0, min(splitted_text.size() - 1, 500)));
            }
            context.write(new IntWritable(docID), new Text(title));
        }

    }

    public static class TitlesExtractorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(TitlesExtractorJob.class);
        job.setJobName(TitlesExtractorJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(TitlesExtractorMapper.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(TitlesExtractorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new TitlesExtractorJob(), args);
        System.exit(ret);
    }
}
