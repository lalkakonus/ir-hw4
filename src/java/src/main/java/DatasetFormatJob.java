import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class DatasetFormatJob extends Configured implements Tool {

    static int DIFFERENT_STATES = 5;

    private static final Path sampleRelationPath = new Path("data/config_statistic_format/sample_relation.tsv");
    private static final Path querySamplesRelationPath = new Path("data/config_statistic_format/query_samples_relation.tsv");
    private static final Path docSamplesRelationPath = new Path("data/config_statistic_format/doc_samples_relation.tsv");
    private static final Path hostDocRelationPath = new Path("data/config_statistic_format/host_urls_relation.tsv");

    public static class DatasetFormatMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        HashMap<String, List<String>> docSampleRelation;
        HashMap<String, List<String>> querySampleRelation;
        HashMap<String, String> sampleRelation;
        HashMap<String, List<String>> hostDocRelation;

        @Override
        protected void setup(Context context) {
            docSampleRelation = loadMap(docSamplesRelationPath);
            querySampleRelation = loadMap(querySamplesRelationPath);
            sampleRelation = loadSampleMap(sampleRelationPath);
            hostDocRelation = loadMap(hostDocRelationPath);
        }

        private HashMap<String, List<String>> loadMap(Path filepath) {
            HashMap<String, List<String>> map = new HashMap<>();
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream inputStream = fs.open(filepath);
                String[] out = IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n");
                for (int i = 1; i< out.length; i++) {
                    String[] parts = out[i].split("\t");
                    String key = parts[0];
                    map.put(key, new ArrayList<>(Arrays.asList(parts[1].split(","))));
                }
                inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return map;
        }

        private HashMap<String, String> loadSampleMap(Path filepath) {
            HashMap<String, String> map = new HashMap<>();
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream inputStream = fs.open(filepath);
                String[] out = IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n");
                for (int i = 1; i< out.length; i++) {
                    String[] parts = out[i].split("\t");
                    String key = parts[1] + ":" + parts[2];
                    map.put(key, parts[0]);
                }
                inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return map;
        }


        public List<String> getCorrIDs(Integer url, boolean flag) {
            List<String> corrIDs = new ArrayList<>();
            if (flag) {
                corrIDs = hostDocRelation.get(url.toString());
            } else {
                corrIDs.add(url.toString());
            }
            return corrIDs;
        }

        private Set<String> getSamplesNumber(Integer queryID, Integer urlID, Boolean isHost) {
            Set<String> sampleIDs = new HashSet<>();
            if (urlID < 0 && queryID >= 0) {
                sampleIDs.addAll(querySampleRelation.get(queryID.toString()));
            } else if (queryID < 0 && urlID >= 0) {
                for (String url: getCorrIDs(urlID, isHost)) {
                    if (docSampleRelation.containsKey(url)) {
                        sampleIDs.addAll(docSampleRelation.get(url));
                    }
                }
            } else if (queryID >= 0 && urlID >= 0) {
                for (String url: getCorrIDs(urlID, isHost)) {
                    String compKey = queryID.toString() + ":" + url;
                    if (sampleRelation.containsKey(compKey)) {
                        sampleIDs.add(sampleRelation.get(compKey));
                    }
                }
            } else {
                throw new IllegalArgumentException("Both URL ID and query ID is missed");
            }
            return  sampleIDs;
        }

        private int getFeatureNumber(Integer queryID, Integer urlID, Boolean isHost, Integer keyNum) {
            boolean hasURL = urlID >= 0;
            boolean hasQuery = queryID >= 0;

            int state;

            if (!isHost && hasURL && hasQuery) {
                state = 0;
            } else if (isHost && hasURL && hasQuery) {
                state = 1;
            } else if (!hasURL && hasQuery){ //
                state = 2;
            } else if (!isHost && hasURL&& !hasQuery) { //
                state = 3;
            } else if (isHost && hasURL  && !hasQuery) { //
                state = 4;
            } else {
                throw new IllegalArgumentException("Wrong predicate");
            }

            return keyNum * DIFFERENT_STATES + state;
        }

        @Override
        protected void map(LongWritable splitOffset, Text line, Context context) throws IOException, InterruptedException {
            String [] parts = line.toString().split("\t");
            String value = parts[1];

            parts = parts[0].split("_");
            Integer queryID = parts[0].equals("X") ? -1 : Integer.parseInt(parts[0]);
            Integer urlID = parts[1].equals("X") ? -1 : Integer.parseInt(parts[1]);
            Boolean isHost = parts[2].charAt(0) == 'H';
            int keyNum = Integer.parseInt(parts[2].substring(1));

            int featureNumber = getFeatureNumber(queryID, urlID, isHost, keyNum);
            Set<String> samplesID = getSamplesNumber(queryID, urlID, isHost);

            for (String sampleID: samplesID) {
                context.write(new IntWritable(Integer.parseInt(sampleID)), new Text(featureNumber + ":" + value));
            }
        }
    }

    public static class DatasetFormatPartitioner extends Partitioner<IntWritable, Text> {
        static final Integer trainSamplesCnt = 202079;

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get() < trainSamplesCnt ? 0 : 1;
        }
    }

    public static class DatasetFormatReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable sampleID, Iterable<Text> features, Context context) throws IOException, InterruptedException {

            StringBuilder featureVector = new StringBuilder();
            for (Text feature: features) {
                String [] parts = feature.toString().split(":");
                int index = Integer.parseInt(parts[0]);
                String value = parts[1];
                featureVector.append(index).append(":").append(value).append(",");
            }
            Text textFeatures = new Text(featureVector.substring(0, featureVector.length() -1));
            context.write(sampleID, textFeatures);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(DatasetFormatJob.class);
        job.setJobName(DatasetFormatJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(DatasetFormatMapper.class);
        job.setNumReduceTasks(1);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setPartitionerClass(DatasetFormatPartitioner.class);

        job.setReducerClass(DatasetFormatReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (!fs.exists(docSamplesRelationPath) && !fs.exists(querySamplesRelationPath) &&
            !fs.exists(sampleRelationPath) && !fs.exists(hostDocRelationPath)) {
            throw new Exception("Config file absent");
        }
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new DatasetFormatJob(), args);
        System.exit(ret);
    }
}
