/**
 * Created by dingjy on 2017/9/13.
 ***/
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountCache {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{


        private final static IntWritable one = new IntWritable(1);

        private boolean caseSensitive;
        private Set<String> wordPattern = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            URI[] localURIs = context.getCacheFiles();
            Path localPath = new Path(localURIs[0].getPath());
            String localFileName = localPath.getName().toString();
            fis = new BufferedReader(new FileReader(localFileName));
            String line = null;
            while ((line = fis.readLine()) != null) {
                String words[] = line.split(" ");
                for (String wordP : words) {
                    wordPattern.add(wordP);
                }
            }

        }



        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();

            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (wordPattern.contains(token)) {
                    context.write(new Text(token), one);}
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 3)) {
            System.err.println("Usage: wordcount <in> <out> <cache>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountCache.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        job.addCacheFile(new Path(otherArgs.get(2)).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

