import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input
            //movieA:movieB \t relation

            //output
            //key: movieA
            //value: movieB:relation
            String[] movie_relation = value.toString().trim().split("\t");
            String[] movies = movie_relation[0].split(":");

            context.write(new Text(movies[0]), new Text(movies[1] + ":" + movie_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        // reduce首先把所有的relation进行一个加和
        //
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA
            //value=<movieB:relation, movieC:relation...>
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();  //map存储的是movieB及其对应的relation
            //sum up relations for movieA -> sum(denominator)
            //relative relation = relation / sum


            //outputKey: movieB
            //outputValue: movie = relative relation
            //已经是列向量了
            while (values.iterator().hasNext()) {
                String[] movie_relation = values.iterator().next().toString().split(":");
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie_relation[0], relation);
            }

            for(Map.Entry<String, Integer> entry: map.entrySet()) {
                //outputKey: movieB
                //得到outputValue: 即movieA=relative relation
                //写出normalize后的列向量
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
