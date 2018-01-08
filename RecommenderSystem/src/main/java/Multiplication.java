import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	//生成同现矩阵的小单元
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input来自normalize之后的数据
			//input: movieB \t movieA=relation

			//把string写成key - value对
			String[] line = value.toString().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));

		}
	}

	//生成rating matrix的小单元
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating

			//把string写成key: movie, value: userID : rating对
			String[] line = value.toString().split(",");
			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}

	//进行小单元相乘
	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//input  注意value有两种不同类型，分别来自不同的mapper
			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>


			//output 注意这里的rating matrix 是由多个user组成的不同rating
			//key = user : movieA
			//value = sub - rating => relation * rating

			Map<String, Double> relationMap = new HashMap<String, Double>();	//记录movie及对应relative relation
			Map<String, Double> ratingMap = new HashMap<String, Double>();		//记录user及对应rating

			for (Text value: values) {
				if(value.toString().contains("=")) {	//意味着value来自同现矩阵
					String[] movie_relation = value.toString().split("=");
					relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}
				else {
					String[] user_rating = value.toString().split(":");	//意味着value来自rating matrix
					ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

			//小单元相乘并写出key-value结果对
			for(Map.Entry<String, Double> entry: relationMap.entrySet()) {
				String movie = entry.getKey();
				double relation = entry.getValue();

				for(Map.Entry<String, Double> element: ratingMap.entrySet()) {
					String user = element.getKey();
					double rating = element.getValue();
					context.write(new Text(user + ":" + movie), new DoubleWritable(relation*rating));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
