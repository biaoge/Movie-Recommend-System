import java.io.IOException;

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


//构造还没normalize的同现矩阵
public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid \t movie1: rating1, movie2: rating2...
			//output key = movie1: movie2
			//output value = 1
			String line = value.toString().trim();
			String[] user_movieRatings = line.split("\t");

			//可能有新用户没有任何rating，属于bad input，这里忽略掉了，不处理。按道理应该throw exception。
			if(user_movieRatings.length != 2){
				return;
			}

			String[] movie_ratings = user_movieRatings[1].split(",");


			//{movie1:rating, movie2:rating..}
			//O(n^2)构造当前user看过的movie的两两组合
			//output key： movie1 : movie2
			//output value: 1
			for(int i = 0; i < movie_ratings.length; i++) {
				String movie1 = movie_ratings[i].trim().split(":")[0];
				
				for(int j = 0; j < movie_ratings.length; j++) {
					String movie2 = movie_ratings[j].trim().split(":")[0];
					context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
				}
			}
			
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//input key movie1:movie2
			//input value = iterable<1, 1, 1>
			int sum = 0;

			//word count reducer
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
