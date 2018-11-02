package relation;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class union{
	public static class UnionMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			context.write(line, one);
		}
	}
	public static class UnionReduce extends Reducer<Text, IntWritable, Text, NullWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : value){
				sum += val.get();
			}
			if(sum > 0)
				context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job unionJob = new Job(conf, "unionJob");
		unionJob.setJarByClass(union.class);
		unionJob.setMapperClass(UnionMap.class);
		unionJob.setReducerClass(UnionReduce.class);
		unionJob.setMapOutputKeyClass(Text.class);
		unionJob.setMapOutputValueClass(IntWritable.class);
		unionJob.setOutputValueClass(NullWritable.class);
		unionJob.setOutputKeyClass(Text.class);
      FileInputFormat.setInputPaths(unionJob, new Path(args[0]), new Path(args[1]));
      FileOutputFormat.setOutputPath(unionJob, new Path(args[2]));
      unionJob.waitForCompletion(true);
	}
}