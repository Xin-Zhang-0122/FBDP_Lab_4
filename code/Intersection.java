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

public class Intersection{
	public static class IntersectionMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			context.write(line, one);
		}
	}
	public static class IntersectionReduce extends Reducer<Text, IntWritable, Text, NullWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val : value){
				sum+=val.get();
			}
			if(sum==2)
				context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job intersectionJob = new Job(conf, "intersectionJob");
		intersectionJob.setJarByClass(Intersection.class);
		intersectionJob.setMapperClass(IntersectionMap.class);
		intersectionJob.setReducerClass(IntersectionReduce.class);
		intersectionJob.setMapOutputKeyClass(Text.class);
		intersectionJob.setMapOutputValueClass(IntWritable.class);
		intersectionJob.setOutputValueClass(NullWritable.class);
      intersectionJob.setOutputKeyClass(Text.class);
      FileInputFormat.setInputPaths(intersectionJob, new Path(args[0]), new Path(args[1]));
      FileOutputFormat.setOutputPath(intersectionJob, new Path(args[2]));
      intersectionJob.waitForCompletion(true);
	}
}