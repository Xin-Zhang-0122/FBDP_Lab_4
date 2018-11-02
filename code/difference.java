package relation;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class difference{
	public static class DifferenceMap extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
      		String fileName = fileSplit.getPath().getName();
      		if (fileName.contains("1")){
      			Text A = new Text();
      			A.set("A");
      			context.write(line, A);
      		}
      		else if (fileName.contains("2")){
      			Text B = new Text();
      			B.set("B");
      			context.write(line, B);
      		}
		}
	}
	public static class DifferenceReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val : values){
				if (!val.toString().equals("A"))
					return;
			}
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job differenceJob = new Job(conf, "differenceJob");
		differenceJob.setJarByClass(difference.class);
		differenceJob.setMapperClass(DifferenceMap.class);
		differenceJob.setReducerClass(DifferenceReduce.class);
		differenceJob.setMapOutputKeyClass(Text.class);
		differenceJob.setMapOutputValueClass(Text.class);
		differenceJob.setOutputKeyClass(Text.class);
		differenceJob.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(differenceJob, new Path(args[0]), new Path(args[1]));
      FileOutputFormat.setOutputPath(differenceJob, new Path(args[2]));
	}
}