package relation;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class nujoin{
	public static class nujoinMap extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			FileSplit filesplit = (FileSplit) context.getInputSplit();
      		String fileName = filesplit.getPath().getName();
			if (fileName.contains("a")){
				String[] s = line.toString().split(",");
				IntWritable id = new IntWritable(Integer.parseInt(s[0]));
				Text t = new Text();
				t.set("A"+","+s[1]+","+s[2]+","+s[3]);
				context.write(id, t);
			}
			else if (fileName.contains("b")){
				String[] s = line.toString().split(",");
				IntWritable id = new IntWritable(Integer.parseInt(s[0]));
				Text t = new Text();
				t.set("B"+","+s[1]+","+s[2]);
				context.write(id, t);
			}
		}	
	}
	public static class nujoinReduce extends Reducer<IntWritable, Text, NullWritable, Text>{
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			String A = new String();
			String B = new String();
			ArrayList<Text>arr = new ArrayList<Text>();
			for(Text val : values){
				count += 1;
				Text each = new Text();
				each.set(val.toString());
				arr.add(each);
			}
			if (count==2){  //IF TWO FILES BOTH HAVE THIS ITEM
				for(int i = 0; i < arr.size(); i++){
					if (arr.get(i).toString().split(",")[0].equals("A")){
						A = arr.get(i).toString();
					}
					else if(arr.get(i).toString().split(",")[0].equals("B")){
						B = arr.get(i).toString();
					}
				}
				Text result = new Text();
				result.set(key.toString()+","+A.split(",")[1]+","+A.split(",")[2]+","+A.split(",")[3]+","+B.split(",")[1]+","+B.split(",")[2]);
				context.write(NullWritable.get(), result);
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job nujoinJob = new Job(conf, "nujoinJob");
		nujoinJob.setJarByClass(nujoin.class);
		nujoinJob.setMapperClass(nujoinMap.class);
		nujoinJob.setReducerClass(nujoinReduce.class);
		nujoinJob.setMapOutputKeyClass(IntWritable.class);
		nujoinJob.setMapOutputValueClass(Text.class);
		nujoinJob.setOutputKeyClass(NullWritable.class);
		nujoinJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(nujoinJob, new Path(args[0]), new Path(args[1]));
      FileOutputFormat.setOutputPath(nujoinJob, new Path(args[2]));
	}
}