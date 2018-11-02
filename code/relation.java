package relation;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class relation {
	public static class RelationA{
		private int id;
		private String name;
		private int age;
		private int weight;

		public RelationA(String line){
			String[] columns = line.split(",");
			id = Integer.parseInt(columns[0]);
			name = columns[1];
			age = Integer.parseInt(columns[2]);
			weight = Integer.parseInt(columns[3]);
		}

		public boolean isCondition(int col, String value){
			if(col == 2 && Integer.parseInt(value) < age) //if(col ==2 && Integer.parseInt(value) == 18)；
				return true;
			return false;
		}
	}

	public static class SelectionMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		private int col;
		private String value;

		@Override
		public void setup(Context context){
			col = context.getConfiguration().getInt("col", 2);
			value = context.getConfiguration().get("value");
		}

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			if(record.isCondition(col, value))
				context.write(line, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("col", Integer.parseInt(args[2]));
		conf.set("value", args[3]);

		Job selectionJob = new Job(conf, "selectionJob");
		selectionJob.setJarByClass(relation.class);
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(Text.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);
		selectionJob.setNumReduceTasks(0);
		selectionJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
        selectionJob.waitForCompletion(true);
	}
}