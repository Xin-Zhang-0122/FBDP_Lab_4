package relation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class projection {
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
		public String getCol(int col){
			switch(col){
			    case 0: return String.valueOf(id);
			    case 1: return name;
			    case 2: return String.valueOf(age);
			    case 3: return String.valueOf(weight);
			    default: return null;
			}
		}
	}
    public static class ProjectionMap extends Mapper<LongWritable, Text, Text, NullWritable> {
        private int col;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            col = context.getConfiguration().getInt("col", 0);
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            context.write(new Text(record.getCol(col)), NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("col", Integer.parseInt(args[2]));
        Job projectionJob = Job.getInstance(conf, "ProgectionJob");
        projectionJob.setJarByClass(projection.class);
        projectionJob.setMapperClass(ProjectionMap.class);
        projectionJob.setMapOutputKeyClass(Text.class);
        projectionJob.setMapOutputValueClass(NullWritable.class);
        projectionJob.setNumReduceTasks(0);
        projectionJob.setInputFormatClass(TextInputFormat.class);
        projectionJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(projectionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(projectionJob, new Path(args[1]));
        projectionJob.waitForCompletion(true);
    }
}