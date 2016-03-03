import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class Temperature {
    
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

        public void map(LongWritable key, Text value, Context context)
        throws IOException,InterruptedException 
        {
            String line = value.toString();
            String[] parts = line.split(" ");
            int temperature = Integer.parseInt(parts[1]);
            String year = parts[0];
            context.write(new Text(year), new IntWritable(temperature));
            }
        }
        
    
    public static class Combine extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException,InterruptedException 
        {
            int maxTemp = 0;
            for (IntWritable x: values){
            	int temperature= x.get();
            	if (maxTemp < temperature){
            		maxTemp = temperature;
            	}
            }
            context.write(key, new IntWritable(maxTemp));
            
        }
        
    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException,InterruptedException 
        {
        	int maxTemp = 0;
            for (IntWritable x: values){
            	int temperature= x.get();
            	if (maxTemp < temperature){
            		maxTemp = temperature;
            	}
            }
            context.write(key, new IntWritable(maxTemp));
            
        }
        
    }
    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        
        Configuration conf= new Configuration();
        
        Job job = new Job(conf,"temperature");
        
        job.setJarByClass(Temperature.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        outputPath.getFileSystem(conf).delete(outputPath);
            
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}