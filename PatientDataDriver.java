import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

 public class PatientDataDriver { 
	public static void main(String[] args) throws Exception { 
		if (args.length != 5) { 
			System.err.println("Usage: PatientDataDriver <input path> <output path> <filterType> <filterValue>"); 
			
			System.exit(-1); 
		} 
 
	Configuration conf = new Configuration(); 
	conf.set("filterType", args[2]); 
	conf.set("filterValue", args[3]); 
	Job job = Job.getInstance(conf, "Patient Data Filter"); 
	job.setJarByClass(PatientDataDriver.class); 
	job.setMapperClass(PatientDataMapper.class); 
	job.setReducerClass(PatientDataReducer.class); 
	job.setOutputKeyClass(NullWritable.class); 
 
	job.setOutputValueClass(Text.class); 
 
	FileInputFormat.addInputPath(job, new Path(args[0])); 
	FileOutputFormat.setOutputPath(job, new Path(args[1])); 

	System.exit(job.waitForCompletion(true) ? 0 : 1); 
 } 
}