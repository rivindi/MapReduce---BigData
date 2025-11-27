import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Reducer; 
import java.io.IOException; 

public class PatientDataReducer extends Reducer<Text, Text, NullWritable, Text> { 
	@Override 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
	InterruptedException { 
 
	 for (Text value : values) { 
		context.write(NullWritable.get(), value); 
	 } 
        } 
} 