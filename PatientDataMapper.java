import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Mapper; 
import java.io.IOException; 


public class PatientDataMapper extends Mapper<LongWritable, Text, Text, Text> { 
	private String filterType; 
	private String filterValue; 

	@Override 
	protected void setup(Context context) throws IOException, InterruptedException { 

	filterType = context.getConfiguration().get("filterType"); 
	filterValue = context.getConfiguration().get("filterValue"); 
        } 

	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, 
	InterruptedException { 
		String line = value.toString(); 
		if (key.get() == 0 && line.contains("PatientNHSNumber")) { 

		context.write(new Text("header"), new Text(line)); 
		return; 
		} 

	String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); 
	
	String record = String.join(",", fields); 

	switch (filterType) { 
		case "patient_id": 
		if (fields[0].equals(filterValue)) { 
		context.write(new Text(filterValue), new Text(record)); 
	} 
	break; 
	case "symptom": 
		if (fields[7].contains(filterValue)) { 
		context.write(new Text(filterValue), new Text(record)); 
	} 
	break; 
	case "region": 
		if (fields[11].equals(filterValue)) { 
		 context.write(new Text(filterValue), new Text(record)); 
	} 
	break; 
  } 
 } 
}