import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxAccessTimeMapper 
	extends Mapper<LongWritable, Text, KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   //Extracting the fields
	  //String[] record = value.toString().split(" ");
	  
   /* 
    * Emitting The lastname and birth year from the record
    * the records are in this format: lastname  firstname birthyear-month-day , e.g., "Jones Zeke 2001-DEC-12"
    * The mapper emits a pair of lastname and birth year as key and the birth year as value, e.g. key= Keypair(Jones,2001), value=2001  
   */ 
	  String[] ipvalues = value.toString().split(" ");
	  String ip = ipvalues[0];
	  String date="";
	  if(ipvalues.length>=3)
	  date = ipvalues[3];
	  
	  String IpPattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
	  
	  Pattern p = Pattern.compile(IpPattern);
	  Matcher match = p.matcher(value.toString());
	  if(match.find())
	  {
		  
	  String newDate="";
	  if(!date.isEmpty())
	  newDate= date.substring(1,date.length());
	  
	  try
		{
			SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:SS");
		    Date val1= null;
		    val1 = format.parse(newDate.toString());
			context.write(new KeyPair(match.group(), newDate), new Text(newDate));

		}
		
	catch(ParseException ex)
		{
		}
	  
	  }
    }
}
