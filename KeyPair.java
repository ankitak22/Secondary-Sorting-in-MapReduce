import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the ip and time
	private Text ip;
	private Text time;
	
	//The defaule constructor
	public KeyPair()
	{
		ip = new Text();
		time= new Text();
	}
	
	//constructor, initializing the ip and time
	public KeyPair(String last, String year)
	{
		ip = new Text(last);
		time= new Text(year);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ip.readFields(in);
		time.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		ip.write(out);
		time.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= ip.compareTo(otherPair.ip);
		if (c!=0)
			return c;
		else
		{
			try
			{
				SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:SS");
			    Date val1= null;
			    Date val2= null;
			    val1 = format.parse(time.toString());
			    val2 = format.parse(otherPair.time.toString());
			    if(val1.after(val2))
			    	return -1;
			    else if(val1.before(val2))
			    	return +1;
			    else
			    	return 0;
			}
			
		catch(ParseException ex)
			{
			return c;
			}
		}
			
	}
	
	//the Getter and setter methods
	public Text getip() {
		return ip;
	}

	public void setip(Text ip) {
		this.ip = ip;
	}

	public Text gettime() {
		return time;
	}

	public void settime(Text time) {
		this.time = time;
	}

}
