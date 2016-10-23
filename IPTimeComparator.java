import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
/**
 * A comparator for the KeyPair class that compairs by lastname and then birth year Desc
 * This comparator is used for sorting the keypair by the mapreduce framework
 * @author Ellie Buxton
 *
 */
public class IPTimeComparator extends WritableComparator {
	
	public IPTimeComparator() {
		super(KeyPair.class, true);	
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getip().compareTo(key2.getip());
		if (c ==0)
		{
		try
		{
			SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:SS");
		    Date val1= null;
		    Date val2= null;
		    val1 = format.parse(key1.gettime().toString());
		    val2 = format.parse(key2.gettime().toString());
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
		else
			return c;
	}
}
