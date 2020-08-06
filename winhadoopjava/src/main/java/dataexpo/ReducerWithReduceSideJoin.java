package dataexpo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text,Text,Text>{
	
	private Text outkey=new Text();
	private Text outvalue=new Text();
	
	protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Iterator<Text> it=values.iterator();
		Text carrierName = new Text(it.next());
		while(it.hasNext()) {
			Text record = it.next();
			outkey.set(key.getCarrierCode());
			outvalue = new Text(carrierName.toString()+"\t"+record.toString()); 
			context.write(outkey, outvalue);
		}
	}



}
