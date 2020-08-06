package reducetest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text,TaggedKey,Text>{
	

	private Text outputvalue=new Text();
	
	protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Iterator<Text> iterator=values.iterator();
		Text carrierName = new Text(iterator.next());
		while(iterator.hasNext()) {
			Text record = iterator.next();			
			outputvalue = new Text(carrierName.toString()+"\t"+record.toString()); 
			context.write(key, outputvalue);
		}
	}
}
