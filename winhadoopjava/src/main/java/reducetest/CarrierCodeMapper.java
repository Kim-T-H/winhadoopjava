package reducetest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, TaggedKey, Text>{
	TaggedKey outkey = new TaggedKey();
	Text outvalue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		CarrierCode code = new CarrierCode(value.toString());
		outkey.setCarrierCode(code.getCarrierCode());
		outkey.setTag(0);
		outvalue.set(code.getCarrierName());
		context.write(outkey, outvalue);
	}
	
	
	
}
