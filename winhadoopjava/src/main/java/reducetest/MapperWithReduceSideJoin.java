package reducetest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithReduceSideJoin extends Mapper<LongWritable, Text, TaggedKey, Text>{

	TaggedKey outputkey = new TaggedKey();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) // 입력 스플릿에서 각 키/값 쌍에대해 한번 호출된다.
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		outputkey.setCarrierCode(al.getUniqueCarrier());
		outputkey.setTag(al.getMonth());
		context.write(outputkey, value);
	}

}
