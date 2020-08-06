package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DistanceMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, LongWritable> {
	//map 출력값
	private final static LongWritable distance = new LongWritable();
	//map 출력키
	private DateKey outputkey = new DateKey();

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		if (al.isDistanceAvailable()) {
			if (al.getDistance() > 0) { // 출발지연비행기
				outputkey.setYear(al.getYear()+"");			//D, 출발지연을 알려줌
				outputkey.setMonth(al.getMonth());
				distance.set(al.getDistance());	//거리설정
				context.write(outputkey, distance);
			}
		}
	}
}
