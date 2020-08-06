package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {
	//map 출력값
	private final static IntWritable one = new IntWritable(1);
	//map 출력키
	private DateKey outputkey = new DateKey();

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		if (al.isDepartureDelayAvailable()) {
			if (al.getDepartureDelayTime() > 0) { // 출발지연비행기
				outputkey.setYear("D," + al.getYear());			//D, 출발지연을 알려줌
				outputkey.setMonth(al.getMonth());
				context.write(outputkey, one);
			} else if (al.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if (al.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}

		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
		// 도착 지연 데이터 출력
		if (al.isArriveDelayAvailable()) {
			if (al.getArriveDelayTime() > 0) {
				outputkey.setYear("A," + al.getYear());
				outputkey.setMonth(al.getMonth());
				context.write(outputkey, one);
			} else if (al.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if (al.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}
}
