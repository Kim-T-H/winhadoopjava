package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper 클래스 : 맵의 기능을 처리
// 원본 데이터를 읽어서 매핑작업을 진행 --> 결과를 내부적으로 작성하여 Reducer의 입력값으로 사용
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// LongWritable : line의 수, TEXT : 원본의 data
	// IntWritable : 결과에 대한 데이터
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// value: this is a pen
		// StringTokenizer : 문자열을 토큰화 해주는 클래스 (공백을 기준으로 분리된 문자열)		
		StringTokenizer itr = new StringTokenizer(value.toString());
		// word: this
		while(itr.hasMoreTokens()) {
			word.set(itr.nextToken()); // this
			context.write(word, one); // context는 Reducer의 입력값으로 들어간다.
			// this, 1, 1
			// is, 1, 1
			// a, 1
			// book, 1
		}
	}
	 
}