package dataexpo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {
	private Hashtable <String, String> joinMap = new Hashtable<String, String>();
	private Text outkey = new Text();
	@Override
	protected void setup(Context context) // mapper가 시작할 때 설정하는 메서드
			throws IOException, InterruptedException {
		try {
			URI[] cacheFiles = context.getCacheFiles(); // cache 파일의 정보들을 리턴
			if(cacheFiles != null && cacheFiles.length > 0 ) {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString())); // cacheFiles[0].toString() : cache 파일의 위치를 문자열로 리턴
				while((line = br.readLine()) != null) {
					CarrierCode code = new CarrierCode(line);
					joinMap.put(code.carrierCode, code.getCarrierName()); // < 항공사 코드, 항공사 이름 >
				}
				br.close();
			}else {
				System.out.println("cache file is null");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		outkey.set(al.getUniqueCarrier());
		context.write(outkey, new Text(joinMap.get(al.getUniqueCarrier()) + "\t" + value.toString()));
		
	}
	
	
}
