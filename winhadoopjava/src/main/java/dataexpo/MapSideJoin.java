package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// 2개의 파일을 Mapper을 이용하여 Join 하기
public class MapSideJoin {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String arg[] = {"D:/ubuntushare/dataexpo/carriers.csv", "D:/ubuntushare/dataexpo/1988.csv", "outfile/mapsidejoin"};
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(arg[2]))) {
			hdfs.delete(new Path(arg[2]), true);
			System.out.println("기존 출력파일 삭제");
		}
		Job job =  new Job(conf, "MapSideJoin");
		job.addCacheFile(new Path(arg[0]).toUri()); // cache 파일로 저장 : carriers.csv 파일 정보를 Mapper에서 사용할 수 있도록 cache에 저장한다.
		FileInputFormat.addInputPath(job, new Path(arg[1])); // 입력파일을 설정
		FileOutputFormat.setOutputPath(job, new Path(arg[2])); // 출력파일을 설정
		job.setJarByClass(MapSideJoin.class); // 작업 클래스 설정
		job.setMapperClass(MapperWithMapSideJoin.class); // Mapper 클래스 설정
		job.setNumReduceTasks(0); // Reducer 클래스 없음
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
