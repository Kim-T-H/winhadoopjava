package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DistanceWithDateKey {
//월별 운항거리 출력
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DistanceWithDateKey");

		// 입출력 데이터 경로 설정
		String in = "D:/ubuntushare/dataexpo/1988.csv";
		String out = "outfile/distance-1988";
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out), true); 
			System.out.println("기본 출력파일 삭제");
		}
		// Job 클래스 설정
		job.setJarByClass(DistanceWithDateKey.class);
		// Mapper 클래스 설정
		job.setMapperClass(DistanceMapperWithDateKey.class);
		// Reducer 클래스 설정
		job.setReducerClass(DistanceReducerWithDateKey.class); 

		// 키를 설정: WritableComparable 인터페이스를 구현 해야함. 키로 정렬됨.
		job.setMapOutputKeyClass(DateKey.class);

		// value 설정 : Writable 인터페이스를 구현 해야함.
		job.setMapOutputValueClass(LongWritable.class);

		// 입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 출력 키 및 출력값 유형 설정
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}
}