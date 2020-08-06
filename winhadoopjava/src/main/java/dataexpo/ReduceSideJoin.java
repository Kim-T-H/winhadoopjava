package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoin {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		String arg[] = {"D:/ubuntushare/dataexpo/carriers.csv", "D:/ubuntushare/dataexpo/1988.csv", "outfile/reducesidejoin"};
		Configuration conf = new Configuration();
		Job job =  new Job(conf, "reducesidejoin");
		FileOutputFormat.setOutputPath(job, new Path(arg[2])); // 출력파일을 설정
		job.setJarByClass(ReduceSideJoin.class); // 작업 클래스 설정
		job.setPartitionerClass(TaggedGroupKeyPartitioner.class);
		job.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
		job.setSortComparatorClass(TaggedGroupComparator.class);
		job.setReducerClass(ReducerWithReduceSideJoin.class); // Reducer 클래스 없음
		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//다중 입력파일
		MultipleInputs.addInputPath(job, new Path(arg[0]), TextInputFormat.class, CarrierCodeMapper.class);	//항공사 코드
		MultipleInputs.addInputPath(job, new Path(arg[1]), TextInputFormat.class, MapperWithReduceSideJoin.class); //비행정보
		job.waitForCompletion(true);
	}

}
