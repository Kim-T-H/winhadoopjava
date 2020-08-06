package dataexpo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//그룹을 설정하기 위한 partitioner 클래스
//hashcode를이용하여  group화 함. => 복합키인 경우 partitioner 클래스를 이용하여 그룹화함.



public class TaggedGroupKeyPartitioner extends Partitioner<TaggedKey,Text>{

	@Override
	public int getPartition(TaggedKey key, Text val, int numPartitions) {
		//항공사 코드의 해시값으로 파티션 계산
		
		
		int hash=key.getCarrierCode().hashCode();
		int partition=hash % numPartitions;
		return partition;
	}

	
	
}
