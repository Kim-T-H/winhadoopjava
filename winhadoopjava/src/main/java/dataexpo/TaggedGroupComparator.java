package dataexpo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedGroupComparator extends WritableComparator{
	protected TaggedGroupComparator() {
		super(TaggedKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1,WritableComparable w2) {
		TaggedKey k1=(TaggedKey) w1;
		TaggedKey k2=(TaggedKey) w2;
		return k1.getCarrierCode().compareTo(k2.getCarrierCode())*(-1);
	}

}
