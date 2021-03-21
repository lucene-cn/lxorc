package cn.lucene.lorc.impl.reader;

import java.util.HashMap;

import cn.lucene.orc.OrcProto;

public interface IOrcSkip {
	public HashMap<Integer, Boolean> getSkipStripe();
	public int getIndex();
	public boolean isSkip( OrcProto.ColumnStatistics stat,String from);
}
