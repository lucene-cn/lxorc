package cn.lucene.lorc.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;

import cn.lucene.lorc.ColumnStatistics;
import cn.lucene.lorc.OrcFile;
import cn.lucene.lorc.OrcFile.ReaderOptions;
import cn.lucene.lorc.OrcFile.WriterOptions;
import cn.lucene.lorc.Reader;
import cn.lucene.lorc.RecordReader;
import cn.lucene.lorc.StripeStatistics;
import cn.lucene.lorc.TypeDescription;
import cn.lucene.lorc.Writer;
import cn.lucene.lorc.impl.reader.IOrcSkip;
import cn.lucene.lorc.lucene.LXLuceneInputStream;
import cn.lucene.lorc.lucene.LXLuceneOutputStream;
import cn.lucene.orc.OrcProto;

public class Test {
	public static void main(String[] args) throws IOException {
	
		testRead();
	}
	
	
	public static void testRead() throws IOException {
		
		File file = new File("E:\\tmp\\orc_lucene.test");
		FSDirectory dir=FSDirectory.open(file.toPath());
		IndexInput input=dir.openInput("tim.orc", IOContext.DEFAULT);
	
		Path path = new Path(file.toURI().toString());
		System.out.println(file.toURI().toString());
		
		FixedBitSet bitset=new FixedBitSet(1000000000);
		bitset.set(100000500);
		bitset.set(300000500);
		bitset.set(500000500);
		bitset.set(600000500, 600000600);

		ReaderOptions opt=OrcFile.readerOptions(new Configuration());
	
		Reader reader = OrcFile.createReader(path,new FSDataInputStream(new LXLuceneInputStream(input)),input.length(), opt);
		
		  int xindex=reader.getSchema().findSubtype("x").getId();
		  HashMap<Integer, Boolean> skip=new HashMap<>();

			IOrcSkip check_skip=new IOrcSkip() {
				
				@Override
				public boolean isSkip( OrcProto.ColumnStatistics stat,String from) {
				
					int index=(int) (stat.getIntStatistics().getMinimum());
					if(index>bitset.length())
					{
						return true;
					}
					
				

					int val=bitset.nextSetBit(index);
					if(stat.getIntStatistics().getMinimum()<=val&&val<=stat.getIntStatistics().getMaximum())
					{

						return false;
					}

					return true;
				
				}
				
				@Override
				public int getIndex() {
					return xindex;
				}

				@Override
				public HashMap<Integer, Boolean> getSkipStripe() {
					return skip;
				}
			};
		
		  System.out.println(reader.getStatistics()[xindex]);
		  List<StripeStatistics> strip=reader.getStripeStatistics();
		  for(int i=0;i<strip.size();i++)
		  {
			  skip.put(i, check_skip.isSkip(strip.get(i).getColumn(xindex),"stripe"));
		  }
		  
		  System.out.println(skip);
		  
		  System.out.println(reader.getSchema().getFieldNames());
		  
		  System.out.println();
		
		System.out.println("File schema: " + reader.getSchema());
		System.out.println("Row count: " + reader.getNumberOfRows());

		// Pick the schema we want to read using schema evolution
		TypeDescription readSchema = TypeDescription.fromString("struct<x:bigint,y:string>");
		// Read the row data
		VectorizedRowBatch batch = readSchema.createRowBatch();
		RecordReader rowIterator = reader.rows(check_skip,reader.options().schema(readSchema));
		LongColumnVector x = (LongColumnVector) batch.cols[0];
		BytesColumnVector y = (BytesColumnVector) batch.cols[1];

		int cnt=0;
		while (rowIterator.nextBatch(batch)) {
			for (int row = 0; row < batch.size; ++row) {
				int xRow = x.isRepeating ? 0 : row;
				
				if(x.vector[xRow]<0||x.vector[xRow]>bitset.length())
				{
					continue; 
				}

				if(bitset.get((int) x.vector[xRow]))
				{
					System.out.print((cnt++) +" y: " + y.toString(row));
					System.out.println(",x: "  + x.vector[xRow] );
				}
				
		
			}
			
			
//		
		}
		rowIterator.close();
		
		System.out.println("finish");
	}
	public static void testWrite() throws IOException {
		File file = new File("E:\\tmp\\orc_lucene.test");
		FSDirectory dir=FSDirectory.open(file.toPath());
		IndexOutput output=dir.createOutput("tim.orc", IOContext.DEFAULT);
	
		String optStr = "struct<x:int,y:string,z:string>";
		TypeDescription schema = TypeDescription.fromString(optStr);
		WriterOptions opts = OrcFile.writerOptions(new Configuration()).setSchema(schema);
		Writer writer = OrcFile.createWriter(new Path(file.getPath()),new FSDataOutputStream(new LXLuceneOutputStream(output),null), opts);
		VectorizedRowBatch batch = schema.createRowBatch();

		System.out.println(batch.getMaxSize());

		LongColumnVector x = (LongColumnVector) batch.cols[0];
		BytesColumnVector y = (BytesColumnVector) batch.cols[1];
		BytesColumnVector z = (BytesColumnVector) batch.cols[2];

		for (int r = 0; r < 2000000000; ++r) {
			int row = batch.size++;
			x.vector[row] = r;
		
			{
				byte[] buffer = (String.valueOf(r%10000)).getBytes(StandardCharsets.UTF_8);
				y.setRef(row, buffer, 0, buffer.length);

			}
			{
				byte[] buffer = (String.valueOf((long)(r/10000))).getBytes(StandardCharsets.UTF_8);
				z.setRef(row, buffer, 0, buffer.length);
			}
		
			if (batch.size == batch.getMaxSize()) {
				writer.addRowBatch(batch);
				batch.reset();
			}
			
			if(r%100000==0)
			{
				System.out.println(r);
			}
		}
		if (batch.size != 0) {
			writer.addRowBatch(batch);
		}
		writer.close();
	}

}
