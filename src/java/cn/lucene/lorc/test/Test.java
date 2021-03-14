package cn.lucene.lorc.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;


import cn.lucene.lorc.OrcFile;
import cn.lucene.lorc.OrcFile.ReaderOptions;
import cn.lucene.lorc.OrcFile.WriterOptions;
import cn.lucene.lorc.Reader;
import cn.lucene.lorc.RecordReader;
import cn.lucene.lorc.TypeDescription;
import cn.lucene.lorc.Writer;

public class Test {
	public static void main(String[] args) throws IOException {
		testRead();
	}
	public static void testRead() throws IOException {
		File file = new File("E:\\tmp\\orc.test");
		Path path = new Path(file.toURI().toString());
		System.out.println(file.toURI().toString());
		ReaderOptions opt=OrcFile.readerOptions(new Configuration());
	
		Reader reader = OrcFile.createReader(path, opt);
		  SearchArgument sarg = SearchArgumentFactory.newBuilder()
                  .startAnd()
                  .equals("x", PredicateLeaf.Type.LONG, 200L)
                  .end()
                  .build();
		
		System.out.println("File schema: " + reader.getSchema());
		System.out.println("Row count: " + reader.getNumberOfRows());

		System.out.println(sarg.toString());
		// Pick the schema we want to read using schema evolution
		TypeDescription readSchema = TypeDescription.fromString("struct<x:bigint,y:string>");
		// Read the row data
		VectorizedRowBatch batch = readSchema.createRowBatch();
		RecordReader rowIterator = reader.rows(reader.options().searchArgument(sarg, new String[] {null,"x"}).schema(readSchema));
		LongColumnVector x = (LongColumnVector) batch.cols[0];
		BytesColumnVector y = (BytesColumnVector) batch.cols[1];

		while (rowIterator.nextBatch(batch)) {
			for (int row = 0; row < batch.size; ++row) {
				int xRow = x.isRepeating ? 0 : row;
//				System.out.print("y: " + y.toString(row));
//				System.out.println(",x: "  + x.vector[xRow] );
			
			
			}
			
		
		}
		rowIterator.close();
	}
	public static void testWrite() throws IOException {
		File file = new File("E:\\tmp\\orc.test");
		Path path = new Path(file.toURI().toString());
		System.out.println(file.toURI().toString());
		String optStr = "struct<x:int,y:string>";
		TypeDescription schema = TypeDescription.fromString(optStr);
		WriterOptions opts = OrcFile.writerOptions(new Configuration()).setSchema(schema);
		Writer writer = OrcFile.createWriter(path, opts);
		VectorizedRowBatch batch = schema.createRowBatch();

		System.out.println(batch.getMaxSize());

		LongColumnVector x = (LongColumnVector) batch.cols[0];
		BytesColumnVector y = (BytesColumnVector) batch.cols[1];
		for (int r = 0; r < 1000000000; ++r) {
			int row = batch.size++;
			x.vector[row] = r;
			byte[] buffer = ("last_"+r+"_"+ (r * 3)).getBytes(StandardCharsets.UTF_8);
			y.setRef(row, buffer, 0, buffer.length);
			// If the batch is full, write it out and start over.
			if (batch.size == batch.getMaxSize()) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		}
		if (batch.size != 0) {
			writer.addRowBatch(batch);
		}
		writer.close();
	}

}
