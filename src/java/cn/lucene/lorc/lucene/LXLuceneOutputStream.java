package cn.lucene.lorc.lucene;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.lucene.store.IndexOutput;

public class LXLuceneOutputStream extends OutputStream {
    private IndexOutput fos;
    
    @Override
	public String toString() {
		return String.valueOf(fos);
	}

	public LXLuceneOutputStream(IndexOutput fos) throws IOException {
    	this.fos=fos;
    }

    @Override
    public void close() throws IOException { fos.close(); }
    @Override
    public void flush() throws IOException {  }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fos.writeBytes(b, off, len);

    }
    
    @Override
    public void write(int b) throws IOException {
    	fos.writeByte((byte) b);
    }
    
    
  }
