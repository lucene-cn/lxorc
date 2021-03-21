package cn.lucene.lorc.lucene;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.lucene.store.IndexInput;

public class LXLuceneInputStream extends FSInputStream  {
    private IndexInput fis;
    private long position;

    public LXLuceneInputStream(IndexInput f) throws IOException {
      this.fis = f;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0) {
        throw new EOFException(
          FSExceptionMessages.NEGATIVE_SEEK);
      }
      fis.seek(pos);
      this.position = pos;
    }
    
    @Override
    public long getPos() throws IOException {
      return this.position;
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
    

    @Override
    public void close() throws IOException { fis.close(); }
    @Override
    public boolean markSupported() { return false; }
    
    @Override
    public int read() throws IOException {
    	return fis.readByte();
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
    	fis.readBytes(b, off, len);
    	return len;
    }
    

 
  }
