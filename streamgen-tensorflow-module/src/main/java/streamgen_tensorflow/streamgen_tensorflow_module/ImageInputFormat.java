package streamgen_tensorflow.streamgen_tensorflow_module;
import java.io.EOFException;
import java.io.IOException;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.util.Preconditions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;



public class ImageInputFormat extends FileInputFormat<byte[]> {
	
	static final long serialVersionUID = 42L;	
	
	
	@Override
	public byte[] nextRecord(byte[] record) throws IOException {
		Preconditions.checkState(!reachedEnd());
		Preconditions.checkState(currentSplit != null && currentSplit.getStart() == 0);
	    Preconditions.checkState(stream != null);
		return readRecord(record, currentSplit.getPath(), stream, currentSplit.getLength());
	}


	private byte[] readRecord(Object record, Path path, FSDataInputStream stream, long length) throws IOException {
		
		if(length > Integer.MAX_VALUE) {
		      throw new IllegalArgumentException("the file is too large to be fully read");
		    }
		return readFully(stream, new byte[(int) length],0, (int) length);
	}


	

	private byte[] readFully(FSDataInputStream stream, byte[] bytes, int off, int length) throws IOException  {
			int bytesRead = 0;
			while (bytesRead < length) {
				int read = stream.read(bytes, off + bytesRead, length - bytesRead);
				if (read < 0) throw new EOFException("Premature end of stream");
			    bytesRead += read;
			}
			return bytes;
		}


	@Override
	public boolean reachedEnd() throws IOException {
		return stream.getPos() != 0;
	}

	ImageInputFormat() {
		this.unsplittable = true;
	}


	


}
