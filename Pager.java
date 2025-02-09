import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class Pager {
    
    private RandomAccessFile file;
    private HashMap<Integer, ByteBuffer> pages = new HashMap<>();
    private long fileLength;
    private static final int PAGE_SIZE = 4096;  

    public Pager(String filename) throws IOException {
        File f = new File(filename);
        this.file = new RandomAccessFile(f, "rw");
        this.fileLength = file.length();
    }

    public ByteBuffer getFile(int pageNum) throws IOException {
        if (pages.containsKey(pageNum)) {
            return pages.get(pageNum).duplicate(); 
        }

        ByteBuffer page = ByteBuffer.allocate(PAGE_SIZE); 

        if (pageNum * PAGE_SIZE < fileLength) {
            file.seek(pageNum * PAGE_SIZE);  
            file.read(page.array());
        }

        pages.put(pageNum, page);
        return page;
    }

    public void flush(int pageNum) throws IOException {
        ByteBuffer buffer = pages.get(pageNum);
        if (buffer == null) return;

        file.seek(pageNum * PAGE_SIZE);
        file.write(buffer.array());
    }

    public void close() throws IOException {
        for (int pageNum : pages.keySet()) {
            flush(pageNum);
        }
        file.close();
    }

    public long getFileSize() {
        return fileLength;
    }
}
