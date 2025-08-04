import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class Pager {
    
    private RandomAccessFile file;
    private HashMap<Integer, ByteBuffer> pages = new HashMap<>();
    private int numPages;
    private static final int PAGE_SIZE = 4096;  


	public int getNumPages() {
		return this.numPages;
	}

	public void setNumPages(int numPages) {
		this.numPages = numPages;
	}


    public Pager(String filename) throws IOException {
        File f = new File(filename);
        this.file = new RandomAccessFile(f, "rw");
        this.numPages = (int)(file.length()/Constant.PAGE_SIZE);
    }

   

    public ByteBuffer getPage(int pageNum) throws IOException {
        if (pages.containsKey(pageNum)) {
            return pages.get(pageNum).duplicate(); 
        }

        ByteBuffer page = ByteBuffer.allocate(PAGE_SIZE); 

        if (pageNum < numPages) {
            file.seek(pageNum * PAGE_SIZE);  
            file.read(page.array());
        }

        pages.put(pageNum, page);
        if(pageNum>=numPages) {
            numPages = pageNum+1;
        }
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

    /*
    Until we start recycling free pages, new pages will always
    go onto the end of the database file
    */
    public int getUnusedPageNum() {
        return numPages;
    }

    // public long getFileSize() {
    //     return fileLength;
    // }
}
