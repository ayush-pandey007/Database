import java.io.IOException;

public class Table {

    public Pager pager;
    public int numRows;

    Table(String filename) throws IOException {

       this.pager = new Pager(filename);
       this.numRows = (int)(pager.getFileSize()/Constant.ROW_SIZE);
    }

    public void close() throws IOException {

        pager.close();
    }
}