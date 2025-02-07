import java.io.IOException;

public class Table {
    
    private Pager pager;
    private int numRows;




        Table(String filename) throws IOException {

           this.pager = new Pager(filename);
           this.numRows = (int)(pager.getFileSize()/Constant.ROW_SIZE);
        }

        public void close() throws IOException {

            pager.close();
        }

        public Pager getPager() {
            return this.pager;
        }
    
        public void setPager(Pager pager) {
            this.pager = pager;
        }
    
        public int getNumRows() {
            return this.numRows;
        }
    
        public void setNumRows(int numRows) {
            this.numRows = numRows;
        }

    
}
