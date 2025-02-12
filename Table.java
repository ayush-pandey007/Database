import java.io.IOException;
import java.nio.ByteBuffer;

public class Table {
    
    private Pager pager;
    public int rootPageNum;




        Table(String filename) throws IOException {

           this.pager = new Pager(filename);
           this.rootPageNum  = 0;

           if(pager.getNumPages()==0) {
                ByteBuffer rootNode = pager.getPage(0);
                Node.initializeLeaf(rootNode);
           }

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
    

    
}
