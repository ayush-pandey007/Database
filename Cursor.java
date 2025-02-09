import java.io.IOException;
import java.nio.ByteBuffer;

public class Cursor {
    
    public Table table;
    public int rowNumber;
    public boolean endOfTable;

    public Cursor(Table table,int rowNumber,boolean endOfTable) {

        this.table = table;
        this.rowNumber =rowNumber;
        this.endOfTable = endOfTable;
    }

    public static Cursor startTable(Table table) {

        return new Cursor(table,0,table.numRows==0);
    }

    public static Cursor endTable(Table table) {

        return new Cursor(table, table.numRows, true);
    }

    public ByteBuffer cursorValue(Cursor cursor) throws IOException {

        int pageNum = cursor.rowNumber/Constant.ROWS_PER_PAGE;
        ByteBuffer page = table.pager.getFile(pageNum);
        int rowOffset = (cursor.rowNumber%Constant.ROWS_PER_PAGE)*Constant.ROW_SIZE;
        page.position(rowOffset);
        return page;
    }

    public void cursorAdvance(Cursor cursor) {

        cursor.rowNumber++;

        if(cursor.rowNumber>=table.numRows) {

            cursor.endOfTable=true;
        }
    }
}
