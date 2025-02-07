import java.io.IOException;
import java.nio.ByteBuffer;

public class Cursor {

    private Table table;
    private int rowNum;
    private boolean endOfTable;

	public Table getTable() {
		return this.table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public int getRowNum() {
		return this.rowNum;
	}

	public void setRowNum(int rowNum) {
		this.rowNum = rowNum;
	}

	public boolean isEndOfTable() {
		return this.endOfTable;
	}

	public void setEndOfTable(boolean endOfTable) {
		this.endOfTable = endOfTable;
	}


    public Cursor(Table table, int rowNum,boolean endOfTable) {

        this.table = table;
        this.rowNum = rowNum;
        this.endOfTable = endOfTable;
    }
    
    static Cursor startTable(Table table) {

        return new Cursor(table, 0, table.getNumRows()==0);
    }

    static Cursor endTable(Table table) {

        return new Cursor(table, table.getNumRows(), true);
    }

    static ByteBuffer cursorValue(Cursor cursor) throws IOException {

        int pageNum = cursor.rowNum/Constant.ROWS_PER_PAGE;
        ByteBuffer page = cursor.table.getPager().getFile(pageNum);
        int rowOffset = (cursor.rowNum%Constant.ROWS_PER_PAGE)*Constant.ROW_SIZE;
        page.position(rowOffset);
        return page;
              
    }

    static void cursorAdvance(Cursor cursor) {

        cursor.rowNum++;

        if(cursor.rowNum>=cursor.table.getNumRows()) {
            cursor.endOfTable = true;
        }
    }
}
