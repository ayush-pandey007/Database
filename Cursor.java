import java.io.IOException;
import java.nio.ByteBuffer;

public class Cursor {

    private Table table;
    private int pageNum;
    private int cellNum;
    private boolean endOfTable;

	public Table getTable() {
		return this.table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public int getPageNum() {
		return this.pageNum;
	}

	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}

	public int getCellNum() {
		return this.cellNum;
	}

	public void setCellNum(int cellNum) {
		this.cellNum = cellNum;
	}

	public boolean isEndOfTable() {
		return this.endOfTable;
	}

	public void setEndOfTable(boolean endOfTable) {
		this.endOfTable = endOfTable;
	}



    public Cursor(Table table, int pageNum,int cellNum,boolean endOfTable) {

        this.table = table;
        // this.rowNum = rowNum;
        this.pageNum = pageNum;
        this.cellNum = cellNum;
        this.endOfTable = endOfTable;
    }
    
    static Cursor startTable(Table table) throws IOException {

        ByteBuffer rootNode = table.getPager().getPage(table.rootPageNum);
        int numCells = Node.getLeafNodeCell(rootNode);
        return new Cursor(table, table.rootPageNum, numCells, true);
        // return new Cursor(table, 0, table.getNumRows()==0);

    }

    static Cursor endTable(Table table) throws IOException {

        ByteBuffer rootNode = table.getPager().getPage(table.rootPageNum);
        int numCells = Node.getLeafNodeCell(rootNode);
        return new Cursor(table, table.rootPageNum, numCells, false);
        // return new Cursor(table, table.getNumRows(), true);
    }

    public ByteBuffer cursorValue() throws IOException {
        ByteBuffer node = table.getPager().getPage(pageNum);
        int cellOffset = Node.getLeafNodeCellOffset(cellNum) + Constant.LEAF_NODE_KEY_SIZE;
        node.position(cellOffset);
        return node.slice();
    }
    

    // static ByteBuffer cursorValue(Cursor cursor) throws IOException {

    //     int pageNum = cursor.rowNum/Constant.ROWS_PER_PAGE;
    //     ByteBuffer page = cursor.table.getPager().getFile(pageNum);
    //     int rowOffset = (cursor.rowNum%Constant.ROWS_PER_PAGE)*Constant.ROW_SIZE;
    //     page.position(rowOffset);
    //     return page;
              
    // }

    // static void cursorAdvance(Cursor cursor) {

    //     cursor.rowNum++;

    //     if(cursor.rowNum>=cursor.table.getNumRows()) {
    //         cursor.endOfTable = true;
    //     }
    // }

    public void advance() throws IOException {

        ByteBuffer node = table.getPager().getPage(pageNum);
        cellNum++;

        if (cellNum >= Node.getLeafNodeCell(node)) {
            endOfTable = true;
        }
    }
}
