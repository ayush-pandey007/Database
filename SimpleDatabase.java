import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class SimpleDatabase {

    private static final int ID_SIZE = 4;
    private static final int USER_SIZE = 32;
    private static final int EMAIL_SIZE = 255;
    private static final int ROW_SIZE = ID_SIZE+USER_SIZE+EMAIL_SIZE;


    private static final int PAGE_SIZE = 4096;
    private static final int ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
    private static final int TABLE_MAX_PAGES = 100;
    private static final int TABLE_MAX_ROWS =ROWS_PER_PAGE * TABLE_MAX_PAGES;

    static class Row {

         private int id;
         private String userName;
         private String email;

        Row(int id,String userName,String email) {

            this.id = id;
            this.userName = userName;
            this.email = email;
        }
    }

    static class Cursor {

        private Table table;
        private int rowNum;
        private boolean endOfTable;

        public Cursor(Table table, int rowNum,boolean endOfTable) {

            this.table = table;
            this.rowNum = rowNum;
            this.endOfTable = endOfTable;
        }
    }

    static Cursor startTable(Table table) {

        return new Cursor(table, 0, table.numRows==0);
    }

    static Cursor endTable(Table table) {

        return new Cursor(table, table.numRows, true);
    }

    static ByteBuffer cursorValue(Cursor cursor) throws IOException {

        int pageNum = cursor.rowNum/ROWS_PER_PAGE;
        ByteBuffer page = cursor.table.pager.getFile(pageNum);
        int rowOffset = (cursor.rowNum%ROWS_PER_PAGE)*ROW_SIZE;
        page.position(rowOffset);
        return page;
              
    }

    static void cursorAdvance(Cursor cursor) {

        cursor.rowNum++;

        if(cursor.rowNum>=cursor.table.numRows) {
            cursor.endOfTable = true;
        }
    }

    static class Table {

        private Pager pager;
        private int numRows;

        Table(String filename) throws IOException {

           this.pager = new Pager(filename);
           this.numRows = (int)(pager.getFileSize()/ROW_SIZE);
        }

        public void close() throws IOException {

            pager.close();
        }
    }
    

    

    enum MetaCommandResult {

        META_COMMAND_SUCCESS,
        META_COMMAND_UNRECOGNIZED_COMMAND
    }
    
    enum PreparedResult {
    
        PREPARED_SUCCESS,
        PREPARED_SYNTAX_ERROR,
        PREPARED_UNRECOGNIZED_STATEMENT
    }
    
    enum StatementType {
    
        STATEMENT_INSERT,
        STATEMENT_SELECT
    }
    
    static class Statement {
    
        StatementType type;
        Row RowToInsert;
    }
    
    
    private static class InputBuffer {

        private String inputBuffer;


        public String getInputBuffer() {

            return inputBuffer;
        }

        public void setInputBuffer(String inputBuffer) {

            this.inputBuffer = inputBuffer;
        }


    }

    public static void printPrompt() {

        System.out.print("db > ");
    }

    public static void readInput(InputBuffer inputBuffer,BufferedReader bufferReader) {

        try{

            String line = bufferReader.readLine();

            inputBuffer.setInputBuffer(line.trim());

        } catch(IOException e) {

            System.out.println("Somethign went wrong"+e.getMessage());
        }

    }

    public static void main(String[] args) throws IOException {

        if(args.length < 0) {
            System.out.println("Enter file name please");
        }

        InputBuffer inputBuffer = new InputBuffer();
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));
        Table table = new Table(args[0]);
        while(true) {
            printPrompt();
            readInput(inputBuffer,bufferReader);


            if(inputBuffer.getInputBuffer().startsWith(".")) {

                switch (doMetaCommand(inputBuffer.getInputBuffer(),bufferReader,table)) {
                    case META_COMMAND_SUCCESS:                       
                        continue;
                    case META_COMMAND_UNRECOGNIZED_COMMAND:
                        System.out.println("Unrecognized keyword at start of '" + inputBuffer.getInputBuffer() + "'.");
                        continue;
                
                }
            }


            Statement statement = new Statement();
            switch (prepareStatement(statement,inputBuffer)) {
                case PREPARED_SUCCESS:          
                    break;

                case PREPARED_UNRECOGNIZED_STATEMENT:
                    System.out.println("Unrecognized statment");
                    continue;
                case PREPARED_SYNTAX_ERROR:
                    System.out.println("Syntax Error");
                    continue;
            
            }

            executeStatement(statement,table);
            System.out.println("Executed.");
            
        }

    }

    private static MetaCommandResult doMetaCommand(String buffer,BufferedReader bufferReader,Table table) {

        if(buffer.equals(".exit")) {
            System.out.println("Exiting the program.");
            try {
                bufferReader.close();
                table.close();
                
            } catch(IOException e) {
                System.out.println(e.getMessage());
            }
            System.exit(0);
        }

        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }

    private static PreparedResult prepareStatement(Statement statement,InputBuffer inputBuffer) {

        if(inputBuffer.getInputBuffer().startsWith("insert")) {

            // statement.type = StatementType.STATEMENT_INSERT;

            // return PreparedResult.PREPARED_SUCCESS;
            return prepareInsert(statement,inputBuffer);
        }
        else if(inputBuffer.getInputBuffer().startsWith("select")) {

            statement.type = StatementType.STATEMENT_SELECT;

            return PreparedResult.PREPARED_SUCCESS;
        }

        return PreparedResult.PREPARED_UNRECOGNIZED_STATEMENT;
    }

    private static void executeStatement(Statement statement,Table table) throws IOException {

        switch (statement.type) {
            case STATEMENT_INSERT:
                // System.out.println("TODO : insert functionallity");
                executeInsertStatement(statement, table);
                break;

            case STATEMENT_SELECT:
                // System.out.println("TODO : select functionallity");
                executeSelectStatement(table);
                break;
        }
    }

    private static void executeInsertStatement(Statement statement, Table table) throws IOException {

        if(table.numRows>=TABLE_MAX_ROWS) {
            System.out.println("Table is full");
            return;
        }

        // int rowNumber = table.numRows;
        // int pageNum = rowNumber/ROWS_PER_PAGE;
        // int offset = (rowNumber%ROWS_PER_PAGE)*ROW_SIZE;
        // // byte[] destination = rowSlot(table,offset);
        // ByteBuffer byteBuffer = table.pager.getFile(pageNum);

        Cursor cursor = endTable(table);

        ByteBuffer byteBuffer = cursorValue(cursor);

        serialization(statement.RowToInsert,byteBuffer);
        table.numRows++;
    }

    private static void executeSelectStatement(Table table) throws IOException {

        // for(int i=0;i<table.numRows;i++) {

        //     int pageNum = i / ROWS_PER_PAGE;
        //     // byte[] page = table.pages[pageNum];
        //     int offset = (i%ROWS_PER_PAGE)*ROW_SIZE;
        //     // byte[] source = table.pages[pageNum];
        //     ByteBuffer byteBuffer = table.pager.getFile(pageNum);
        //     Row row = deserialization(byteBuffer,offset);
        //     PrintRow(row);
        // }
        Cursor cursor = startTable(table);
        

        while(!cursor.endOfTable) {

            ByteBuffer byteBuffer = cursorValue(cursor);
            Row row = deserialization(byteBuffer);
            PrintRow(row);
            cursorAdvance(cursor);
        }
    }

    private static void PrintRow(Row row) {

        System.out.printf("(%d, %s, %s)%n", row.id, row.userName, row.email);
    }

    // private static byte[] rowSlot(Table table,int offset) {

    //     int pageNum = table.numRows / ROWS_PER_PAGE; // Calculate which page the row belongs to

    //     if (table.pages[pageNum] == null) { // Allocate the page if it doesn't exist
    //         table.pages[pageNum] = new byte[PAGE_SIZE];
    //     }
    
    //     return table.pages[pageNum]; // Return the page
    // }

    private static void serialization(Row source,ByteBuffer destination) {

        // // ByteBuffer byteBuffer = ByteBuffer.wrap(destination);
        // if (offset + ROW_SIZE > destination.capacity()) {
        // throw new BufferOverflowException(); 
        // }
        // destination.position(offset);
        destination.putInt(source.id);
        putString(destination,source.userName,USER_SIZE);
        putString(destination,source.email,EMAIL_SIZE);

    }

    private static void putString(ByteBuffer byteBuffer,String value,int maxSize) {

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        int length = Math.min(bytes.length,maxSize);

        if (byteBuffer.remaining() < maxSize) {
            throw new BufferOverflowException(); // Prevent overflow
        }

        byteBuffer.put(bytes, 0,length);

        for(int i= length;i<maxSize;i++) {

            byteBuffer.put((byte)0);
        }

    }

    private static PreparedResult prepareInsert(Statement statement,InputBuffer inputBuffer) {
        
        statement.type = StatementType.STATEMENT_INSERT;
        String input = inputBuffer.getInputBuffer();

        String[] parts = input.split(" ");
        System.out.println(parts[0]);

        try {

            int id = Integer.parseInt(parts[1]);
            String username = parts[2];
            String email = parts[3];

            if(username.length() > USER_SIZE || email.length() > EMAIL_SIZE)
                return PreparedResult.PREPARED_SYNTAX_ERROR;
            
            statement.RowToInsert = new Row(id, username, email);
            return PreparedResult.PREPARED_SUCCESS;
        } catch (NumberFormatException e) {

            return PreparedResult.PREPARED_SYNTAX_ERROR;
        }



    }

    private static Row deserialization(ByteBuffer source) {

        // ByteBuffer byteBuffer = ByteBuffer.wrap(source);
        // source.position(offset);
        int id = source.getInt();
        String username = getString(source,USER_SIZE);
        String email = getString(source,EMAIL_SIZE);

        return new Row(id,username,email);
    }

    private static String getString(ByteBuffer byteBuffer,int maxSize) {

        byte[] bytes = new byte[maxSize];

        byteBuffer.get(bytes);

        int length = 0;

        while(length<maxSize && bytes[length]!=0) {

            length++;
        }

        return new String(bytes,0,length,StandardCharsets.UTF_8);

    }
}


