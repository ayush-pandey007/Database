import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;




public class SimpleDatabase {




 


    enum MetaCommandResult {


        META_COMMAND_SUCCESS,
        META_COMMAND_UNRECOGNIZED_COMMAND
    }


    enum ExecuteResult {


        EXECUTE_SUCCESS,
        EXECUTE_DUPLICATE_KEY,
        EXECUTE_TABLE_FULL
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


        String filename = (args.length < 1) ? "database.db" : args[0];


        InputBuffer inputBuffer = new InputBuffer();
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));
        Table table = new Table(filename);
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
            // System.out.println("Executed.");
           
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


                  ExecuteResult result = executeInsertStatement(statement, table);


                  switch (result) {
                    case EXECUTE_SUCCESS:
                        System.out.println("Executed.");
                        break;
                    case EXECUTE_DUPLICATE_KEY:
                        System.out.println("Error: Duplicate key");
                        break;
                    case EXECUTE_TABLE_FULL:
                        System.out.println("Error: Table full");
                        break;
                 
                    default:
                        break;
                  }
                break;


            case STATEMENT_SELECT:
                executeSelectStatement(table);
                break;
        }
    }


    private static ExecuteResult executeInsertStatement(Statement statement, Table table) throws IOException {


        Row rowToInsert = statement.RowToInsert;
        int keyToInsert = rowToInsert.id;
        Cursor cursor = Node.tableFind(table, keyToInsert);




        ByteBuffer leaf = table.getPager().getPage(cursor.getPageNum());
        int numCells = Node.getLeafNodeCell(leaf);
        if(cursor.getCellNum() < numCells) {
            int keyAtIndex = Node.getLeafNodeKey(leaf, cursor.getCellNum());
            if(keyAtIndex==keyToInsert){
                return ExecuteResult.EXECUTE_DUPLICATE_KEY;
            }
        }
       
        Node.insertLeafNode(cursor, rowToInsert.id, rowToInsert);
        return ExecuteResult.EXECUTE_SUCCESS;


       
    }


    private static void executeSelectStatement(Table table) throws IOException {


        Cursor cursor = Cursor.startTable(table);
       


        while(!cursor.isEndOfTable()) {


            ByteBuffer byteBuffer = cursor.cursorValue();
            Row row = deserialization(byteBuffer);
            PrintRow(row);
            cursor.advance();
        }
    }


    private static void PrintRow(Row row) {


        System.out.printf("(%d, %s, %s)%n", row.id, row.userName, row.email);
    }


    // private static void serialization(Row source,ByteBuffer destination) {
    //     destination.putInt(source.id);
    //     putString(destination,source.userName,Constant.USER_SIZE);
    //     putString(destination,source.email,Constant.EMAIL_SIZE);


    // }


    // private static void putString(ByteBuffer byteBuffer,String value,int maxSize) {


    //     byte[] bytes = value.getBytes(StandardCharsets.UTF_8);


    //     int length = Math.min(bytes.length,maxSize);


    //     if (byteBuffer.remaining() < maxSize) {
    //         throw new BufferOverflowException();
    //     }


    //     byteBuffer.put(bytes, 0,length);


    //     for(int i= length;i<maxSize;i++) {


    //         byteBuffer.put((byte)0);
    //     }


    // }


    private static PreparedResult prepareInsert(Statement statement,InputBuffer inputBuffer) {
       
        statement.type = StatementType.STATEMENT_INSERT;
        String input = inputBuffer.getInputBuffer();


        String[] parts = input.split(" ");
        System.out.println(parts[0]);


        try {


            int id = Integer.parseInt(parts[1]);
            String username = parts[2];
            String email = parts[3];


            if(username.length() > Constant.USER_SIZE || email.length() > Constant.EMAIL_SIZE)
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
        String username = getString(source,Constant.USER_SIZE);
        String email = getString(source,Constant.EMAIL_SIZE);


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
