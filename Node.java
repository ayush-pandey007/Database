import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;





public class Node {
    
    public static void initializeLeaf(ByteBuffer node) {
        
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.putInt(0);
    }

    public static int getLeafNodeCell(ByteBuffer node) {

        return node.getInt();
    }

    public static void setLeafNodeCell(int numCells,ByteBuffer node) {

        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.position(numCells);
    }

    public static int getLeafNodeKey(ByteBuffer node, int cellNum) {

        int offset = getLeafNodeCellOffset(cellNum);
        return node.getInt(offset);
    }

    public static ByteBuffer getLeafNodeValue(ByteBuffer node,int cellNum) {

        int offset = getLeafNodeCellOffset(cellNum)+Constant.LEAF_NODE_KEY_SIZE;
        node.position(offset);
        return node.slice();
    }

    public static int getLeafNodeCellOffset(int cellNum) {

        return Constant.LEAF_NODE_HEADER_SIZE + cellNum * Constant.LEAF_NODE_CELL_SIZE;
    }

    public static void insertLeafNode(Cursor cursor,int key,Row row) throws IOException {

        ByteBuffer node = cursor.getTable().getPager().getPage(cursor.getPageNum());
        int numCells = getLeafNodeCell(node);
        if(numCells>= Constant.LEAF_NODE_MAX_CELLS) {

            System.out.println("TODO: do split algorithm");

        }

        if(cursor.getCellNum() < numCells) {

            for(int i = numCells;i>=0;i--) {

                node.position(getLeafNodeCellOffset(i));
                ByteBuffer source = node.slice();
                node.position(getLeafNodeCellOffset(i-1));
                node.put(source);
            }
            setLeafNodeCell(numCells+1, node);
            node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
            node.putInt(key);
            serialization(row,node);
        }

    }

     private static void serialization(Row source,ByteBuffer destination) {
        destination.putInt(source.id);
        putString(destination,source.userName,Constant.USER_SIZE);
        putString(destination,source.email,Constant.EMAIL_SIZE);

    }

    private static void putString(ByteBuffer byteBuffer,String value,int maxSize) {

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        int length = Math.min(bytes.length,maxSize);

        if (byteBuffer.remaining() < maxSize) {
            throw new BufferOverflowException(); 
        }

        byteBuffer.put(bytes, 0,length);

        for(int i= length;i<maxSize;i++) {

            byteBuffer.put((byte)0);
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
