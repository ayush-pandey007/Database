import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Node {
    
    // Node type methods
    public static byte getNodeType(ByteBuffer node) {
        return node.get(Constant.NODE_TYPE_OFFSET);
    }
    
    public static void setNodeType(ByteBuffer node, byte type) {
        node.put(Constant.NODE_TYPE_OFFSET, type);
    }
    
    public static void initializeLeaf(ByteBuffer node) {
        setNodeType(node, Constant.NODE_LEAF);
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.putInt(0);
    }

    public static int getLeafNodeCell(ByteBuffer node) {
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        return node.getInt();
    }

    public static void setLeafNodeCell(int numCells, ByteBuffer node) {
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.putInt(numCells);
    }

    public static int getLeafNodeKey(ByteBuffer node, int cellNum) {
        int offset = getLeafNodeCellOffset(cellNum);
        return node.getInt(offset);
    }

    public static ByteBuffer getLeafNodeValue(ByteBuffer node, int cellNum) {
        int offset = getLeafNodeCellOffset(cellNum) + Constant.LEAF_NODE_KEY_SIZE;
        node.position(offset);
        return node.slice();
    }

    public static int getLeafNodeCellOffset(int cellNum) {
        return Constant.LEAF_NODE_HEADER_SIZE + cellNum * Constant.LEAF_NODE_CELL_SIZE;
    }

    /**
     * Return the position of the given key.
     * If the key is not present, return the position
     * where it should be inserted
     */
    public static Cursor tableFind(Table table, int key) throws IOException {
        int rootPageNum = table.rootPageNum;
        ByteBuffer rootNode = table.getPager().getPage(rootPageNum);
        
        if (getNodeType(rootNode) == Constant.NODE_LEAF) {
            return leafNodeFind(table, rootPageNum, key);
        } else {
            System.out.println("Need to implement searching an internal node");
            System.exit(1);
            return null; // This will never be reached
        }
    }
    
    public static Cursor leafNodeFind(Table table, int pageNum, int key) throws IOException {
        ByteBuffer node = table.getPager().getPage(pageNum);
        int numCells = getLeafNodeCell(node);
        
        Cursor cursor = new Cursor(table, pageNum, 0, false);
        
        // Binary search
        int minIndex = 0;
        int onePastMaxIndex = numCells;
        while (onePastMaxIndex != minIndex) {
            int index = (minIndex + onePastMaxIndex) / 2;
            int keyAtIndex = getLeafNodeKey(node, index);
            if (key == keyAtIndex) {
                cursor.setCellNum(index);
                return cursor;
            }
            if (key < keyAtIndex) {
                onePastMaxIndex = index;
            } else {
                minIndex = index + 1;
            }
        }
        
        cursor.setCellNum(minIndex);
        return cursor;
    }

    public static void insertLeafNode(Cursor cursor, int key, Row row) throws IOException {
        ByteBuffer node = cursor.getTable().getPager().getPage(cursor.getPageNum());
        int numCells = getLeafNodeCell(node);
        
        if (numCells >= Constant.LEAF_NODE_MAX_CELLS) {
            System.out.println("TODO: do split algorithm");
            return;
        }

        if (cursor.getCellNum() < numCells) {
            // Make room for new cell
            for (int i = numCells; i > cursor.getCellNum(); i--) {
                // Copy cell i-1 to cell i
                int srcOffset = getLeafNodeCellOffset(i - 1);
                int destOffset = getLeafNodeCellOffset(i);
                
                // Copy the key
                int keyToCopy = node.getInt(srcOffset);
                node.putInt(destOffset, keyToCopy);
                
                // Copy the value (row data)
                node.position(srcOffset + Constant.LEAF_NODE_KEY_SIZE);
                ByteBuffer sourceValue = node.slice();
                sourceValue.limit(Constant.LEAF_NODE_VALUE_SIZE);
                
                node.position(destOffset + Constant.LEAF_NODE_KEY_SIZE);
                node.put(sourceValue);
            }
        }
        
        setLeafNodeCell(numCells + 1, node);
        
        // Insert the new key and value
        int cellOffset = getLeafNodeCellOffset(cursor.getCellNum());
        node.putInt(cellOffset, key);
        node.position(cellOffset + Constant.LEAF_NODE_KEY_SIZE);
        serialization(row, node);
    }

    private static void serialization(Row source, ByteBuffer destination) {
        destination.putInt(source.id);
        putString(destination, source.userName, Constant.USER_SIZE);
        putString(destination, source.email, Constant.EMAIL_SIZE);
    }

    private static void putString(ByteBuffer byteBuffer, String value, int maxSize) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(bytes.length, maxSize);

        if (byteBuffer.remaining() < maxSize) {
            throw new BufferOverflowException(); 
        }

        byteBuffer.put(bytes, 0, length);

        for (int i = length; i < maxSize; i++) {
            byteBuffer.put((byte)0);
        }
    }

    private static Row deserialization(ByteBuffer source) {
        int id = source.getInt();
        String username = getString(source, Constant.USER_SIZE);
        String email = getString(source, Constant.EMAIL_SIZE);
        return new Row(id, username, email);
    }

    private static String getString(ByteBuffer byteBuffer, int maxSize) {
        byte[] bytes = new byte[maxSize];
        byteBuffer.get(bytes);
        int length = 0;
        while (length < maxSize && bytes[length] != 0) {
            length++;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }
}
