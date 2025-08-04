import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;




public class Node {
    
    // Node type and root helper methods
    public static byte getNodeType(ByteBuffer node) {
        return node.get(Constant.NODE_TYPE_OFFSET);
    }
    
    public static void setNodeType(ByteBuffer node, byte nodeType) {
        node.put(Constant.NODE_TYPE_OFFSET, nodeType);
    }
    
    public static boolean isNodeRoot(ByteBuffer node) {
        byte value = node.get(Constant.IS_ROOT_OFFSET);
        return value != 0;
    }
    
    public static void setNodeRoot(ByteBuffer node, boolean isRoot) {
        byte value = isRoot ? (byte)1 : (byte)0;
        node.put(Constant.IS_ROOT_OFFSET, value);
    }
    
    // Leaf node methods
    public static void initializeLeaf(ByteBuffer node) {
        setNodeType(node, Constant.NODE_LEAF);
        setNodeRoot(node, false);
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
    
    public static ByteBuffer getLeafNodeCell(ByteBuffer node, int cellNum) {
        int offset = getLeafNodeCellOffset(cellNum);
        node.position(offset);
        return node.slice();
    }

    // Internal node methods
    public static void initializeInternal(ByteBuffer node) {
        setNodeType(node, Constant.NODE_INTERNAL);
        setNodeRoot(node, false);
        node.position(Constant.INTERNAL_NODE_NUM_KEYS_OFFSET);
        node.putInt(0);
    }
    
    public static int getInternalNodeNumKeys(ByteBuffer node) {
        node.position(Constant.INTERNAL_NODE_NUM_KEYS_OFFSET);
        return node.getInt();
    }
    
    public static void setInternalNodeNumKeys(ByteBuffer node, int numKeys) {
        node.position(Constant.INTERNAL_NODE_NUM_KEYS_OFFSET);
        node.putInt(numKeys);
    }
    
    public static int getInternalNodeRightChild(ByteBuffer node) {
        node.position(Constant.INTERNAL_NODE_RIGHT_CHILD_OFFSET);
        return node.getInt();
    }
    
    public static void setInternalNodeRightChild(ByteBuffer node, int rightChild) {
        node.position(Constant.INTERNAL_NODE_RIGHT_CHILD_OFFSET);
        node.putInt(rightChild);
    }
    
    public static int getInternalNodeCellOffset(int cellNum) {
        return Constant.INTERNAL_NODE_HEADER_SIZE + cellNum * Constant.INTERNAL_NODE_CELL_SIZE;
    }
    
    public static int getInternalNodeChild(ByteBuffer node, int childNum) {
        int numKeys = getInternalNodeNumKeys(node);
        if (childNum > numKeys) {
            System.out.printf("Tried to access child_num %d > num_keys %d%n", childNum, numKeys);
            System.exit(1);
            return -1; // This will never be reached, but needed for compilation
        } else if (childNum == numKeys) {
            return getInternalNodeRightChild(node);
        } else {
            int offset = getInternalNodeCellOffset(childNum);
            return node.getInt(offset);
        }
    }
    
    public static void setInternalNodeChild(ByteBuffer node, int childNum, int child) {
        int numKeys = getInternalNodeNumKeys(node);
        if (childNum > numKeys) {
            System.out.printf("Tried to access child_num %d > num_keys %d%n", childNum, numKeys);
            System.exit(1);
        } else if (childNum == numKeys) {
            setInternalNodeRightChild(node, child);
        } else {
            int offset = getInternalNodeCellOffset(childNum);
            node.putInt(offset, child);
        }
    }
    
    public static int getInternalNodeKey(ByteBuffer node, int keyNum) {
        int offset = getInternalNodeCellOffset(keyNum) + Constant.INTERNAL_NODE_CHILD_SIZE;
        return node.getInt(offset);
    }
    
    public static void setInternalNodeKey(ByteBuffer node, int keyNum, int key) {
        int offset = getInternalNodeCellOffset(keyNum) + Constant.INTERNAL_NODE_CHILD_SIZE;
        node.putInt(offset, key);
    }
    
    public static int getNodeMaxKey(ByteBuffer node) {
        switch (getNodeType(node)) {
            case Constant.NODE_INTERNAL:
                int numKeys = getInternalNodeNumKeys(node);
                return getInternalNodeKey(node, numKeys - 1);
            case Constant.NODE_LEAF:
                int numCells = getLeafNodeCell(node);
                return getLeafNodeKey(node, numCells - 1);
            default:
                System.out.println("Unknown node type");
                System.exit(1);
                return -1;
        }
    }

    public static void insertLeafNode(Cursor cursor, int key, Row row) throws IOException {
        ByteBuffer node = cursor.getTable().getPager().getPage(cursor.getPageNum());
        int numCells = getLeafNodeCell(node);
        
        if (numCells >= Constant.LEAF_NODE_MAX_CELLS) {
            // Node full - split
            leafNodeSplitAndInsert(cursor, key, row);
            return;
        }

        if (cursor.getCellNum() < numCells) {
            // Make room for new cell
            for (int i = numCells; i > cursor.getCellNum(); i--) {
                ByteBuffer sourceCell = getLeafNodeCell(node, i - 1);
                int destOffset = getLeafNodeCellOffset(i);
                node.position(destOffset);
                
                // Copy the entire cell (key + value)
                byte[] cellData = new byte[Constant.LEAF_NODE_CELL_SIZE];
                sourceCell.get(cellData);
                node.put(cellData);
            }
        }
        
        // Insert new cell
        setLeafNodeCell(numCells + 1, node);
        int cellOffset = getLeafNodeCellOffset(cursor.getCellNum());
        node.position(cellOffset);
        node.putInt(key);
        serialization(row, node);
    }
    
    public static void leafNodeSplitAndInsert(Cursor cursor, int key, Row value) throws IOException {
        /*
        Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent.
        */
        
        ByteBuffer oldNode = cursor.getTable().getPager().getPage(cursor.getPageNum());
        int newPageNum = cursor.getTable().getPager().getUnusedPageNum();
        ByteBuffer newNode = cursor.getTable().getPager().getPage(newPageNum);
        initializeLeaf(newNode);
        
        /*
        All existing keys plus new key should be divided
        evenly between old (left) and new (right) nodes.
        Starting from the right, move each key to correct position.
        */
        for (int i = Constant.LEAF_NODE_MAX_CELLS; i >= 0; i--) {
            ByteBuffer destinationNode;
            if (i >= Constant.LEAF_NODE_LEFT_SPLIT_COUNT) {
                destinationNode = newNode;
            } else {
                destinationNode = oldNode;
            }
            int indexWithinNode = i % Constant.LEAF_NODE_LEFT_SPLIT_COUNT;
            int destinationOffset = getLeafNodeCellOffset(indexWithinNode);
            
            if (i == cursor.getCellNum()) {
                // Insert the new row
                destinationNode.position(destinationOffset);
                destinationNode.putInt(key);
                serialization(value, destinationNode);
            } else if (i > cursor.getCellNum()) {
                // Copy from old node (shift by 1 because we're inserting)
                ByteBuffer sourceCell = getLeafNodeCell(oldNode, i - 1);
                byte[] cellData = new byte[Constant.LEAF_NODE_CELL_SIZE];
                sourceCell.get(cellData);
                destinationNode.position(destinationOffset);
                destinationNode.put(cellData);
            } else {
                // Copy from old node
                ByteBuffer sourceCell = getLeafNodeCell(oldNode, i);
                byte[] cellData = new byte[Constant.LEAF_NODE_CELL_SIZE];
                sourceCell.get(cellData);
                destinationNode.position(destinationOffset);
                destinationNode.put(cellData);
            }
        }
        
        // Update cell count on both leaf nodes
        setLeafNodeCell(Constant.LEAF_NODE_LEFT_SPLIT_COUNT, oldNode);
        setLeafNodeCell(Constant.LEAF_NODE_RIGHT_SPLIT_COUNT, newNode);
        
        if (isNodeRoot(oldNode)) {
            createNewRoot(cursor.getTable(), newPageNum);
        } else {
            System.out.println("Need to implement updating parent after split");
            System.exit(1);
        }
    }
    
    public static void createNewRoot(Table table, int rightChildPageNum) throws IOException {
        /*
        Handle splitting the root.
        Old root copied to new page, becomes left child.
        Address of right child passed in.
        Re-initialize root page to contain the new root node.
        New root node points to two children.
        */
        
        ByteBuffer root = table.getPager().getPage(table.rootPageNum);
        ByteBuffer rightChild = table.getPager().getPage(rightChildPageNum);
        int leftChildPageNum = table.getPager().getUnusedPageNum();
        ByteBuffer leftChild = table.getPager().getPage(leftChildPageNum);
        
        // Left child has data copied from old root
        byte[] rootData = new byte[Constant.PAGE_SIZE];
        root.position(0);
        root.get(rootData);
        leftChild.position(0);
        leftChild.put(rootData);
        setNodeRoot(leftChild, false);
        
        // Root node is a new internal node with one key and two children
        initializeInternal(root);
        setNodeRoot(root, true);
        setInternalNodeNumKeys(root, 1);
        setInternalNodeChild(root, 0, leftChildPageNum);
        int leftChildMaxKey = getNodeMaxKey(leftChild);
        setInternalNodeKey(root, 0, leftChildMaxKey);
        setInternalNodeRightChild(root, rightChildPageNum);
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
