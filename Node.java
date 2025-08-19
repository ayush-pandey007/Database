import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class Node {


    public static byte getNodeType(ByteBuffer node) {


        return node.get(Constant.NODE_TYPE_OFFSET);
    }


    public static void setNodeType(ByteBuffer node,byte type) {


        node.put(Constant.NODE_TYPE_OFFSET,type);
    }
   
    public static void initializeLeaf(ByteBuffer node) {
       
        setNodeType(node,Constant.NODE_LEAF);
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.putInt(0);
        // next leaf = -1 (null)
        node.position(Constant.LEAF_NODE_NEXT_LEAF_OFFSET);
        node.putInt(-1);
    }


    public static int getLeafNodeCell(ByteBuffer node) {


        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        return node.getInt();
    }


    public static void setLeafNodeCell(int numCells,ByteBuffer node) {
        node.position(Constant.LEAF_NODE_NUM_CELLS_OFFSET);
        node.putInt(numCells);
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


    public static int getLeafNodeNextLeaf(ByteBuffer node) {
        node.position(Constant.LEAF_NODE_NEXT_LEAF_OFFSET);
        return node.getInt();
    }


    public static void setLeafNodeNextLeaf(ByteBuffer node, int nextPage) {
        node.position(Constant.LEAF_NODE_NEXT_LEAF_OFFSET);
        node.putInt(nextPage);
    }




    public static void initializeInternal(ByteBuffer node) {
        setNodeType(node, Constant.NODE_INTERNAL);
        setInternalNodeNumKeys(node, 0);
        setInternalNodeRightChild(node, -1);
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


    public static void setInternalNodeRightChild(ByteBuffer node, int pageNum) {
        node.position(Constant.INTERNAL_NODE_RIGHT_CHILD_OFFSET);
        node.putInt(pageNum);
    }


    public static int getInternalNodeChild(ByteBuffer node, int index) {
        int offset = Constant.INTERNAL_NODE_HEADER_SIZE + index * Constant.INTERNAL_NODE_CELL_SIZE;
        return node.getInt(offset);
    }


    public static void setInternalNodeChild(ByteBuffer node, int index, int childPage) {
        int offset = Constant.INTERNAL_NODE_HEADER_SIZE + index * Constant.INTERNAL_NODE_CELL_SIZE;
        node.putInt(offset, childPage);
    }


    public static int getInternalNodeKey(ByteBuffer node, int index) {
        int offset = Constant.INTERNAL_NODE_HEADER_SIZE + index * Constant.INTERNAL_NODE_CELL_SIZE + Constant.INTERNAL_NODE_CHILD_SIZE;
        return node.getInt(offset);
    }


    public static void setInternalNodeKey(ByteBuffer node, int index, int key) {
        int offset = Constant.INTERNAL_NODE_HEADER_SIZE + index * Constant.INTERNAL_NODE_CELL_SIZE + Constant.INTERNAL_NODE_CHILD_SIZE;
        node.putInt(offset, key);
    }


    public static Cursor tableFind(Table table,int key) throws IOException {


        int rootPageNum = table.rootPageNum;
        ByteBuffer rootNode = table.getPager().getPage(rootPageNum);


        if(getNodeType(rootNode) == Constant.NODE_LEAF) {
            return leafNodeFind(table,rootPageNum,key);
        } else {
            return internalNodeFind(table, rootPageNum, key);
        }
    }


    private static Cursor internalNodeFind(Table table, int pageNum, int key) throws IOException {
        ByteBuffer node = table.getPager().getPage(pageNum);
        int numKeys = getInternalNodeNumKeys(node);




        int min = 0;
        int max = numKeys;
        while (min < max) {
            int index = (min + max) / 2;
            int keyAtIndex = getInternalNodeKey(node, index);
            if (key < keyAtIndex) {
                max = index;
            } else {
                min = index + 1;
            }
        }


        int childPage = (min == numKeys) ? getInternalNodeRightChild(node) : getInternalNodeChild(node, min);
        ByteBuffer child = table.getPager().getPage(childPage);
        if (getNodeType(child) == Constant.NODE_LEAF) {
            return leafNodeFind(table, childPage, key);
        }
        return internalNodeFind(table, childPage, key);
    }


    public static Cursor leafNodeFind(Table table,int pageNum,int key) throws IOException {


        ByteBuffer node = table.getPager().getPage(pageNum);
        int numCells = getLeafNodeCell(node);
       
        Cursor cursor = new Cursor(table, pageNum, 0, false);


        int minIndex = 0;
        int onePastMaxIndex = numCells;
        while(onePastMaxIndex != minIndex) {


            int index = (minIndex+onePastMaxIndex)/2;
            int keyAtIndex = getLeafNodeKey(node, index);
            if(key==keyAtIndex) {


                cursor.setCellNum(index);
                return cursor;
            }
            if(key < keyAtIndex) {
                onePastMaxIndex = index;
            } else {
                minIndex = index + 1;
            }
        }


        cursor.setCellNum(minIndex);
        return cursor;


    }


    public static int getLeafNodeCellOffset(int cellNum) {


        return Constant.LEAF_NODE_HEADER_SIZE + cellNum * Constant.LEAF_NODE_CELL_SIZE;
    }


    public static void insertLeafNode(Cursor cursor,int key,Row row) throws IOException {


        ByteBuffer node = cursor.getTable().getPager().getPage(cursor.getPageNum());
        int numCells = getLeafNodeCell(node);
        if(numCells>= Constant.LEAF_NODE_MAX_CELLS) {
            leafSplitAndInsert(cursor, key, row);
            return;
        }




        if(cursor.getCellNum() < numCells) {
            for(int i = numCells; i > cursor.getCellNum(); i--) {


                int srcOffset = getLeafNodeCellOffset(i-1);
                int dstOffset = getLeafNodeCellOffset(i);


                int keyToCopy = node.getInt(srcOffset);
                node.putInt(dstOffset,keyToCopy);
               
                node.position(srcOffset+Constant.LEAF_NODE_KEY_SIZE);
                ByteBuffer sourceValue = node.slice();
                sourceValue.limit(Constant.LEAF_NODE_VALUE_SIZE);


                node.position(dstOffset+Constant.LEAF_NODE_KEY_SIZE);
                node.put(sourceValue);
            }
        }




        int insertOffset = getLeafNodeCellOffset(cursor.getCellNum());
        node.putInt(insertOffset, key);
        node.position(insertOffset + Constant.LEAF_NODE_KEY_SIZE);
        serialization(row, node);
        setLeafNodeCell(numCells+1, node);
    }


    private static void leafSplitAndInsert(Cursor cursor, int key, Row row) throws IOException {
        Table table = cursor.getTable();
        Pager pager = table.getPager();


        int oldPageNum = cursor.getPageNum();
        ByteBuffer oldNode = pager.getPage(oldPageNum);
        int oldNumCells = getLeafNodeCell(oldNode);




        int[] keys = new int[oldNumCells];
        byte[][] values = new byte[oldNumCells][Constant.LEAF_NODE_VALUE_SIZE];
        for (int j = 0; j < oldNumCells; j++) {
            int srcOffset = getLeafNodeCellOffset(j);
            keys[j] = oldNode.getInt(srcOffset);
            oldNode.position(srcOffset + Constant.LEAF_NODE_KEY_SIZE);
            ByteBuffer srcVal = oldNode.slice();
            srcVal.limit(Constant.LEAF_NODE_VALUE_SIZE);
            srcVal.get(values[j], 0, Constant.LEAF_NODE_VALUE_SIZE);
        }




        int newPageNum = pager.getNumPages();
        ByteBuffer newNode = pager.getPage(newPageNum);
        initializeLeaf(newNode);




        setLeafNodeCell(0, oldNode);




        int totalCells = oldNumCells + 1;
        int splitLeft = totalCells / 2; // floor




        Cursor tempFind = leafNodeFind(cursor.getTable(), oldPageNum, key);
        int insertIndex = tempFind.getCellNum();




        for (int i = 0; i < totalCells; i++) {
            boolean isNew = (i == insertIndex);
            boolean goesToNew = (i >= splitLeft);
            ByteBuffer destNode = goesToNew ? newNode : oldNode;
            int destIndex = getLeafNodeCell(destNode);
            int destOffset = getLeafNodeCellOffset(destIndex);


            if (isNew) {
                destNode.putInt(destOffset, key);
                destNode.position(destOffset + Constant.LEAF_NODE_KEY_SIZE);
                serialization(row, destNode);
            } else {
                int j = (i < insertIndex) ? i : i - 1;
                destNode.putInt(destOffset, keys[j]);
                destNode.position(destOffset + Constant.LEAF_NODE_KEY_SIZE);
                destNode.put(values[j], 0, Constant.LEAF_NODE_VALUE_SIZE);
            }
            setLeafNodeCell(destIndex + 1, destNode);
        }




        int oldNext = getLeafNodeNextLeaf(oldNode);
        setLeafNodeNextLeaf(newNode, oldNext);
        setLeafNodeNextLeaf(oldNode, newPageNum);




        int oldMaxKey = getLeafNodeKey(oldNode, getLeafNodeCell(oldNode) - 1);
        leafInsertIntoParent(table, oldPageNum, oldMaxKey, newPageNum);
    }


    private static void leafInsertIntoParent(Table table, int leftChildPage, int key, int rightChildPage) throws IOException {
        Pager pager = table.getPager();
        int rootPageNum = table.rootPageNum;


        if (leftChildPage == rootPageNum) {
            createNewRoot(table, leftChildPage, key, rightChildPage);
            return;
        }




        ByteBuffer root = pager.getPage(rootPageNum);
        if (getNodeType(root) == Constant.NODE_LEAF) {
            createNewRoot(table, leftChildPage, key, rightChildPage);
            return;
        }


        insertIntoInternal(root, leftChildPage, key, rightChildPage, pager);
    }


    private static void insertIntoInternal(ByteBuffer internalNode, int leftChildPage, int key, int rightChildPage, Pager pager) throws IOException {
        int numKeys = getInternalNodeNumKeys(internalNode);
        if (numKeys >= Constant.INTERNAL_NODE_MAX_CELLS) {


            throw new IOException("Internal node split not implemented");
        }




        int insertIndex = 0;
        while (insertIndex < numKeys && getInternalNodeChild(internalNode, insertIndex) != leftChildPage) {
            insertIndex++;
        }
        if (insertIndex == numKeys) {
           
            if (getInternalNodeRightChild(internalNode) == leftChildPage) {


                setInternalNodeChild(internalNode, numKeys, leftChildPage);
                setInternalNodeKey(internalNode, numKeys, key);
                setInternalNodeRightChild(internalNode, rightChildPage);
                setInternalNodeNumKeys(internalNode, numKeys + 1);
                return;
            }


        }




        for (int i = numKeys; i > insertIndex; i--) {
            setInternalNodeChild(internalNode, i, getInternalNodeChild(internalNode, i-1));
            setInternalNodeKey(internalNode, i, getInternalNodeKey(internalNode, i-1));
        }


        setInternalNodeChild(internalNode, insertIndex, leftChildPage);
        setInternalNodeKey(internalNode, insertIndex, key);


        if (insertIndex == numKeys) {
            setInternalNodeRightChild(internalNode, rightChildPage);
        }
        setInternalNodeNumKeys(internalNode, numKeys + 1);
    }


    private static void createNewRoot(Table table, int leftChildPage, int key, int rightChildPage) throws IOException {
        Pager pager = table.getPager();
        int newRootPage = pager.getNumPages();
        ByteBuffer root = pager.getPage(newRootPage);
        initializeInternal(root);
        setInternalNodeNumKeys(root, 1);
        setInternalNodeChild(root, 0, leftChildPage);
        setInternalNodeKey(root, 0, key);
        setInternalNodeRightChild(root, rightChildPage);
        table.rootPageNum = newRootPage;
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
