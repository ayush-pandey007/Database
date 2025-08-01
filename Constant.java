public class Constant {
    
    // Node Types
    public static final byte NODE_INTERNAL = 0;
    public static final byte NODE_LEAF = 1;
    
    public static final int ID_SIZE = 4;
    public static final int USER_SIZE = 32;
    public static final int EMAIL_SIZE = 255;
    public static final int ROW_SIZE = ID_SIZE+USER_SIZE+EMAIL_SIZE;


    public static final int PAGE_SIZE = 4096;
    public static final int ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
    public static final int TABLE_MAX_PAGES = 100;
    public static final int TABLE_MAX_ROWS =ROWS_PER_PAGE * TABLE_MAX_PAGES;


    public static final int NODE_TYPE_SIZE = 1;
    public static final int NODE_TYPE_OFFSET = 0;
    public static final int IS_ROOT_SIZE = 1;
    public static final int IS_ROOT_OFFSET = NODE_TYPE_SIZE;
    public static final int PARENT_POINTER_SIZE = 4;
    public static final int PARENT_POINTER_OFFSET = IS_ROOT_SIZE+IS_ROOT_OFFSET;
    public static final int COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE+IS_ROOT_SIZE+PARENT_POINTER_SIZE;

    public static final int LEAF_NODE_NUM_CELLS_SIZE = 4;
    public static final int LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
    public static final int LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


    public static final int LEAF_NODE_KEY_SIZE = 4;
    public static final int LEAF_NODE_VALUE_SIZE = ROW_SIZE;
    public static final int LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE+LEAF_NODE_VALUE_SIZE;
    public static final int LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE-LEAF_NODE_HEADER_SIZE;
    public static final int LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS/LEAF_NODE_CELL_SIZE;
}
