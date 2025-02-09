public class Constant {
    
    public static final int ID_SIZE = 4;
    public static final int USER_SIZE = 32;
    public static final int EMAIL_SIZE = 255;
    public static final int ROW_SIZE = ID_SIZE+USER_SIZE+EMAIL_SIZE;


    public static final int PAGE_SIZE = 4096;
    public static final int ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
    public static final int TABLE_MAX_PAGES = 100;
    public static final int TABLE_MAX_ROWS =ROWS_PER_PAGE * TABLE_MAX_PAGES;
}
