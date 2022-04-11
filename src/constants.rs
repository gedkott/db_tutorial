pub const EMAIL_SIZE: usize = std::mem::size_of::<[u8; 255]>();
pub const USERNAME_SIZE: usize = std::mem::size_of::<[u8; 32]>();
pub const ID_SIZE: usize = std::mem::size_of::<u32>();
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
pub const MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const ID_OFFSET: usize = 0;
pub const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
pub const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * MAX_PAGES;

/*
 * Common Node Header Layout
 */
pub const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: u32 = 0;
pub const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
pub const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
pub const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
pub const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body Layout
 */
pub const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_KEY_OFFSET: usize = 0;
pub const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
