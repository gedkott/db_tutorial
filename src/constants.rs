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
