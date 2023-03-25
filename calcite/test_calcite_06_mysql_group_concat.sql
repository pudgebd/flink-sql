SELECT pub_id,GROUP_CONCAT(DISTINCT cate_id ORDER BY  cate_id ASC SEPARATOR ' ') FROM book_mast GROUP BY pub_id;

