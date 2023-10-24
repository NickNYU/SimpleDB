package simpledb.storage.page;

import simpledb.storage.Page;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public class DefaultPageManager implements PageManager {

    private final int pageNum;

    private final Page[] pages;

    public DefaultPageManager(int pageNum) {
        this.pageNum = pageNum;
        this.pages = new Page[pageNum];
    }

    @Override
    public Page getPage() {
        return null;
    }
}
