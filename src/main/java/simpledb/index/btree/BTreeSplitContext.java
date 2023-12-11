package simpledb.index.btree;

import simpledb.index.BTreeFile;
import simpledb.index.BTreeLeafPage;
import simpledb.storage.Field;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Map;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/12/11
 */
public class BTreeSplitContext {

    private final BTreeLeafPage source;

    private final BTreeLeafPage dest;

    private final Map<PageId, Page> dirtyPages;

    private final Field field;

    private final BTreeFile btreeFile;

    public BTreeSplitContext(BTreeLeafPage source, BTreeLeafPage dest, Map<PageId, Page> dirtyPages,
                             Field field, BTreeFile btreeFile) {
        this.source = source;
        this.dest = dest;
        this.dirtyPages = dirtyPages;
        this.field = field;
        this.btreeFile = btreeFile;
    }

    public BTreeLeafPage getSource() {
        return source;
    }

    public BTreeLeafPage getDest() {
        return dest;
    }

    public Map<PageId, Page> getDirtyPages() {
        return dirtyPages;
    }

    public Field getField() {
        return field;
    }

    public BTreeFile getBtreeFile() {
        return btreeFile;
    }
}
