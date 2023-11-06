package simpledb.core.file;

import simpledb.storage.Page;

import java.io.File;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public interface FileWriter {

    void writePage(File file, long start, Page page);
}
