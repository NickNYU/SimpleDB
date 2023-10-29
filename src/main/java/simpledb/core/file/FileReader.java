package simpledb.core.file;

import java.io.File;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/29
 */
public interface FileReader<T> {

    T getContent(File file, long start, long end);
}
