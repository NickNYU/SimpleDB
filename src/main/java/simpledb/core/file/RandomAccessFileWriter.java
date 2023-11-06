package simpledb.core.file;

import simpledb.storage.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public class RandomAccessFileWriter implements FileWriter {

    @Override
    public void writePage(File file, long start, Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(start); // 移动到指定位置
            raf.write(page.getPageData()); // 写入数据
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
