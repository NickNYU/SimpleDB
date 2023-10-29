package simpledb.core.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/29
 */
public class ByteArrayFileReader implements FileReader<byte[]> {
    @Override
    public byte[] getContent(File file, long start, long end) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(start); // 移动到起始位置
            long bytesRead = 0;
            int bufferSize = (int) (end - start + 1);
            byte[] buffer = new byte[bufferSize];

            int totalRead = 0;
            while (bytesRead < bufferSize) {
                int read = raf.read(buffer, totalRead, bufferSize - totalRead);
                if (read == -1) {
                    break;
                }
                totalRead += read;
                bytesRead += read;
            }

            return buffer;
        } catch (IOException e) {
            e.printStackTrace();
            return null; // 出现异常时返回null，你可以根据需要进行错误处理
        }
    }
}
