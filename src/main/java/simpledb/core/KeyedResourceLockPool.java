package simpledb.core;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public interface KeyedResourceLockPool<T> {

    ResourceLocker getOrCreate(T key);

    void release(T key);
}
