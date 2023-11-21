package simpledb.storage.lock;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/19
 */
public class DefaultLockContext implements Locker.LockContext {

    private final TransactionId transactionId;

    private final PageId pageId;

    private final Permissions permissions;

    public DefaultLockContext(TransactionId transactionId, PageId pageId, Permissions permissions) {
        this.transactionId = transactionId;
        this.pageId = pageId;
        this.permissions = permissions;
    }

    @Override
    public TransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public PageId getPageId() {
        return pageId;
    }

    @Override
    public Permissions getPermission() {
        return permissions;
    }

    @Override
    public String toString() {
        return "DefaultLockContext{" +
                "transactionId=" + transactionId +
                ", pageId=" + pageId +
                ", permissions=" + permissions +
                '}';
    }

    public static final Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private TransactionId transactionId;

        private PageId pageId;

        private Permissions permissions;

        public TransactionId getTransactionId() {
            return transactionId;
        }

        public Builder transactionId(TransactionId transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public PageId getPageId() {
            return pageId;
        }

        public Builder pageId(PageId pageId) {
            this.pageId = pageId;
            return this;
        }

        public Permissions getPermissions() {
            return permissions;
        }

        public Builder permissions(Permissions permissions) {
            this.permissions = permissions;
            return this;
        }

        public DefaultLockContext build() {
            return new DefaultLockContext(transactionId, pageId, permissions);
        }
    }
}
