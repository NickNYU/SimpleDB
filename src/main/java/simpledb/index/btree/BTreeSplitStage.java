package simpledb.index.btree;

import simpledb.common.DbException;
import simpledb.storage.Tuple;

import java.util.Iterator;
import java.util.Stack;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/12/11
 */
public enum BTreeSplitStage implements Iterator<BTreeSplitStage> {

    TRANSFER_HALF_DATA {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public BTreeSplitStage next() {
            return UPDATE_OLD_PAGE_RIGHT_SIBLING;
        }

        @Override
        public void action(BTreeSplitContext context) throws DbException {
            Iterator<Tuple> tupleIterator = context.getSource().reverseIterator();
            Stack<Tuple> stack = new Stack<>();
            int moving = context.getSource().getNumTuples() / 2;
            while (tupleIterator.hasNext() && moving > 0) {
                stack.push(tupleIterator.next());
                moving --;
            }
            while (!stack.isEmpty()) {
                Tuple tuple = stack.pop();
                context.getDest().insertTuple(tuple);
                context.getSource().deleteTuple(tuple);
            }
        }
    },
    UPDATE_OLD_PAGE_RIGHT_SIBLING {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public BTreeSplitStage next() {
            return UPDATE_PAGE_POINTER;
        }

        @Override
        public void action(BTreeSplitContext context) {
            if (context.getSource().getRightSiblingId() == null) {
                return;
            }

        }
    },
    UPDATE_PAGE_POINTER {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public BTreeSplitStage next() {
            return UPDATE_PARENT_PAGE_POINTER;
        }

        @Override
        public void action(BTreeSplitContext context) {

        }
    },
    UPDATE_PARENT_PAGE_POINTER {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public BTreeSplitStage next() {
            return RETURN_PAGE_FOR_TUPLE;
        }

        @Override
        public void action(BTreeSplitContext context) {

        }
    },
    RETURN_PAGE_FOR_TUPLE {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public BTreeSplitStage next() {
            return null;
        }

        @Override
        public void action(BTreeSplitContext context) {

        }
    };

    public abstract void action(BTreeSplitContext context) throws DbException;
}
