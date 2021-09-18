package org.otaku.jobmanager;

import java.util.ArrayList;
import java.util.List;

/**
 * 注，这里的方法约定返回null代表集合empty，不会返回empty集合
 */
public class ChildrenCache {
    private List<String> children;

    public ChildrenCache() {
        children = null;
    }

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    //更新cache，返回新增的条目
    public List<String> addAndSet(List<String> newChildren) {
        if (children == null) {
            children = newChildren;
            return new ArrayList<>(newChildren);
        }
        List<String> diff = new ArrayList<>();
        for (var child : newChildren) {
            if (!children.contains(child)) {
                diff.add(child);
            }
        }
        children = newChildren;
        return diff.isEmpty() ? null : diff;
    }

    //更新cache，返回被删除的条目
    public List<String> removeAndSet(List<String> newChildren) {
        if (children == null) {
            children = newChildren;
            return null;
        }
        List<String> diff = new ArrayList<>();
        for (var child : children) {
            if (!newChildren.contains(child)) {
                diff.add(child);
            }
        }
        return diff.isEmpty() ? null : diff;
    }

}
