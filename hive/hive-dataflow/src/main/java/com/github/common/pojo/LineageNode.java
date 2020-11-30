package com.github.common.pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: Ren, Xie
 * @desc:
 */
@Data
public class LineageNode {
    private LNode       parent;
    private List<LNode> tabs = new ArrayList<>();
    private String      filePath;

    public void addChild(LNode child) {
        tabs.add(child);
    }
}