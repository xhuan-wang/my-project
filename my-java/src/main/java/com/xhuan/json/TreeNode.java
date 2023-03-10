package com.xhuan.json;

import lombok.Data;

import java.util.List;

/**
 * @Classname TreeNode
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/6 14:53
 * @Created by xiaohuan
 */
@Data
public class TreeNode {

    /** 节点名称 */
    private String name;

    /** 父节点名称*/
    private String pName;

    /** 子节点 */
    private List<TreeNode> children;

    public TreeNode(String name, String pName) {
        this.name = name;
        this.pName = pName;
    }
}
