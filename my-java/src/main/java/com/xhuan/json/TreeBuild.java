package com.xhuan.json;

import cn.hutool.json.JSONUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname TreeBuild
 * @Description TODO
 * @Version 1.0.0
 * @Date 2023/3/6 14:56
 * @Created by xiaohuan
 */
public class TreeBuild {

    // 保存参与构建树形的所有数据（通常数据库查询结果）
    public List<TreeNode> nodeList;

    /**
     *  构造方法
     *  @param nodeList 将数据集合赋值给nodeList，即所有数据作为所有节点。
     */
    public TreeBuild(List<TreeNode> nodeList){
        this.nodeList = nodeList;
    }

    /**
     *   获取需构建的所有根节点（顶级节点） "0"
     *   @return 所有根节点List集合
     */
    public List<TreeNode> getRootNode(){
        // 保存所有根节点（所有根节点的数据）
        List<TreeNode> rootNodeList = new ArrayList<>();
        // treeNode：查询出的每一条数据（节点）
        for (TreeNode treeNode : nodeList){
            // 判断当前节点是否为根节点，此处注意：若parentId类型是String，则要采用equals()方法判断。
            if (null == treeNode.getPName()) {
                // 是，添加
                rootNodeList.add(treeNode);
            }
        }
        return rootNodeList;
    }

    /**
     *  根据每一个顶级节点（根节点）进行构建树形结构
     *  @return  构建整棵树
     */
    public List<TreeNode> buildTree(){
        // treeNodes：保存一个顶级节点所构建出来的完整树形
        List<TreeNode> treeNodes = new ArrayList<TreeNode>();
        // getRootNode()：获取所有的根节点
        for (TreeNode treeRootNode : getRootNode()) {
            // 将顶级节点进行构建子树
            treeRootNode = buildChildTree(treeRootNode);
            // 完成一个顶级节点所构建的树形，增加进来
            treeNodes.add(treeRootNode);
        }
        return treeNodes;
    }

    /**
     *  递归-----构建子树形结构
     *  @param  pNode 根节点（顶级节点）
     *  @return 整棵树
     */
    public TreeNode buildChildTree(TreeNode pNode){
        List<TreeNode> childTree = new ArrayList<TreeNode>();
        // nodeList：所有节点集合（所有数据）
        for (TreeNode treeNode : nodeList) {
            // 判断当前节点的父节点ID是否等于根节点的ID，即当前节点为其下的子节点
            if (treeNode.getPName() != null && treeNode.getPName().equals(pNode.getName())) {
                // 再递归进行判断当前节点的情况，调用自身方法
                childTree.add(buildChildTree(treeNode));
            }
        }
        // for循环结束，即节点下没有任何节点，树形构建结束，设置树结果
        pNode.setChildren(childTree);
        return pNode;
    }

    public static boolean hasParentNode(List<TreeNode> nodeList, String name) {
        boolean isParentNode = false;
        for (TreeNode tree : nodeList) {
            if (tree.getName().equals(name)) {
                isParentNode = true;
                break;
            }
        }
        return isParentNode;
    }

    public static boolean hasChildNode(List<TreeNode> nodeList, String name) {
        boolean isChildNode = false;
        for (TreeNode tree : nodeList) {
            if (tree.getName().equals(name)) {
                isChildNode = true;
                break;
            }
        }
        return isChildNode;
    }

    public static void main(String[] args) {
        // 模拟测试数据（通常为数据库的查询结果）
//        List<TreeNode> treeNodeList = new ArrayList<>();
//        treeNodeList.add(new TreeNode(1, 0, "顶级节点A"));
//        treeNodeList.add(new TreeNode(2, 0, "顶级节点B"));
//        treeNodeList.add(new TreeNode(3, 1, "父节点是A"));
//        treeNodeList.add(new TreeNode(4, 2, "父节点是B"));
//        treeNodeList.add(new TreeNode(5, 2, "父节点是B"));
//        treeNodeList.add(new TreeNode(6, 3, "父节点的ID是3"));

//        // 创建树形结构（数据集合作为参数）
//        TreeBuild treeBuild = new TreeBuild(treeNodeList);
//        // 原查询结果转换树形结构
//        treeNodeList = treeBuild.buildTree();
//        System.out.println(JSONUtil.toJsonStr(treeNodeList));

        List<String> columnList = new ArrayList<>();
        columnList.add("code");
        columnList.add("msg");
        columnList.add("data.records.id");
        columnList.add("data.records.nickname");
        columnList.add("data.records.username");
        columnList.add("data.records.birthday");
        columnList.add("data.total");
        columnList.add("data.pages");
        columnList.add("data.current");
        columnList.add("data.size");

        List<TreeNode> treeNodeList = new ArrayList<>();
        for (int i = 0; i < columnList.size(); i++) {
            String column = columnList.get(i);
            String[] arr = column.split("\\.");

            for (int j = 0; j < arr.length; j++) { // 遍历数组
                if (treeNodeList.size() == 0) { //如果treeNodeList为0，则初始化
                    treeNodeList.add(new TreeNode(arr[j], null));
                } else { // 如果不为0，则取里面的节点进行判断
                    if (j <= 0) { // 如果是第一个，则一定是最外层节点

                        if (!hasParentNode(treeNodeList, arr[j])) {
                            TreeNode treeNode = new TreeNode(arr[j], null);
                            treeNodeList.add(treeNode);
                        }
                    } else { // 如果不是第一个，则找到其父节点，并进行处理
                        boolean isParentNode = false;
                        int index = 0;
                        for (int k = 0; k < treeNodeList.size(); k++) { //遍历treeNodeList，找到其父节点
                            if (treeNodeList.get(k).getName().equals(arr[j - 1])) {
                                isParentNode = true;
                                index = k;
                                break;
                            }
                        }
                        if (isParentNode) { //如果存在父节点
                            if (treeNodeList.get(index).getChildren() != null) { //如果其父节点的子节点List不为null
                                List<TreeNode> childrenNodeList = treeNodeList.get(index).getChildren();
                                if (!hasChildNode(childrenNodeList, arr[j])) { //如果其父节点的子节点List中存在，则加入到父节点的子节点List
                                    TreeNode treeNode = new TreeNode(arr[j], treeNodeList.get(index).getName());
                                    childrenNodeList.add(treeNode);
                                    treeNodeList.add(treeNode);
                                }
                            } else { //如果其父节点的子节点List为null
                                List<TreeNode> tmpList = new ArrayList<>();
                                TreeNode treeNode = new TreeNode(arr[j], treeNodeList.get(index).getName());
                                tmpList.add(treeNode);
                                treeNodeList.get(index).setChildren(tmpList);
                                treeNodeList.add(treeNode);
                            }
                        } else { // 如果没有其父节点
                            treeNodeList.add(new TreeNode(arr[j], arr[j - 1]));
                        }
                    }
                }
            }
        }

        // 创建树形结构（数据集合作为参数）
        TreeBuild treeBuild = new TreeBuild(treeNodeList);
        // 原查询结果转换树形结构
        treeNodeList = treeBuild.buildTree();
        System.out.println(JSONUtil.toJsonStr(treeNodeList));
        System.out.println(treeNodeList.size());
        for (TreeNode treeNode :
                treeNodeList) {
            System.out.println(treeNode);
        }
    }
}
