package org.luvx.hive.dataflow.bean;

import lombok.Data;

import java.util.List;

/**
 * @ClassName: org.luvx.hive.bean
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/11 14:36
 */
@Data
public class Edge {
    private List<Integer> sources;
    private List<Integer> targets;
    private String        expression;
    private String        edgeType;
}