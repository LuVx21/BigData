package org.luvx.hive.dataflow.bean;

import lombok.Data;

import java.util.List;

/**
 * @author Ren, Xie
 */
@Data
public class Edge {
    private List<Integer> sources;
    private List<Integer> targets;
    private String        expression;
    private String        edgeType;
}