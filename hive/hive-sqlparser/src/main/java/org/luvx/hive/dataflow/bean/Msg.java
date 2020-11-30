package org.luvx.hive.dataflow.bean;

import lombok.Data;

import java.util.List;

/**
 * @ClassName: org.luvx.hive
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/11 14:35
 */
@Data
public class Msg {
    private String        version;
    private String        user;
    private Long          timestamp;
    private Long          duration;
    private List<String>  jobIds;
    private String        engine;
    private String        database;
    private String        hash;
    private String        queryText;
    private List<Edge>    edges;
    private List<Vertice> vertices;
}
