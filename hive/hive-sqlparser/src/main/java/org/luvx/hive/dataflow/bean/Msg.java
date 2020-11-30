package org.luvx.hive.dataflow.bean;

import lombok.Data;

import java.util.List;

/**
 * @author Ren, Xie
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
