package com.github.graph;

import com.github.common.constant.LineageLabel;
import lombok.Getter;
import lombok.Setter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.io.IOException;

/**
 * @author: Ren, Xie
 * @desc:
 */
public class IndexQueryLineage {
    @Getter
    @Setter
    GraphDatabaseService graphDb;

    private static final String DB_PATH = "target/lineage-db";

    public Node indexNode(String tabName) throws IOException {
        // graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("lineage-db")) ;
        /*
        Iterator<Node> ir = graphDb.findNodes(DynamicLabel.label("xxx"));
        while (ir.hasNext()) {
            System.out.println(ir.next().getProperty("name"));
        }
        */

        Node node = graphDb.findNode(LineageLabel.node, "name", tabName);
        return node;
    }
}