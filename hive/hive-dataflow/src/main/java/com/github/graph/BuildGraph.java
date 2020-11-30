package com.github.graph;


import com.github.common.constant.LineageLabel;
import com.github.common.constant.RelTypes;
import com.github.common.constant.TreeGlass;
import com.github.common.pojo.LNode;
import com.github.common.pojo.LineageNode;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @author Ren, Xie
 */
@Slf4j
public class BuildGraph {
    private static final String               DB_PATH = "lineage-db";
    private              GraphDatabaseService graphDb;
    private              Index<Node>          index;
    private              IndexQueryLineage    query   = null;

    public BuildGraph() {
        query = new IndexQueryLineage();
    }

    public void addNode(LineageNode lineNode) {
        if (lineNode.getParent() == null) {
            return;
        }
        Transaction tx = graphDb.beginTx();
        try {
            index = graphDb.index().forNodes("nodes");
            Node parentNode = query.indexNode(lineNode.getParent().getName());
            if (parentNode == null) {
                parentNode = graphDb.createNode(LineageLabel.node);
            }

            String bbb = lineNode.getParent().getType();
            parentNode.setProperty("name", lineNode.getParent().getName());
            index.add(parentNode, "name", lineNode.getParent().getName());
            parentNode.setProperty("type", lineNode.getParent().getType());
            parentNode.setProperty("filePath", lineNode.getFilePath());

            // int no = ai.
            for (LNode lnode : lineNode.getTabs()) {
                Node node = query.indexNode(lnode.getName());
                if (node == null) {
                    node = graphDb.createNode(LineageLabel.node);
                }

                String aaa = lnode.getType();
                System.out.println("parent:" + bbb + "\tchild:" + aaa);
                node.setProperty("name", lnode.getName());
                index.add(node, "name", lnode.getName());
                node.setProperty("type", lnode.getType());
                node.setProperty("filePath", lnode.getFilePath());
                RelTypes type;
                if (
                        Objects.equals(lnode.getType(), TreeGlass.TOK_LIKETABLE) ||
                                (
                                        Objects.equals(lineNode.getParent().getType(), TreeGlass.TOK_INSERT_INTO)
                                                && Objects.equals(lnode.getType(), ("test"))
                                )
                ) {
                    type = RelTypes.LIKETABLE;
                } else if (lnode.getType().equals(TreeGlass.TOK_UNIONALL)) {
                    type = RelTypes.UNION;
                } else if (lnode.getType().equals(TreeGlass.TOK_LEFTOUTERJOIN)) {
                    type = RelTypes.LJOIN;
                } else if (lnode.getType().equals(TreeGlass.TOK_RIGHTOUTERJOIN)) {
                    type = RelTypes.RJOIN;
                } else {
                    type = RelTypes.ERR;
                }
                Relationship rship = parentNode.createRelationshipTo(node, type);
                log.info("add name:{}", lnode.getName());
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
            tx.failure();
        } finally {
            tx.finish();
        }
    }

    public void openDb() throws IOException {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DB_PATH));
        query.setGraphDb(graphDb);
        registerShutdownHook(graphDb);
    }

    public void initDb() throws IOException {
        FileUtils.deleteRecursively(new File(DB_PATH));
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DB_PATH));
        query.setGraphDb(graphDb);
        registerShutdownHook(graphDb);
    }

    public void removeData() {
        try (Transaction tx = graphDb.beginTx()) {
            // START SNIPPET: removingData
            // let's remove the data
//            firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
//            firstNode.delete();
//            secondNode.delete();
            // END SNIPPET: removingData

            tx.success();
        }
    }

    public void shutDown() {
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
                System.out.println("executing VM hook!");
            }
        });
    }
}