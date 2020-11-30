package com.github.common.constant;

/**
 * @author: Ren, Xie
 * @desc:
 */

import org.neo4j.graphdb.RelationshipType;

public enum RelTypes implements RelationshipType {
    UNION, LJOIN, RJOIN, LIKETABLE, ERR
}