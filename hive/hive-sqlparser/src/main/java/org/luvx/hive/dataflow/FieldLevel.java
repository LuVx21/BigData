package org.luvx.hive.dataflow;

import com.alibaba.fastjson.JSON;
import org.luvx.hive.bean.Edge;
import org.luvx.hive.bean.Msg;
import org.luvx.hive.bean.Vertice;

import java.util.List;

/**
 * @ClassName: org.luvx
 * @Description: 数据血缘字段级别
 * @Author: Ren, Xie
 * @Date: 2019/11/1 11:41
 */
public class FieldLevel {
    public static void main(String[] args) {
        String json = "{\"version\":\"1.0\",\"user\":\"root\",\"timestamp\":1573457923,\"duration\":184653,\"jobIds\":[\"job_1573442818662_0010\",\"job_1573442818662_0011\"],\"engine\":\"mr\",\"database\":\"boot\",\"hash\":\"9718a01d05b806d65f5757d346f884f8\",\"queryText\":\"insert into table t_summary\\nselect\\n    R2.id,\\n    r1.name as xingming,\\n    concat(r1.name, '.', r2.name) as xueke,\\n    r2.score as fenshu\\nfrom t_student r1\\nleft join t_score r2\\n    on r1.id = r2.student_id\\nwhere\\n    R1.id = 1\",\"edges\":[{\"sources\":[4],\"targets\":[0],\"edgeType\":\"PROJECTION\"},{\"sources\":[5],\"targets\":[1],\"edgeType\":\"PROJECTION\"},{\"sources\":[5,6],\"targets\":[2],\"expression\":\"concat(r1.name, '.', r2.name)\",\"edgeType\":\"PROJECTION\"},{\"sources\":[7],\"targets\":[3],\"edgeType\":\"PROJECTION\"},{\"sources\":[8],\"targets\":[0,1,2,3],\"expression\":\"(r1.id = 1)\",\"edgeType\":\"PREDICATE\"},{\"sources\":[9],\"targets\":[0,1,2,3],\"expression\":\"(r2.student_id = 1)\",\"edgeType\":\"PREDICATE\"},{\"sources\":[4],\"targets\":[0],\"expression\":\"compute_stats(boot.t_score.id, 'hll')\",\"edgeType\":\"PROJECTION\"},{\"sources\":[5],\"targets\":[1],\"expression\":\"compute_stats(boot.t_student.name, 'hll')\",\"edgeType\":\"PROJECTION\"},{\"sources\":[5,6],\"targets\":[2],\"expression\":\"compute_stats(concat(r1.name, '.', r2.name), 'hll')\",\"edgeType\":\"PROJECTION\"},{\"sources\":[7],\"targets\":[3],\"expression\":\"compute_stats(boot.t_score.score, 'hll')\",\"edgeType\":\"PROJECTION\"}],\"vertices\":[{\"id\":0,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_summary.id\"},{\"id\":1,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_summary.student_name\"},{\"id\":2,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_summary.xueke_name\"},{\"id\":3,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_summary.score\"},{\"id\":4,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_score.id\"},{\"id\":5,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_student.name\"},{\"id\":6,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_score.name\"},{\"id\":7,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_score.score\"},{\"id\":8,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_student.id\"},{\"id\":9,\"vertexType\":\"COLUMN\",\"vertexId\":\"boot.t_score.student_id\"}]}";

        Msg msg = JSON.parseObject(json, Msg.class);

        List<Edge> edges = msg.getEdges();
        List<Vertice> vertices = msg.getVertices();
        for (Edge edge : edges) {
            if (edge.getEdgeType().equals("PREDICATE")) {
                continue;
            }

            if (
                    edge.getExpression() != null
                            && edge.getExpression().startsWith("compute_stats")
            ) {
                continue;
            }

            List<Integer> sources = edge.getSources();
            List<Integer> targets = edge.getTargets();
            for (Integer source : sources) {
                for (Integer target : targets) {
                    print(vertices.get(source), vertices.get(target));
                }
            }
        }
    }

    private static void print(Vertice from, Vertice to) {
        System.out.println(to.getVertexId() + " <- " + from.getVertexId());
    }
}
