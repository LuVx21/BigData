package org.luvx.hive;

import org.apache.hadoop.hive.ql.tools.LineageInfo;

/**
 * @ClassName: org.luvx
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/1 11:41
 */
public class TableLevel {
    public static void main(String[] args) throws Exception {
        String sql = "insert into table t_summary select R2.id, r1.name as xingming, concat(r1.name, '.', r2.name) as xueke, r2.score as fenshu from t_student r1 left join t_score r2 on r1.id = r2.student_id where R1.id = 1";
        LineageInfo.main(new String[]{sql});
    }
}
