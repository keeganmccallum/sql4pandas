from pdsql import PDSQL
import pandas as pd
import numpy as np
import pdb

if __name__ == "__main__":

    tbl1 = pd.DataFrame(np.random.randn(1000, 5) * 50,
                        columns=['a', 'b', 'c', 'd', 'e'])
    tbl2 = tbl1.copy()
    # tbl2 *= 0.7

    panel = {'tbl1': tbl1, 'tbl2': tbl2}
    crs = PDSQL(panel)

    crs.execute("""SELECT
                    CASE
                        WHEN tbl.a < 5
                        THEN tbl2.e
                        ELSE tbl.a
                    END as case1, tbl2.b as randomness
                    FROM (SELECT tbl1.a, tbl1.b
                          FROM tbl1
                          WHERE tbl1.a > 5) tbl
                        LEFT JOIN (SELECT tbl2.a, tbl2.e, tbl2.b
                            FROM tbl2
                            WHERE tbl2.a < 5) tbl2
                            ON tbl.b = tbl2.b""")
    print crs.fetchall()

    crs.execute("""SELECT tbl2.a, tbl1.b
                   FROM tbl2
                       LEFT JOIN tbl1
                           ON tbl2.e = tbl1.e
                   WHERE tbl1.a > 0 AND  tbl2.b < 0
                   """)
    print crs.fetchall()
    crs.execute("""SELECT SUM(tbl1.e)
               FROM tbl2
                   LEFT JOIN tbl1
                       ON tbl2.e = tbl1.e
               WHERE tbl1.a > 0 AND  tbl2.b < 0
               GROUP BY tbl1.a, tbl2.b
               """)
    print crs.fetchall()

    def test():
        crs.execute("""SELECT e FROM tbl1""")
        print crs.fetchall()
        crs.execute("""SELECT tbl2.e as test
                       FROM tbl1
                            INNER JOIN tbl2
                                ON tbl2.a = tbl1.a
                        WHERE tbl2.a > 7""", 'test', 7)
        print crs.fetchall()
        crs.execute("""SELECT
                    CASE
                        WHEN SUM(tbl1.e) > 0
                        THEN SUM(tbl1.e)
                        ELSE SUM(tbl2.a)
                    END AS rand,
                    MIN(tbl1.b) as min,
                    CASE
                        WHEN MIN(tbl1.c) < 0
                        THEN MIN(tbl1.c)
                        WHEN MAX(tbl2.b) > 0
                        THEN MAX(tbl1.e)
                        ELSE SUM(tbl1.b)
                    END as crazy
                   FROM tbl2
                       LEFT JOIN tbl1
                           ON tbl2.e = tbl1.e
                   WHERE tbl1.a > 0 AND tbl2.b < 0
                   GROUP BY tbl1.a, tbl2.b
                   ORDER BY SUM(tbl1.d)""")
        print crs.fetchall()
        crs.execute("""SELECT
                    CASE
                        WHEN tbl1.e > 0
                        THEN tbl1.e
                        ELSE tbl2.a
                    END, tbl1.b
                   FROM tbl2
                       LEFT JOIN tbl1
                           ON tbl2.e = tbl1.e
                   WHERE tbl1.a > 0 AND  tbl2.b < 0
                   """)
        return crs.fetchall()

    testing = False
    if testing:
        tests = 100
        from timeit import Timer
        t = Timer(test)
        print '***me'
        print t.timeit(number=tests)
        print 'result'
        print test()
    else:
        print test()
    pdb.set_trace()
