from sql4pandas import PandasCursor
import pandas as pd
import numpy as np
import unittest


# # unit tests
# class Tests(unittest.TestCase):

#     def testOne(self):
#         self.failUnless(IsOdd(1))

#     def testTwo(self):
#         self.failIf(IsOdd(2))

# if __name__ == '__main__':
#     unittest.main()

if __name__ == "__main__":

    tbl1 = pd.DataFrame(np.random.randn(1000, 5) * 50,
                        columns=['a', 'b', 'c', 'd', 'e'])
    tbl1['f'] = 'five'
    tbl2 = tbl1.copy()
    # tbl2 *= 0.7

    db = {'tbl1': tbl1, 'tbl2': tbl2}
    crs = PandasCursor(db)


    crs.execute("""SELECT 5, 'testing', 5 + 5 as ten, tbl1.e as e
                   INTO random_table
                   from tbl1""")
    print crs.fetchall()
    crs.execute("""SELECT * FROM random_table""")
    print crs.fetchall()
    crs.execute("""SELECT 5, 'test', 5 + 5 as ten, tbl1.e as e from tbl1""")
    print crs.fetchall()
    crs.execute("""SELECT 5 as five, 'test' as test, 5 + 5 as ten, tbl1.e as e
                   from tbl1""")
    print crs.fetchall()
    crs.execute(""" SELECT
                        CASE
                            WHEN tbl1.f = 'five'
                            THEN 'test'
                            ELSE tbl1.a
                        END as case
                    FROM tbl1
          """)
    print crs.fetchall()
    crs.execute("""SELECT SUM(tbl1.a), SUM(tbl1.b), SUM(tbl1.a) + SUM(tbl1.b)
                   FROM tbl1""")
    print crs.fetchall()
    crs.execute("""SELECT
                      tbl1.e, tbl1.b, tbl1.a,
                      ((tbl1.e + tbl1.b) / tbl1.a) * 10 as eb,
                      (tbl1.e + tbl1.b) / tbl1.a as ba
                   FROM tbl1""")
    print crs.fetchall()
    crs.execute("""SELECT tbl1.e, tbl1.b, tbl1.e + tbl1.b as eb
                   FROM tbl1""")
    print crs.fetchall()
    crs.execute("""SELECT SUM(tbl1.e) FROM tbl1""")
    # print crs.fetchall()
    crs.execute("""SELECT
                    CASE
                        WHEN tbl.a < 5
                        THEN tbl2.e
                        ELSE tbl.a
                    END as case1, tbl2.b as randomness
                    FROM (SELECT tbl1.a, tbl1.b
                          FROM tbl1
                          WHERE tbl1.a > 5) tbl
                        LEFT JOIN
                        (SELECT tbl2.a, tbl2.e, tbl2.b
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
