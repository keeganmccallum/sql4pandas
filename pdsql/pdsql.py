import numexpr as ne
from pdparser import PDParser


class PDSQL(object):
    """takes a dictionary of pandas dataframes as an argument"""
    def __init__(self, dfs):
        self.db = dfs
        self._curr_val = None
        self._groupby = None
        self.cases = {}
        self.fns = {}
        self.joins = []
        self.aliases = {}
        self.temp_tables = []

    def execute(self, statement, *args, **kwargs):

        def _execute(parsed):

            def id_dict(identifiers):
                """translate dictionary of string identifiers used into
                usable dictionary to pass to numexpr"""
                _dict = {}
                for idx, (col, fn) in identifiers.iteritems():
                    if fn is None:
                        _dict[idx] = self._curr_val[col]
                    else:
                        _dict[idx] = self._curr_val[col][fn]
                return _dict

            def _single_identifier(identifier):
                """need to check if there is only one table with this
                column name, if so append that table, if not
                raise ambigous column name error"""
                matches = \
                    filter(lambda x: identifier == x.split('.')[1],
                           self._curr_val.columns)
                if len(matches) == 1:
                    return matches[0]
                else:
                    raise ValueError('Ambigous or non-exsistant column name: %s'
                                     % identifier)

            def _alias(alias, col, fn):
                if col is None:
                    self._curr_val[alias] = \
                        self._curr_val[_single_identifier(alias)]
                elif fn is None:
                    self._curr_val[alias] = self._curr_val[col]
                else:
                    self._curr_val[alias] = self._curr_val[col][fn]

            def _get_val(col, fn):
                if fn is not None:
                    return self._curr_val[col][fn]
                else:
                    return self._curr_val[col]

            def _case(case):
                as_name, else_stmt, stmts = \
                    case['as_name'], case.get('else_stmt', None), case['stmts']

                # make a copy of a column in the data frame and use it as a base
                col = self._curr_val.iloc[:, 0].copy()
                if else_stmt is not None:
                    else_val = _get_val(*else_stmt)
                    col.loc[:] = else_val
                else:
                    # default to NULL as no else val specified
                    col.loc[:] = None

                for (ev_str, identifiers), stmt in stmts:
                    idx = ne.evaluate(ev_str, id_dict(identifiers))
                    val = _get_val(*stmt)
                    col[idx] = val[idx]

                self._curr_val[as_name] = col

            def _select(identifiers):
                if identifiers is None:
                    return
                # first setup any aliases
                [_alias(alias, *val) for alias, val in self.aliases.iteritems()]
                ids = []
                for col, fn in identifiers:
                    if fn is None:
                        idx = col
                    else:
                        idx = col+'_'+fn
                        self._curr_val[idx] = self._curr_val[col][fn]
                    ids.append(idx)
                self._curr_val = self._curr_val[ids]

            def _from(tbl):
                table, identifier = tbl
                self._curr_val = self.db[table].copy()
                # add identifier for table to column names
                self._curr_val.columns = \
                    [identifier + '.' + col for col in self._curr_val.columns]
                if len(self.joins) > 0:
                    [_join(*j) for j in self.joins]

            def _join(right, how, left_on, right_on, right_identifier):
                right = self.db[right].copy()
                # need to make interchangable
                if left_on not in self._curr_val.columns:
                    right_on, left_on = left_on, right_on
                right.columns = \
                    [right_identifier + '.' + col for col in right.columns]
                self._curr_val = \
                    self._curr_val.merge(right, how=how, left_on=left_on,
                                         right_on=right_on)

            def _where(cond):
                ev_str, identifiers = cond
                index = ne.evaluate(ev_str, id_dict(identifiers))
                self._curr_val = self._curr_val[index]

            def _apply_functions(fns):
                # TODO: to implement error handling to notify user when
                # selected attribute not in aggregate function but groupby
                # applied
                if self._groupby is not None:
                    self._curr_val = self._groupby.agg(fns).reset_index()

            def _group(group_by):
                self._groupby = self._curr_val.groupby(group_by)
                if self.fns is not None:
                    _apply_functions(self.fns)

            def _order(identifiers):
                self._curr_val.sort(identifiers, inplace=True)

            sections = {'SELECT': _select, 'FROM': _from,
                        'WHERE': _where, 'GROUP': _group,
                        'ORDER': _order}

            # dictionary to store functions and arguments to functions
            # needed to execute in proper SQL order of operations
            _exec = {}

            for k, v in parsed.iteritems():
                if k == 'NESTED_QUERIES':
                    for ident, query in v.iteritems():
                        _execute(query)
                        self.db[ident] = self.fetchall()
                        self.db[ident].columns = \
                            [col.split('.')[1] for col in self.db[ident].columns]
                        self.temp_tables.append(ident)
                elif k in sections.keys():
                    _exec[k] = sections[k], v

            self.fns, self.joins, self.aliases, self.cases = \
                [parsed.get(x, [] if x == 'JOINS' else {})
                 for x in 'FUNCTIONS', 'JOINS', 'ALIASES', 'CASES']

            # execute statement in proper SQL order. ORDER is set before SELECT
            # for our use case as we may need to sort by a column before it is
            # filtered out in SELECT statement
            for keyword in 'FROM', 'WHERE', 'GROUP', 'ORDER', 'SELECT':
                cases = self.cases.get(keyword, [])
                if len(cases) > 0:
                    # setup any case statements at the correct part of evaluation
                    [_case(case) for case in cases]
                fn, args = _exec.get(keyword, (None, None))
                if fn is not None:
                    fn(args)

        parser = PDParser()
        _execute(parser.parse_statement(statement))

        # clearout any temporary tables before next statement is executed
        for x in self.temp_tables:
            del self.db[x]
        self.temp_tables = []

    def fetchall(self):
        return self._curr_val
