import pandas as pd
from sqlparser import SQLParser


class PandasCursor (object):
    """takes a dictionary of pandas dataframes as an argument"""
    def __init__(self, dfs):
        self.db = dfs
        self._curr_val = None

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

            def _operation(op):
                as_name, expr = op['as_name'], op['expr']
                ev_str, identifiers = expr
                col = pd.eval(ev_str, local_dict=id_dict(identifiers))
                self._curr_val[as_name] = col

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
                    print ev_str
                    idx = pd.eval(ev_str, local_dict=id_dict(identifiers))
                    val = _get_val(*stmt)
                    col[idx] = val[idx]

                self._curr_val[as_name] = col

            def _select(identifiers):
                if identifiers is None or len(identifiers) == 0:
                    return
                # first setup any aliases
                [_alias(alias, *val) for alias, val in aliases.iteritems()]

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
                if len(joins) > 0:
                    [_join(*j) for j in joins]
                # setup literal columns specified in select statement
                for identifier, value in literals.iteritems():
                    self._curr_val[identifier] = value

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
                index = pd.eval(ev_str, local_dict=id_dict(identifiers))
                self._curr_val = self._curr_val[index]

            def _apply_functions(funs, groupby=None):
                # dictionary that provides a mechanism to override functions,
                # functions are passed from SQL statment, in lowercase. set
                # functions name(lowercase) as key and then specify the function
                # to be run in on column
                overrides = {
                    # 'isnull': (lambda x: x) just an example for now
                }
                funs = {k: [overrides.get(fn, fn) for fn in v]
                        for k, v in funs.iteritems()}

                if groupby is None:
                    # create a fake column which holds only one value, so
                    # group will aggregate entire columns into one group
                    fake_column = '####fake'
                    self._curr_val[fake_column] = 0
                    groupby = self._curr_val.groupby(fake_column)
                self._curr_val = groupby.agg(funs).reset_index()

            def _group(group_by):
                groupby = self._curr_val.groupby(group_by)
                if fns is not None:
                    _apply_functions(fns, groupby)

            def _order(identifiers):
                self._curr_val.sort(identifiers, inplace=True)

            sections = {'SELECT': _select, 'FROM': _from,
                        'WHERE': _where, 'GROUP': _group,
                        'ORDER': _order}

            # dictionary to store functions and arguments to functions
            # needed to execute in proper SQL order of operations
            _exec = {}
            temp_tables = []

            for k, v in parsed.iteritems():
                if k == 'NESTED_QUERIES':
                    for ident, query in v.iteritems():
                        _execute(query)
                        self.db[ident] = self.fetchall()
                        self.db[ident].columns = \
                            [col.split('.')[1] for col in self.db[ident].columns]
                        temp_tables.append(ident)
                elif k in sections.keys():
                    _exec[k] = sections[k], v

            fns, joins, aliases, cases, ops = \
                [parsed.get(x, [] if x == 'JOINS' else {})
                 for x in 'FUNCTIONS', 'JOINS', 'ALIASES', 'CASES', 'OPS']
            literals = parsed.get('LITERALS', {})

            # execute statement in proper SQL order. ORDER is set before SELECT
            # for our use case as we may need to sort by a column before it is
            # filtered out in SELECT statement
            for keyword in 'FROM', 'WHERE', 'GROUP', 'ORDER', 'SELECT':
                _ops = ops.get(keyword, [])
                if len(_ops) > 0:
                    # setup any operations at the correct part of evaluation
                    [_operation(op) for op in _ops]
                _cases = cases.get(keyword, [])
                if len(_cases) > 0:
                    # setup any case statements at the correct part of evaluation
                    [_case(case) for case in _cases]
                fn, args = _exec.get(keyword, (None, None))
                if fn is not None:
                    fn(args)
                elif keyword == 'GROUP' and len(fns) > 0:
                    # we need to do any aggregating at this point in the query,
                    # even if not grouping
                    _apply_functions(fns)

            into = parsed.get('INTO', None)
            if into is not None:
                self.db[into] = self._curr_val
                self._curr_val = None

            # clearout any temporary tables before next statement is executed
            for x in temp_tables:
                del self.db[x]

        parser = SQLParser()
        _execute(parser.parse_statement(statement))

    def fetchall(self):
        return self._curr_val

    def fetch_dicts(self):
        """convenience function to fetch objects as a list of dictionaries,
        good for JSON apis"""
        return self.fetchall().T.to_dict().values()
