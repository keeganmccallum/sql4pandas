from sqlparse import parse, tokens


class SQLParser(object):
    """class used to parse sql statements into a data structure
    which can be used to execute the statement"""

    def parse_statement(self, statement):
        """converts a statement in string form to tokens and passes off to
        the parser"""

        def parse_tkns(tkns):
            """parse tokens into datastructure used to execute statement"""
            fns = {}
            joins = []
            aliases = {}
            cases = {}
            ops = {}
            self.case_num = 0
            nested_queries = {}
            literals = {}

            # some helpers for determining a token's attributes when it isn't
            # completely straight forward
            is_identifier = lambda token: token._get_repr_name() == 'Identifier'
            is_case = lambda token: token._get_repr_name() == 'Case'
            is_function = lambda token: token._get_repr_name() == 'Function'
            is_comparison = lambda token: token._get_repr_name() == 'Comparison'
            is_operator = lambda token: token.ttype == tokens.Operator \
                or token.ttype == tokens.Wildcard

            def strip_tkns(tkns, punctuation=None):
                """convenience function to remove whitespace tokens and comments
                from list, optionally also remove punctuation"""
                if punctuation is None:
                    return [token for token in tkns if not token.is_whitespace()
                            and token._get_repr_name() != 'Comment']
                return [token for token in tkns if not token.is_whitespace()
                        and token._get_repr_name() != 'Comment'
                        and token.ttype != tokens.Token.Punctuation]

            def get_fns(tkns):
                """get a dictionary of all functions in statement, needed for
                order of operations with grouping and case statements"""
                for tkn in tkns:
                    if tkn._get_repr_name() == 'Function':
                        col, fn = sql_function(tkn)
                        _fns = fns.get(col, [])
                        if fn not in _fns:
                            _fns.append(fn)
                        fns[col] = _fns
                    elif tkn.is_group():
                        get_fns(tkn.tokens)

            def col_identifier(token):
                if token.ttype in tokens.Literal:
                    literals[token.value] = token.value
                    return token.value, None
                tkns = token.tokens

                # strip whitespace and punctuation
                tkns = strip_tkns(tkns)
                if len(tkns) == 1:
                    identifier = tkns[0].value
                    if tkns[0].ttype in tokens.Literal:
                        literals[identifier] = identifier
                    # handle issue of ambigous column names through aliasing
                    # for now, may be able to find a more efficient way in future
                    aliases[identifier] = None, None
                    return identifier, None

                # find the index of 'AS' in tkns
                as_idx = next((i for i, t in enumerate(tkns)
                               if t.value.upper() == 'AS'), None)
                if as_idx is None:
                    op_idx = next((i for i, t in enumerate(tkns)
                                  if is_operator(t)), None)
                    if op_idx is None:
                        return tkns[0].value + '.' + tkns[-1].value, None
                    return operation(tkns)

                as_name = tkns[as_idx+1].value
                tkns = tkns[:as_idx]
                if len(tkns) == 1:
                    # handle aliasing
                    if tkns[0].ttype in tokens.Literal:
                        literals[as_name] = tkns[0].value
                        return as_name, None
                    elif is_case(tkns[0]):
                        return parse_case(tkns[0].tokens, as_name=as_name)
                    elif is_identifier(tkns[0]):
                        aliases[as_name] = col_identifier(tkns[0]), None
                    elif is_function(tkns[0]):
                        col, fn = sql_function(tkns[0])
                        aliases[as_name] = col, fn
                    return as_name, None

                op_idx = next((i for i, t in enumerate(tkns)
                               if is_operator(t)), None)
                if op_idx is None:
                    # handle aliasing for special case where parser doesn't group
                    # identifier properly
                    aliases[as_name] = tkns[0].value + "." + tkns[-1].value, None
                    return as_name, None
                return operation(tkns, as_name)

            def sql_function(token):
                tkns = token.tokens
                fn, parens = tkns
                col = parens.tokens[1]
                fn = fn.value.lower()
                col = col_identifier(col)[0]
                return col, fn

            def identifier_list(token):
                """used to parse sql identifiers into actual
                table/column groupings"""
                if is_identifier(token):
                    return col_identifier(token)

                if is_function(token):
                    return [sql_function(token)]

                if is_case(token):
                    return parse_case(token)

                tkns = token.tokens
                if len(tkns) == 1:
                    if is_function(tkns[0]):
                        return sql_function(tkns[0])
                    return col_identifier(token)
                proc = []
                # filter whitespace and punctuation
                for tkn in tkns:
                    if is_identifier(tkn):
                        proc.append(col_identifier(tkn))
                    elif is_case(tkn):
                        proc.append(parse_case(tkn.tokens))
                    elif is_function(tkn):
                        col, fn = sql_function(tkn)
                        proc.append((col, fn))
                    elif not tkn.is_whitespace() \
                            and tkn.ttype != tokens.Punctuation:
                        proc.append(col_identifier(tkn))

                return proc

            def operation(tkns, as_name=None):
                """perform arithmetic operations"""
                # identifiers used in comparision, needed to work around issue
                # #83 of numexpr
                identifiers = {}

                if len(tkns) == 1:
                    return col_identifier(tkns[0])
                # get indicies in tkns where operators are
                op_indices = [i for i, t in enumerate(tkns)
                              if is_operator(t)]
                # get operators
                operators = [t.value for t in tkns
                             if is_operator(t)]
                # group other tokens around operators
                ids = [tkns[:op_indices[0]]]
                ids += [tkns[i1+1:i2] for i1, i2
                        in zip(op_indices[:-1], op_indices[1:])]
                ids += [tkns[op_indices[-1]+1:]]

                def get_id(_id):
                    if len(_id) > 1:
                        return ''.join([t.value for t in _id]), None
                    token = _id[0]
                    if token._get_repr_name() == 'Parenthesis':
                        # TODO: instead of just leveraging parsing,
                        # pass parenthesis into numexpr for performance
                        # gains
                        return operation(token.tokens[1:-1])
                    if token._get_repr_name() == 'Integer':
                        return token.value, None
                    if is_function(token):
                        return sql_function(token)
                    elif token.is_group():
                        return col_identifier(token)

                ids = map(get_id, ids)
                cols = [(x if y is None else (x+'_'+y)).replace('.', '_')
                        for x, y in ids]
                for _id, col in zip(ids, cols):
                    try:
                        # only add non-numbers to column identifiers dict
                        float(col)
                    except:
                        identifiers[col] = _id
                expr = reduce(lambda x, (y, z): x+' '+y+' '+z,
                              zip(operators, cols[1:]), cols[0])

                # give auto-genereted name if no alias specified
                if as_name is None:
                    as_name = ''.join(cols)
                op = {'as_name': as_name,
                      'expr': (expr, identifiers)}

                _ops = ops.get(curr_sect, [])
                _ops.append(op)
                ops[curr_sect] = _ops

                return as_name, None

            def comparison(comps, operators=None):
                # identifiers used in comparision, needed to work around issue #83
                identifiers = {}

                # need a counter for number of comparisons for variable names
                def comp_str(comp):
                    comp_map = {
                        '=': '==',
                        '<>': '!=',
                    }
                    comp = strip_tkns(comp)
                    assert len(comp) == 3
                    col, comp, val = comp
                    comp = comp_map.get(comp.value, comp.value)
                    if is_function(col):
                        col, fn = sql_function(col)
                        col_str = (col+'_'+fn).replace('.', '_')
                        identifiers[col_str] = col, fn
                    elif col.is_group():
                        col = col_identifier(col)[0]
                        col_str = col.replace('.', '_')
                        identifiers[col_str] = col, None
                    if val.is_group():
                        val = col_identifier(val)[0]
                        identifiers[val.replace('.', '_')] = val, None
                    else:
                        val = val.value
                    val_str = val.replace('.', '_')
                    return """({col} {comp} {val})""".format(col=col_str,
                                                             comp=comp,
                                                             val=val_str)

                comp = comps[0]
                ev_str = comp_str(comp)
                if operators is not None:
                    for comp, op in zip(comps[1:], operators):
                        # build string to eventually evaluate
                        if op == 'AND':
                            ev_str += " & " + comp_str(comp)
                        elif op == 'OR':
                            ev_str += " | " + comp_str(comp)

                return ev_str, identifiers

            def parse_case(tkns, as_name=None):
                def get_stmt(token):
                    if is_function(token):
                        return sql_function(token)
                    else:
                        return col_identifier(token)

                # give auto-genereted name if no alias specified
                if as_name is None:
                    self.case_num += 1
                    as_name = 'case' + str(self.case_num)
                case = {'as_name': as_name,
                        'stmts': []}
                # remove whitespace from tokens
                tkns = strip_tkns(tkns)
                # need to parse backwards for proper order of operations
                for i, token in reversed(list(enumerate(tkns))):
                    # stop at CASE as we are looping in reverse so will
                    # be starting at END
                    if token.ttype == tokens.Keyword.CASE:
                        break
                    elif tkns[i-1].value == 'ELSE':
                        case['else_stmt'] = get_stmt(token)
                    elif tkns[i-1].value == 'THEN':
                        stmt = get_stmt(token)
                    elif is_comparison(token):
                        if token.is_group():
                            cond = comparison([token.tokens])
                        else:
                            cond = comparison([[tkns[i-1], token,
                                                tkns[i+1]]])
                        case['stmts'].append((cond, stmt))

                _cases = cases.get(curr_sect, [])
                _cases.append(case)
                cases[curr_sect] = _cases
                return as_name, None

            def parse_select(tkns):
                identifiers = []
                tkns = strip_tkns(tkns)
                for i, token in enumerate(tkns):
                    if token.ttype is tokens.Wildcard:
                        return
                    elif is_identifier(token):
                        identifiers = [col_identifier(token)]
                    elif token.is_group():
                        identifiers = identifier_list(token)
                return identifiers

            def parse_into(tkns):
                for token in tkns:
                    if is_identifier(token):
                        return token.value

            def tbl_identifier(tkns):
                """returns identifier as tuple of
                tablename, identifier"""
                if len(tkns) == 1:
                    return (tkns[0].value,) * 2
                return tkns[0].value, tkns[-1].value

            def parse_from(tkns):
                how = None
                for i, token in enumerate(tkns):
                    if token._get_repr_name() == 'Parenthesis':
                        table, identifier = tbl_identifier(tkns[i+1].tokens)
                        table = '###temp_' + table
                        nested = parse_tkns(token.tokens[1:-1])
                        nested_queries[table] = nested
                        # remove next token from list as it is already processed
                        del tkns[i+1]
                    elif token.is_group():
                        table, identifier = tbl_identifier(token.tokens)
                    elif 'JOIN' in token.value:
                        how = token.value.split()[0].lower()
                        break
                if how is not None:
                    parse_join(tkns[i+1:], how)

                return table, identifier

            def parse_join(tkns, how):
                for i, token in enumerate(tkns):
                    if 'JOIN' in token.value:
                        how_new = token.value.split()[0].lower()
                        parse_join(tkns[i+1:], how_new)
                        break
                    elif token._get_repr_name() == 'Parenthesis':
                        right, right_identifier = tbl_identifier(tkns[i+1].tokens)
                        right = '###temp_' + right
                        nested = parse_tkns(token.tokens[1:-1])
                        nested_queries[right] = nested
                        # remove next token from list as it is already processed
                        del tkns[i+1]
                    elif is_comparison(token):
                        left_on = col_identifier(token.tokens[0])[0]
                        right_on = col_identifier(token.tokens[-1])[0]
                    elif token.is_group():
                        right, right_identifier = tbl_identifier(token.tokens)
                joins.append((right, how, left_on, right_on, right_identifier))

            def parse_where(tkns):
                # list of boolean indices to apply to current value
                comps = [token.tokens for token in tkns if is_comparison(token)]
                operators = [token.value for token in tkns
                             if token.value in ('AND', 'OR')]
                return comparison(comps, operators)

            def parse_group(tkns):
                for tkn in tkns:
                    if tkn.is_group():
                        group_by = zip(*identifier_list(tkn))[0]
                return group_by

            def parse_order(tkns):
                for token in tkns:
                    if token.is_group():
                        identifiers = identifier_list(token)
                return identifiers

            sections = {'SELECT': parse_select,
                        'INTO': parse_into,
                        'FROM': parse_from,
                        'WHERE': parse_where,
                        'GROUP': parse_group,
                        'ORDER': parse_order}

            # remove whitespace from tokens
            tkns = strip_tkns(tkns)

            _parsed = {}
            for i, token in enumerate(tkns):
                if i == 0:
                    start = 0
                    curr_sect = token.value.upper()
                    continue
                if token._get_repr_name().upper() == 'WHERE':
                    _parsed[curr_sect] = sections[curr_sect](tkns[start:i])
                    # start next category of statement
                    curr_sect = 'WHERE'
                    _parsed['WHERE'] = sections['WHERE'](token.tokens)
                    continue
                if token.value.upper() in sections.keys() \
                        and token.ttype in tokens.Keyword:
                    if curr_sect != 'WHERE':
                        _parsed[curr_sect] = sections[curr_sect](tkns[start:i])
                    # start next category of statement
                    start = i
                    curr_sect = token.value.upper()

            # add in last section
            if curr_sect != 'WHERE':
                _parsed[curr_sect] = sections[curr_sect](tkns[start:])

            get_fns(tkns)
            _parsed['FUNCTIONS'] = fns
            _parsed['JOINS'] = joins
            _parsed['ALIASES'] = aliases
            _parsed['CASES'] = cases
            _parsed['NESTED_QUERIES'] = nested_queries
            _parsed['OPS'] = ops
            _parsed['LITERALS'] = literals
            return _parsed

        tkns = parse(statement)[0].tokens
        return parse_tkns(tkns)
