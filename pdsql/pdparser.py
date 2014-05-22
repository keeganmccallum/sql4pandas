from sqlparse_mod import parse, tokens


class PDParser(object):
    """class used to parse sql statements into a data structure
    which can be used to execute the statement"""
    def __init__(self):
        self.fns = {}
        self.joins = []
        self.aliases = {}
        self.case_num = 0
        self.cases = {}

    def strip_tkns(self, tkns, punctuation=None):
        """convenience function to remove whitespace tokens and comments
        from list, optionally also remove punctuation"""
        if punctuation is None:
            return [token for token in tkns if not token.is_whitespace()
                    and token._get_repr_name() != 'Comment']
        return [token for token in tkns if not token.is_whitespace()
                and token._get_repr_name() != 'Comment'
                and token.ttype != tokens.Token.Punctuation]

    def parse_statement(self, statement):
        tkns = parse(statement)[0].tokens
        sections = {tokens.Keyword.SELECT: self.parse_select,
                    tokens.Keyword.FROM: self.parse_from,
                    tokens.Keyword.WHERE: self.parse_where,
                    tokens.Keyword.GROUP: self.parse_group,
                    tokens.Keyword.ORDER: self.parse_order}

        # remove whitespace from tokens
        tkns = self.strip_tkns(tkns)

        _parsed = {}
        for i, token in enumerate(tkns):
            if i == 0:
                start = 0
                self._curr_sect = token.value
                continue
            if token.ttype in sections.keys():
                _parsed[self._curr_sect] = \
                    sections[tkns[start].ttype](tkns[start:i])
                # start next category of statement
                start = i
                self._curr_sect = token.value

        # add in last section
        _parsed[self._curr_sect] = sections[tkns[start].ttype](tkns[start:])

        self.get_fns(tkns)
        _parsed['FUNCTIONS'] = self.fns
        _parsed['JOINS'] = self.joins
        _parsed['ALIASES'] = self.aliases
        _parsed['CASES'] = self.cases

        return _parsed

    def get_fns(self, tkns):
        """get a dictionary of all functions in statement, needed for order
        of operations with grouping and case statements"""
        for tkn in tkns:
            if tkn._get_repr_name() == 'Function':
                col, fn = self.sql_function(tkn)
                fns = self.fns.get(col, [])
                if fn not in fns:
                    fns.append(fn)
                self.fns[col] = fns
            elif tkn.is_group():
                self.get_fns(tkn.tokens)

    def col_identifier(self, token):
        tkns = token.tokens
        # strip whitespace and punctuation
        tkns = self.strip_tkns(tkns, punctuation=True)
        if len(tkns) == 1:
            identifier = tkns[0].value
            # handle issue of ambigous column names through aliasing
            # for now, may be able to find a more efficient way in future
            self.aliases[identifier] = None, None
            return identifier, None
        elif len(tkns) == 3 and tkns[1].value.upper() == 'AS':
            # handle aliasing
            as_name = tkns[-1].value
            if tkns[0]._get_repr_name() == 'Case':
                return self.parse_case(tkns[0].tokens, as_name=as_name)
            elif tkns[0]._get_repr_name() == 'Identifier':
                self.aliases[as_name] = self.col_identifier(tkns[0]), None
            elif tkns[0]._get_repr_name() == 'Function':
                col, fn = self.sql_function(tkns[0])
                self.aliases[as_name] = col, fn
            return as_name, None
        elif len(tkns) == 4 and tkns[2].value.upper() == 'AS':
            # handle aliasing for special case where parser doesn't group
            # identifier properly
            as_name = tkns[-1].value
            self.aliases[as_name] = tkns[0].value + "." + tkns[1].value, None
            return as_name, None
        else:
            return tkns[0].value + '.' + tkns[-1].value, None

    def sql_function(self, token):
        tkns = token.tokens
        fn, parens = tkns
        col = parens.tokens[1]
        fn = fn.value.lower()
        col = self.col_identifier(col)[0]
        return col, fn

    def identifier_list(self, token):
        """used to parse sql identifiers into actual
        table/column groupings"""
        if token._get_repr_name() == 'Identifier':
            return self.col_identifier(token)

        if token._get_repr_name() == 'Function':
            return [self.sql_function(token)]

        if token._get_repr_name() == 'Case':
            return self.parse_case(token)

        tkns = token.tokens
        if len(tkns) == 1:
            if tkns[0]._get_repr_name() == 'Function':
                return self.sql_function(tkns[0])
            return self.col_identifier(token)
        proc = []
        # filter whitespace and punctuation
        for tkn in tkns:
            if token._get_repr_name() == 'Identifier':
                proc.append(self.col_identifier(tkn))
            elif tkn._get_repr_name() == 'Case':
                proc.append(self.parse_case(tkn.tokens))
            elif tkn._get_repr_name() == 'Function':
                col, fn = self.sql_function(tkn)
                proc.append((col, fn))
            elif not tkn.is_whitespace() \
                    and tkn.ttype != tokens.Punctuation:
                proc.append(self.col_identifier(tkn))

        return proc

    def comparison(self, comps, operators=None):
        # identifiers used in comparision, needed to work around issue #83
        identifiers = {}

        # need a counter for number of comparisons for variable names
        def comp_str(comp):
            comp_map = {
                '=': '==',
                '<>': '!=',
            }
            comp = self.strip_tkns(comp)
            assert len(comp) == 3
            col, comp, val = comp
            comp = comp_map.get(comp.value, comp.value)
            if col._get_repr_name() == 'Function':
                col, fn = self.sql_function(col)
                col_str = (col+'_'+fn).replace('.', '_')
                identifiers[col_str] = col, fn
            elif col.is_group():
                col = self.col_identifier(col)[0]
                col_str = col.replace('.', '_')
                identifiers[col_str] = col, None
            if val.is_group():
                val = self.col_identifier(val)[0]
                identifiers[val.replace('.', '_')] = val, None
            else:
                val = val.value
            val_str = val.replace('.', '_')
            return """({col} {comp} {val})""".format(col=col_str,
                                                     comp=comp, val=val_str)

        comp = comps[0]
        ev_str = comp_str(comp)
        if operators is not None:
            for comp, op in zip(comps[1:], operators):
                # build string to eventually evanluate
                if op == 'AND':
                    ev_str += " & " + comp_str(comp)
                elif op == 'OR':
                    ev_str += " | " + comp_str(comp)

        return ev_str, identifiers

    def parse_case(self, tkns, as_name=None):
        def get_stmt(token):
            if token._get_repr_name() == 'Function':
                return self.sql_function(token)
            else:
                return self.col_identifier(token)

        # give auto-genereted name if no alias specified
        if as_name is None:
            self.case_num += 1
            as_name = 'case' + str(self.case_num)
        case = {'as_name': as_name,
                'stmts': []}
        # remove whitespace from tokens
        tkns = self.strip_tkns(tkns)
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
            elif token._get_repr_name() == 'Comparison':
                if token.is_group():
                    cond = self.comparison([token.tokens])
                else:
                    cond = self.comparison([[tkns[i-1], token, tkns[i+1]]])
                case['stmts'].append((cond, stmt))

        cases = self.cases.get(self._curr_sect, [])
        cases.append(case)
        self.cases[self._curr_sect] = cases
        return as_name, None

    def parse_select(self, tkns):
        identifiers = []
        tkns = self.strip_tkns(tkns)
        for i, token in enumerate(tkns):
            if token.ttype is tokens.Wildcard:
                return
            elif token._get_repr_name() == 'Identifier':
                identifiers = [self.col_identifier(token)]
            elif token.is_group():
                identifiers = self.identifier_list(token)
        return identifiers

    def tbl_identifier(self, tkns):
        """returns identifier as tuple of
        tablename, identifier"""
        if len(tkns) == 1:
            return (tkns[0].value,) * 2
        return tkns[0].value, tkns[-1].value

    def parse_from(self, tkns):
        how = None
        for i, token in enumerate(tkns):
            if token.is_group():
                table, identifier = self.tbl_identifier(token.tokens)
            elif 'JOIN' in token.value:
                how = token.value.split()[0].lower()
                break
        if how is not None:
            self.parse_join(tkns[i+1:], how)

        return table, identifier

    def parse_join(self, tkns, how):
        for i, token in enumerate(tkns):
            if 'JOIN' in token.value:
                how_new = token.value.split()[0].lower()
                self.parse_join(tkns[i+1:], how_new)
                break
            elif token._get_repr_name() == 'Comparison':
                left_on = self.col_identifier(token.tokens[0])[0]
                right_on = self.col_identifier(token.tokens[-1])[0]
            elif token.is_group():
                right, right_identifier = self.tbl_identifier(token.tokens)
        self.joins.append((right, how, left_on, right_on, right_identifier))

    def parse_where(self, tkns):
        # list of boolean indices to apply to current value
        comps = [token.tokens for token in tkns
                 if token._get_repr_name() == 'Comparison']
        operators = [token.value for token in tkns
                     if token.value in ('AND', 'OR')]
        return self.comparison(comps, operators)

    def parse_group(self, tkns):
        for tkn in tkns:
            if tkn.is_group():
                group_by = zip(*self.identifier_list(tkn))[0]
        return group_by

    def parse_order(self, tkns):
        for token in tkns:
            if token.is_group():
                identifiers = self.identifier_list(token)
        return identifiers
