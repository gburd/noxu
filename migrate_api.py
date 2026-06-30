#!/usr/bin/env python3
"""Mechanical 7.0 API migration for Noxu DB call sites.

Transforms (semantics-preserving):
  reads  : X.get(TXN, K, &mut D)  -> X.get_into(TXN, K, &mut D) (bool)
           with surrounding `== OperationStatus::Success` collapsed to the bool.
  writes : X.put(None, K, V)            -> X.put(K, V)
           X.put(Some(&t), K, V)        -> X.put_in(&t, K, V)
           X.delete(None, K)            -> X.delete(K)
           X.delete(Some(&t), K)        -> X.delete_in(&t, K)
           X.put_no_overwrite(None,K,V) -> X.put_no_overwrite(K,V)
           X.put_no_overwrite(Some(&t),K,V) -> X.put_no_overwrite_in(&t,K,V)
  cursor : X.open_cursor(None, C)       -> X.open_cursor(C)
           X.open_cursor(Some(&t), C)   -> X.open_cursor_in(&t, C)

This operates with a paren-balanced argument splitter so nested calls are safe.
Only the FIRST argument (the txn) is rewritten; key/value args are untouched.
"""
import re
import sys


def split_args(s):
    """Split a top-level comma-separated argument list (no surrounding parens)."""
    args, depth, cur = [], 0, ""
    for ch in s:
        if ch in "([{":
            depth += 1
            cur += ch
        elif ch in ")]}":
            depth -= 1
            cur += ch
        elif ch == "," and depth == 0:
            args.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip() or args:
        args.append(cur)
    return args


def find_call(text, start):
    """Given text and index of '(' return index just past the matching ')'."""
    depth = 0
    i = start
    while i < len(text):
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


METHODS = ("get", "put", "delete", "put_no_overwrite",
           "put_with_options", "get_with_options", "open_cursor")


def rewrite(text):
    out = []
    i = 0
    # match `.<method>(` where method in METHODS
    pat = re.compile(r"\.(get|put|delete|put_no_overwrite|put_with_options|get_with_options|open_cursor)\(")
    while True:
        m = pat.search(text, i)
        if not m:
            out.append(text[i:])
            break
        method = m.group(1)
        open_paren = m.end() - 1
        close = find_call(text, open_paren)
        if close == -1:
            out.append(text[i:m.end()])
            i = m.end()
            continue
        inner = text[open_paren + 1:close]
        args = split_args(inner)
        # Drop a trailing empty arg from a trailing comma (multi-line calls).
        if args and args[-1].strip() == "":
            trailing_comma = True
            args = args[:-1]
        else:
            trailing_comma = False
        first = args[0].strip() if args else ""
        new_call = None

        is_none = first == "None"
        m_some = re.fullmatch(r"Some\((&?[\w.]+|&?[\w.]+\(\))\)", first) if first else None
        # Generic Some(<expr>) capture (balanced not needed; txn exprs are simple)
        if not m_some and first.startswith("Some(") and first.endswith(")"):
            m_some = re.match(r"Some\((.*)\)$", first)

        rest = [a for a in args[1:]]

        def join(parts):
            return ",".join(parts)

        if method == "get":
            # out-param read forms: primary (txn, key, &mut data) = 3 args,
            # secondary (txn, key, &mut p_key, &mut data) = 4 args.  Both
            # route to get_into, preserving the Option txn.
            if len(args) in (3, 4) and (is_none or m_some):
                if is_none:
                    new_call = ".get_into(None," + join(rest) + ")"
                else:
                    txnexpr = m_some.group(1)
                    new_call = ".get_into(Some(" + txnexpr + ")," + join(rest) + ")"
        elif method == "get_with_options":
            # keep Option txn signature; no change needed (txn stays Option)
            new_call = None
        elif method == "put_with_options":
            new_call = None  # txn stays Option
        elif method in ("put", "put_no_overwrite"):
            if len(args) == 3 and is_none:
                new_call = "." + method + "(" + join(rest) + ")"
            elif len(args) == 3 and m_some:
                txnexpr = m_some.group(1)
                new_call = "." + method + "_in(" + txnexpr + "," + join(rest) + ")"
        elif method == "delete":
            if len(args) == 2 and is_none:
                new_call = ".delete(" + join(rest) + ")"
            elif len(args) == 2 and m_some:
                txnexpr = m_some.group(1)
                new_call = ".delete_in(" + txnexpr + "," + join(rest) + ")"
        elif method == "open_cursor":
            if len(args) == 2 and is_none:
                new_call = ".open_cursor(" + join(rest) + ")"
            elif len(args) == 2 and m_some:
                txnexpr = m_some.group(1)
                new_call = ".open_cursor_in(" + txnexpr + "," + join(rest) + ")"

        out.append(text[i:m.start()])
        if new_call is not None:
            out.append(new_call)
        else:
            out.append(text[m.start():close + 1])
        i = close + 1
    return "".join(out)


def fix_comparisons(text):
    """Second pass: fix `OperationStatus` comparisons left dangling after the
    call-shape rewrite.  Uses balanced-paren matching for `assert_eq!(...)`.
    """
    out = []
    i = 0
    needle = "assert_eq!("
    while True:
        j = text.find(needle, i)
        if j == -1:
            out.append(text[i:])
            break
        open_paren = j + len(needle) - 1
        close = find_call(text, open_paren)
        if close == -1:
            out.append(text[i:j + len(needle)])
            i = j + len(needle)
            continue
        inner = text[open_paren + 1:close]
        args = split_args(inner)
        replaced = None
        if len(args) >= 2 and "OperationStatus::" in args[1]:
            expr = args[0].strip()
            sm = re.search(r"OperationStatus::(\w+)", args[1])
            status = sm.group(1)
            extra = "".join("," + a for a in args[2:]) if len(args) > 2 else ""
            if re.search(r"\.(put|put_in)\(", expr) and "no_overwrite" not in expr:
                replaced = expr  # Result<()>: just run it
            elif re.search(r"\.(get_into|delete_in|put_no_overwrite_in)\(|\.(delete|put_no_overwrite)\((?!\))", expr):
                if status == "Success":
                    replaced = "assert!(" + expr + extra + ")"
                elif status in ("NotFound", "KeyExist", "KeyExists"):
                    replaced = "assert!(!(" + expr + ")" + extra + ")"
        out.append(text[i:j])
        if replaced is not None:
            out.append(replaced)
        else:
            out.append(text[j:close + 1])
        i = close + 1
    return "".join(out)


def fix_var_status(text):
    """Third pass: track `let VAR = <converted call>...;` bindings and fix
    later `OperationStatus` comparisons on VAR.

    Tracking is scoped per top-level `fn` block (split on `\n    fn ` and
    `\nfn `) so a variable named `status` in one test does not pollute the
    classification of `status` in another.
    """
    # Split into chunks at function boundaries, keeping the delimiter.
    parts = re.split(r"(\n(?:    )?(?:pub )?(?:async )?fn )", text)
    return "".join(_fix_chunk(p) for p in parts)


def _fix_chunk(text):
    kind = {}  # var -> 'unit' | 'bool' | 'ambiguous'
    for m in re.finditer(r"let\s+(?:mut\s+)?(\w+)\s*=\s*([^;]*?);", text, re.S):
        var, rhs = m.group(1), m.group(2)
        if re.search(r"\.(put|put_in)\(", rhs) and "no_overwrite" not in rhs:
            this = "unit"
        elif re.search(r"\.(get_into|delete_in|put_no_overwrite_in)\(|\.(delete|put_no_overwrite)\((?!\))", rhs):
            this = "bool"
        elif re.search(r"OperationStatus", rhs) or re.search(r"\.(get|put|delete)\(", rhs):
            this = "ambiguous"
        else:
            continue
        if var in kind and kind[var] != this:
            kind[var] = "ambiguous"
        else:
            kind[var] = this

    def status_ok(status):
        return status == "Success"

    # Balanced assert_eq!(VAR, OperationStatus::X [, msg...]) handling.
    out = []
    i = 0
    needle = "assert_eq!("
    while True:
        j = text.find(needle, i)
        if j == -1:
            out.append(text[i:])
            break
        open_paren = j + len(needle) - 1
        close = find_call(text, open_paren)
        if close == -1:
            out.append(text[i:j + len(needle)])
            i = j + len(needle)
            continue
        args = split_args(text[open_paren + 1:close])
        replaced = None
        if len(args) >= 2 and re.fullmatch(r"\s*\w+\s*", args[0]) \
                and "OperationStatus::" in args[1]:
            var = args[0].strip()
            sm = re.search(r"OperationStatus::(\w+)", args[1])
            status = sm.group(1)
            extra = "".join("," + a for a in args[2:]) if len(args) > 2 else ""
            k = kind.get(var)
            if k == "unit":
                replaced = ""
            elif k == "bool":
                neg = "" if status_ok(status) else "!"
                replaced = "assert!(" + neg + var + extra + ")"
        out.append(text[i:j])
        out.append(replaced if replaced is not None else text[j:close + 1])
        i = close + 1
    text = "".join(out)

    def cmp(m):
        var, op, status = m.group(1), m.group(2), m.group(3)
        k = kind.get(var)
        if k == "bool":
            positive = status_ok(status)
            if op == "==":
                return var if positive else ("!" + var)
            else:
                return ("!" + var) if positive else var
        return m.group(0)

    text = re.sub(
        r"(\w+)\s*(==|!=)\s*(?:noxu_db::)?OperationStatus::(\w+)",
        cmp, text)
    return text


def fix_inline_unwrap_cmp(text):
    """Fourth pass: inline `<bool-call>.unwrap() OP OperationStatus::X`.

    Only fires when the comparison's left side is an expression that ends in
    `.unwrap()` AND contains a converted bool-returning call (get_into /
    delete / delete_in / put_no_overwrite[_in]).  Uses a small balanced scan
    backwards from `.unwrap()` is overkill; instead we match the common
    `EXPR.unwrap() OP OperationStatus::X` where EXPR has balanced parens.
    """
    bool_call = re.compile(r"\.(get_into|delete_in|put_no_overwrite_in)\(|\.(delete|put_no_overwrite)\((?!\))")
    out = []
    i = 0
    while True:
        m = re.search(r"\.unwrap\(\)\s*(==|!=)\s*(?:noxu_db::)?OperationStatus::(\w+)", text[i:])
        if not m:
            out.append(text[i:])
            break
        abs_start = i + m.start()
        op, status = m.group(1), m.group(2)
        # Walk backwards from the `.unwrap()` to the start of the call
        # expression, tracking paren depth so we stop at the enclosing
        # boundary (the `(` of `assert!(`, a `;`, `{`, `}`, `,`, `&&`, `||`,
        # or line start) rather than inside `get_into(...)`.
        depth = 0
        p = abs_start - 1
        while p >= i:
            c = text[p]
            if c == ")":
                depth += 1
            elif c == "(":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and (c in ";{}\n," or
                                 text[p:p + 2] in ("&&", "||")):
                break
            p -= 1
        seg_start = p
        expr = text[seg_start + 1:abs_start + len(".unwrap()")]
        # Strip a leading control-flow keyword if present.
        kw = re.match(r"(\s*)(if|while|match|return|let\s+\w+\s*=)\s+", expr)
        prefix = ""
        if kw:
            prefix = expr[:kw.end()]
            expr = expr[kw.end():]
        if bool_call.search(expr):
            positive = (status == "Success")
            if op == "==":
                repl = expr if positive else ("!(" + expr + ")")
            else:
                repl = ("!(" + expr + ")") if positive else expr
            out.append(text[i:seg_start + 1])
            out.append(prefix + repl)
            i = i + m.end()
        else:
            out.append(text[i:abs_start + len(".unwrap()")])
            i = abs_start + len(".unwrap()")
    return "".join(out)


def fix_match_on_bool(text):
    """Fifth pass: `match EXPR.<boolcall>(...).unwrap() { OperationStatus::
    Success => .., OperationStatus::NotFound => .. }` -> bool arms.
    """
    bool_call = re.compile(r"\.(get_into|delete_in|put_no_overwrite_in)\(|\.(delete|put_no_overwrite)\((?!\))")
    out = []
    i = 0
    while True:
        m = re.search(r"match\s+([^\n{]*?\.unwrap\(\))\s*\{", text[i:])
        if not m:
            out.append(text[i:])
            break
        scrut = m.group(1)
        brace_open = i + m.end() - 1
        # find matching close brace
        depth = 0
        p = brace_open
        while p < len(text):
            if text[p] == "{":
                depth += 1
            elif text[p] == "}":
                depth -= 1
                if depth == 0:
                    break
            p += 1
        block = text[brace_open:p + 1]
        out.append(text[i:i + m.start()])
        if bool_call.search(scrut):
            block = re.sub(r"(?:noxu_db::)?OperationStatus::Success(\s*=>)", r"true\1", block)
            block = re.sub(r"(?:noxu_db::)?OperationStatus::NotFound(\s*=>)", r"false\1", block)
            out.append("match " + scrut + " ")
            out.append(block)
            i = p + 1
        else:
            out.append(text[i + m.start():p + 1])
            i = p + 1
    return "".join(out)


def main():
    for path in sys.argv[1:]:
        with open(path) as f:
            src = f.read()
        new = rewrite(src)
        new = fix_comparisons(new)
        new = fix_var_status(new)
        new = fix_inline_unwrap_cmp(new)
        new = fix_match_on_bool(new)
        if new != src:
            with open(path, "w") as f:
                f.write(new)
            print("rewrote", path)


if __name__ == "__main__":
    main()
