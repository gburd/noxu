#!/usr/bin/env python3
"""
Fix intra-module `crate::` references in moved files.

In files under src/<mod>/*.rs, any `crate::<x>` where <x> is NOT one of
the 18 merged module names was formerly an intra-crate reference and must
now be `crate::<mod>::<x>`.
"""
import sys
import re
import os

# These are the 18 module names at the noxu crate root.
ROOT_MODS = {
    "util", "sync", "latch", "config", "log", "tree", "txn",
    "evictor", "cleaner", "recovery", "dbi", "engine", "db",
    "bind", "collections", "persist", "xa", "rep", "observe",
}

def fix_intra_module_refs(content: str, mod_name: str) -> str:
    """
    Replace `crate::<x>` with `crate::<mod_name>::<x>` for any x that is
    NOT in ROOT_MODS (i.e., was a local module/type in the original crate).
    
    Handles:
    - crate::SimpleIdent     -> crate::mod::SimpleIdent
    - crate::{A, B}          -> crate::mod::{A, B}
    - crate::ident::subpath  -> crate::mod::ident::subpath  (if ident not in ROOT_MODS)
    """
    def replace_crate_ref(m):
        after = m.group(1)  # everything after `crate::`
        
        # Extract the first identifier (or `{` for brace group)
        first_ident_match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)', after)
        if first_ident_match:
            first_ident = first_ident_match.group(1)
            if first_ident in ROOT_MODS:
                return m.group(0)  # already a correct root-level reference
        elif after.startswith('{'):
            pass  # brace group, needs fixing
        else:
            return m.group(0)  # unknown pattern, leave alone
        
        return f"crate::{mod_name}::" + after
    
    # Match `crate::` followed by either an identifier or `{`
    # Use a negative lookahead to avoid matching the module name itself
    content = re.sub(r'\bcrate::([a-zA-Z_][a-zA-Z0-9_]*|\{[^}]*\})', replace_crate_ref, content)
    return content

if __name__ == "__main__":
    mod_name = sys.argv[1]  # e.g. "sync", "log", "db"
    filepath = sys.argv[2]
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    content = fix_intra_module_refs(content, mod_name)
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Fixed {filepath}")
    else:
        print(f"No change: {filepath}")
