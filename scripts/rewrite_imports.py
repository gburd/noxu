#!/usr/bin/env python3
"""
Rewrite noxu_xxx:: import paths in Rust source files.

In src/ files (internal to crate):  noxu_xxx:: -> crate::xxx::
In tests/ files (external):        noxu_db::  -> noxu::   (since db surface is re-exported at root)
                                   noxu_xxx:: -> noxu::xxx::
"""
import sys
import re
import os

CRATE_MAP = [
    "util", "sync", "latch", "config", "log", "tree", "txn",
    "evictor", "cleaner", "recovery", "dbi", "engine", "db",
    "bind", "collections", "persist", "xa", "rep", "observe",
]

def rewrite_src(content: str) -> str:
    """Rewrite imports for a file living inside src/ (uses crate:: prefix)."""
    for mod in CRATE_MAP:
        crate_name = f"noxu_{mod}"
        # noxu_xxx:: -> crate::xxx::
        content = re.sub(rf'\bnoxu_{mod}::', f'crate::{mod}::', content)
        # `use noxu_xxx;` -> `use crate::xxx;`
        content = re.sub(rf'\buse noxu_{mod};', f'use crate::{mod};', content)
        # `use noxu_xxx as foo` -> `use crate::xxx as foo`
        content = re.sub(rf'\buse noxu_{mod}\b', f'use crate::{mod}', content)
    return content

def rewrite_test(content: str) -> str:
    """Rewrite imports for a file living inside tests/ (uses noxu:: prefix)."""
    # db types are re-exported at noxu root, so noxu_db:: -> noxu::
    content = re.sub(r'\bnoxu_db::', 'noxu::', content)
    content = re.sub(r'\buse noxu_db;', 'use noxu;', content)
    content = re.sub(r'\buse noxu_db\b', 'use noxu', content)
    # All other noxu_xxx:: -> noxu::xxx::
    for mod in CRATE_MAP:
        if mod == "db":
            continue
        content = re.sub(rf'\bnoxu_{mod}::', f'noxu::{mod}::', content)
        content = re.sub(rf'\buse noxu_{mod};', f'use noxu::{mod};', content)
        content = re.sub(rf'\buse noxu_{mod}\b', f'use noxu::{mod}', content)
    return content

def strip_crate_attrs(content: str) -> str:
    """Remove crate-level attributes that conflict when used in module context."""
    # Remove #![forbid(unsafe_code)] - the merged crate contains unsafe
    content = re.sub(r'#!\[forbid\(unsafe_code\)\]\s*\n', '', content)
    # Remove #![crate_type = ...] and #![crate_name = ...]
    content = re.sub(r'#!\[crate_(?:type|name)[^\]]*\]\s*\n', '', content)
    return content

if __name__ == "__main__":
    mode = sys.argv[1]  # "src" or "test"
    filepath = sys.argv[2]
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if mode == "src":
        content = strip_crate_attrs(content)
        content = rewrite_src(content)
    elif mode == "test":
        content = rewrite_test(content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Processed {filepath}")
