#!/usr/bin/env python3
"""
move_imports.py

Refactor import lists by moving specific names from one module to another.

CLI:
  python move_imports.py pjk.base:Usage,ParsedToken pjk.usage --root=src --dry-run
  python move_imports.py pjk.base:Usage,ParsedToken pjk.usage --root=src

Behavior:
  from pjk.base import A, Usage as U, ParsedToken, B
→
  from pjk.base import A, B
  from pjk.usage import Usage as U, ParsedToken

Also supports relative:
  from .base import Usage → from .usage import Usage

Dry-run prints exact old/new blocks per file; apply writes files.
"""

from __future__ import annotations
import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple


# =========================
# Spec
# =========================

@dataclass(frozen=True)
class MoveSpec:
    from_module: str              # e.g. "pjk.base"
    to_module: str                # e.g. "pjk.usage"
    moved_symbols: Tuple[str, ...]  # e.g. ("Usage", "ParsedToken")

    @property
    def from_basename(self) -> str:
        return self.from_module.rsplit(".", 1)[-1]

    @property
    def to_basename(self) -> str:
        return self.to_module.rsplit(".", 1)[-1]

    @staticmethod
    def parse(from_arg: str, to_arg: str) -> "MoveSpec":
        if ":" not in from_arg:
            raise ValueError('FROM must be "module.path:Name1,Name2" (e.g., "pjk.base:Usage,ParsedToken")')
        module, names = from_arg.split(":", 1)
        module = module.strip()
        to_arg = to_arg.strip()
        if not module or not to_arg:
            raise ValueError("Modules must be non-empty.")
        moved = tuple(n.strip() for n in names.split(",") if n.strip())
        if not moved:
            raise ValueError("No symbols provided to move.")
        return MoveSpec(from_module=module, to_module=to_arg, moved_symbols=moved)


# =========================
# Mover
# =========================

class ImportMover:
    """
    Rewrites 'from X import ...' where X matches:
      - exactly the absolute FROM module (e.g., 'pjk.base'), OR
      - a relative module whose basename matches FROM ('.base', '..base').

    It splits items into "stay" (keep on original module) and "moved" (rewrite to TO module).
    """

    _FROM_RE = re.compile(r'^(?P<indent>[ \t]*)from\s+(?P<module>[A-Za-z_][\w\.]*|\.+)\s+import\s*(?P<tail>.*)$')

    def __init__(self, spec: MoveSpec, root: Path, dry_run: bool) -> None:
        self.spec = spec
        self.root = root
        self.dry_run = dry_run

    # ----- Public -----

    def run(self) -> int:
        changed_files = 0
        for path in self._iter_py_files():
            changed = self._process_file(path)
            if changed:
                changed_files += 1
        return changed_files

    # ----- File iteration -----

    def _iter_py_files(self) -> Iterable[Path]:
        skip = {".git", "venv", ".venv", "__pycache__", "build", "dist"}
        for p in self.root.rglob("*.py"):
            if any(part in skip for part in p.parts):
                continue
            yield p

    # ----- Core processing -----

    def _process_file(self, path: Path) -> bool:
        contents = path.read_text(encoding="utf-8")
        lines = contents.splitlines(keepends=True)

        i = 0
        out: List[str] = []
        modified = False
        previews: List[Tuple[str, str]] = []  # (old_block, new_block) for dry-run printing

        while i < len(lines):
            m = self._FROM_RE.match(lines[i])
            if not m:
                out.append(lines[i])
                i += 1
                continue

            indent, module_str, tail = m.group("indent"), m.group("module"), m.group("tail")
            block_lines, next_i = self._consume_import_block(lines, i, tail)
            original_block = "".join(block_lines)

            if not self._is_target_module(module_str):
                out.extend(block_lines)
                i = next_i
                continue

            items = self._parse_items_from_block(original_block)
            if not items:
                out.extend(block_lines)
                i = next_i
                continue

            moved, stay = self._partition_items(items, set(self.spec.moved_symbols))
            if not moved:
                out.extend(block_lines)
                i = next_i
                continue

            new_block = self._build_replacement(indent, module_str, moved, stay)

            if self.dry_run:
                previews.append((original_block, new_block))
                out.extend(block_lines)  # do NOT modify buffer in dry-run
            else:
                out.append(new_block)
                modified = True

            i = next_i

        if self.dry_run:
            if previews:
                print(f"\nFile: {path}")
                for old, new in previews:
                    print("--- old:")
                    print(old, end="" if old.endswith("\n") else "\n")
                    print("+++ new:")
                    print(new, end="" if new.endswith("\n") else "\n")
            # A file "changed" in dry-run means we *would* change it.
            return bool(previews)

        if modified:
            path.write_text("".join(out), encoding="utf-8")
            return True
        return False

    # ----- Helpers -----

    def _consume_import_block(self, lines: List[str], start_idx: int, tail: str) -> Tuple[List[str], int]:
        """
        Returns (block_lines, next_index_after_block).
        Extends through subsequent lines until parentheses are balanced.
        """
        block = [lines[start_idx]]
        depth = tail.count("(") - tail.count(")")
        i = start_idx + 1
        while depth > 0 and i < len(lines):
            block.append(lines[i])
            depth += lines[i].count("(") - lines[i].count(")")
            i += 1
        return block, i

    def _is_target_module(self, module_str: str) -> bool:
        if module_str == self.spec.from_module:
            return True
        if module_str.startswith(".") and module_str.rsplit(".", 1)[-1] == self.spec.from_basename:
            return True
        return False

    def _parse_items_from_block(self, block_text: str) -> List[str]:
        """
        Extract the item list after ' import ' in a block. Supports parenthesis and multiline.
        """
        try:
            _, after = block_text.split(" import ", 1)
        except ValueError:
            return []

        s = after.strip()
        if s.endswith("\n"):
            s = s[:-1]

        t = s.strip()
        if t.startswith("(") and t.endswith(")"):
            t = t[1:-1]

        items: List[str] = []
        buf: List[str] = []
        depth = 0
        for ch in t:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth = max(0, depth - 1)
                buf.append(ch)
            elif ch == "," and depth == 0:
                tok = "".join(buf).strip()
                if tok:
                    items.append(tok)
                buf = []
            else:
                buf.append(ch)
        last = "".join(buf).strip()
        if last:
            items.append(last)
        return items

    def _base_name(self, token: str) -> str:
        """
        Return symbol before any 'as Alias'.
        """
        parts = token.split()
        if len(parts) >= 3:
            for i in range(1, len(parts) - 1):
                if parts[i] == "as":
                    return " ".join(parts[:i])
        return token

    def _partition_items(self, items: List[str], moved_set: set[str]) -> Tuple[List[str], List[str]]:
        moved: List[str] = []
        stay: List[str] = []
        for tok in items:
            name = self._base_name(tok).strip()
            (moved if name in moved_set else stay).append(tok)
        return moved, stay

    def _rewrite_module_for_moved(self, module_str: str) -> str:
        """
        For moved tokens:
          - absolute exact FROM → TO
          - relative '.base'/'..base' → swap basename with TO basename
        """
        if module_str == self.spec.from_module:
            return self.spec.to_module
        if module_str.startswith("."):
            head, _, _ = module_str.rpartition(".")
            return (head + "." if head else ".") + self.spec.to_basename
        return module_str  # fallback, should not occur given _is_target_module

    def _build_replacement(self, indent: str, module_str: str,
                           moved_tokens: List[str], stay_tokens: List[str]) -> str:
        lines: List[str] = []
        if stay_tokens:
            lines.append(f"{indent}from {module_str} import {', '.join(stay_tokens)}")
        if moved_tokens:
            new_mod = self._rewrite_module_for_moved(module_str)
            lines.append(f"{indent}from {new_mod} import {', '.join(moved_tokens)}")
        return "\n".join(lines) + "\n"


# =========================
# CLI
# =========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Move selected imported names from one module to another.")
    p.add_argument("from_spec", help='Format: "module.path:Name1,Name2"  (e.g., "pjk.base:Usage,ParsedToken")')
    p.add_argument("to_module", help='Format: "module.path"             (e.g., "pjk.usage")')
    p.add_argument("--root", default=".", help="Root directory to scan (default: .)")
    p.add_argument("--dry-run", action="store_true", help="Show planned changes without writing files")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spec = MoveSpec.parse(args.from_spec, args.to_module)
    mover = ImportMover(spec=spec, root=Path(args.root), dry_run=args.dry_run)
    changed = mover.run()
    print(f"\n{'[DRY-RUN] Files that would change' if args.dry_run else 'Files changed'}: {changed}")


if __name__ == "__main__":
    main()

