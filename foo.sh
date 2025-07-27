#!/bin/bash

SPDX_LINE="# SPDX-License-Identifier: Apache-2.0"
COPYRIGHT_LINE="# Copyright 2024 Mike Schultz"

# Recursively find your project .py files
find . \
  -type f \
  -name "*.py" \
  ! -name "__init__.py" \
  ! -path "./venv/*" \
  ! -path "./.local/*" \
  ! -path "./.openai_internal/*" \
  | while read -r file; do
    if ! grep -q "$SPDX_LINE" "$file"; then
        echo "🔧 Adding SPDX header to $file"
        tmpfile=$(mktemp)
        {
            echo "$SPDX_LINE"
            echo "$COPYRIGHT_LINE"
            echo
            cat "$file"
        } > "$tmpfile"
        mv "$tmpfile" "$file"
    fi
done
