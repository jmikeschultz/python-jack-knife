# python-jack-knife (pjk)

A clean Python rewrite of the original open-source **Data Jack Knife (DJK)**.

The Java version will eventually die — this is its reincarnation.

For now, the CLI entrypoint is `pjk`, since `djk` is still bound to the old Java implementation.

---

## 🚀 Installation (Pre-1.0 Release)

Until 1.0 is reached, you can install directly from GitHub:

```bash
pip install git+https://github.com/jmikeschultz/python-jack-knife.git@v0.5.0
```

## Dot paths

`drop`, `select`, and named `reduce` aggregations support dictionary dot paths. See
[dot-path behavior and implementation notes](docs/dot_paths.md) for examples,
literal-key precedence, and possible future consolidation.
