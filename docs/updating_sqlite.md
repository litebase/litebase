# Updating SQLite

Litebase embeds SQLite as the core database engine. As new SQLite versions are released, the can be incorporated into this project by running the following command:

```bash
./scripts/update_sqlite.sh
```

The `sqlite3.c` and `sqlite3.h` files will be updated. These changes can be committed to a new branch, and a [pull request](https://github.com/litebase/litebase/pulls) opened to ensure all tests pass.
