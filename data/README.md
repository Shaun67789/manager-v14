# 📦 Database Folder

Place your `manager.db` file here.

If you downloaded a backup from the dashboard (`/api/download_db`), just drop it in this folder as `manager.db` and redeploy.

```
data/
  manager.db   ← your database goes here
  README.md    ← this file
```

The bot will automatically pick it up and migrate any missing columns.
