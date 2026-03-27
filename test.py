try:
    from database import db
    stats = db.get_all_stats()
    print("Stats:", stats)
except Exception as e:
    import traceback
    traceback.print_exc()
