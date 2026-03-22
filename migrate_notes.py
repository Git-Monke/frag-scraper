import sqlite3, json

DB_PATH = "fragrances.db"
NOTE_COLS = ("top_notes_json", "middle_notes_json", "base_notes_json")

conn = sqlite3.connect(DB_PATH)
rows = conn.execute(
    "SELECT id, top_notes_json, middle_notes_json, base_notes_json FROM fragrances"
).fetchall()

updated = 0
for row in rows:
    frag_id = row[0]
    new_vals = {}
    for i, col in enumerate(NOTE_COLS, 1):
        raw = row[i]
        if not raw:
            continue
        try:
            notes = json.loads(raw)
        except Exception:
            continue
        cleaned = [{k: v for k, v in n.items() if k != 'image_url'} for n in notes]
        new_json = json.dumps(cleaned)
        if new_json != raw:
            new_vals[col] = new_json
    if new_vals:
        set_clause = ", ".join(f"{c} = ?" for c in new_vals)
        conn.execute(
            f"UPDATE fragrances SET {set_clause} WHERE id = ?",
            list(new_vals.values()) + [frag_id],
        )
        updated += 1

conn.commit()
conn.close()
print(f"Migrated {updated} rows.")
