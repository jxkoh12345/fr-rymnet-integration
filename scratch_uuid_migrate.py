import db

conn = db._connect()
cur = conn.cursor()

try:
    cur.execute("SELECT COUNT(*) FROM hik_records")
    before_records = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hik_record_status")
    before_status = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hik_record_status s LEFT JOIN hik_records r ON s.hik_record_id = r.id WHERE r.id IS NULL")
    orphans_before = cur.fetchone()[0]
    print(f"before: hik_records={before_records}, hik_record_status={before_status}, orphan status rows={orphans_before}")

    cur.execute("ALTER TABLE hik_records ADD COLUMN new_id uuid DEFAULT gen_random_uuid()")
    cur.execute("ALTER TABLE hik_record_status ADD COLUMN new_id uuid DEFAULT gen_random_uuid()")
    cur.execute("ALTER TABLE hik_record_status ADD COLUMN new_hik_record_id uuid")
    cur.execute("""
        UPDATE hik_record_status s SET new_hik_record_id = r.new_id
        FROM hik_records r WHERE s.hik_record_id = r.id
    """)

    # hik_records_test (LIKE ... INCLUDING ALL) shares hik_records_id_seq — give it its own
    # sequence first so dropping hik_records.id doesn't cascade into the test table.
    cur.execute("CREATE SEQUENCE IF NOT EXISTS hik_records_test_id_seq OWNED BY hik_records_test.id")
    cur.execute("ALTER TABLE hik_records_test ALTER COLUMN id SET DEFAULT nextval('hik_records_test_id_seq')")
    cur.execute("SELECT setval('hik_records_test_id_seq', COALESCE((SELECT MAX(id) FROM hik_records_test), 1))")

    cur.execute("ALTER TABLE hik_record_status DROP CONSTRAINT fk_hik_record")
    cur.execute("ALTER TABLE hik_record_status DROP CONSTRAINT hik_record_status_pkey")
    cur.execute("ALTER TABLE hik_records DROP CONSTRAINT hik_records_pkey")

    cur.execute("ALTER TABLE hik_record_status DROP COLUMN hik_record_id")
    cur.execute("ALTER TABLE hik_record_status DROP COLUMN id")
    cur.execute("ALTER TABLE hik_records DROP COLUMN id")

    cur.execute("ALTER TABLE hik_records RENAME COLUMN new_id TO id")
    cur.execute("ALTER TABLE hik_record_status RENAME COLUMN new_id TO id")
    cur.execute("ALTER TABLE hik_record_status RENAME COLUMN new_hik_record_id TO hik_record_id")

    cur.execute("ALTER TABLE hik_records ALTER COLUMN id SET NOT NULL")
    cur.execute("ALTER TABLE hik_record_status ALTER COLUMN id SET NOT NULL")
    cur.execute("ALTER TABLE hik_record_status ALTER COLUMN hik_record_id SET NOT NULL")

    cur.execute("ALTER TABLE hik_records ADD PRIMARY KEY (id)")
    cur.execute("ALTER TABLE hik_record_status ADD PRIMARY KEY (id)")
    cur.execute("""
        ALTER TABLE hik_record_status
        ADD CONSTRAINT fk_hik_record FOREIGN KEY (hik_record_id)
        REFERENCES hik_records(id) ON DELETE CASCADE
    """)

    cur.execute("SELECT COUNT(*) FROM hik_records")
    after_records = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hik_record_status")
    after_status = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hik_record_status s LEFT JOIN hik_records r ON s.hik_record_id = r.id WHERE r.id IS NULL")
    orphans_after = cur.fetchone()[0]
    print(f"after:  hik_records={after_records}, hik_record_status={after_status}, orphan status rows={orphans_after}")

    if after_records != before_records or after_status != before_status or orphans_after != orphans_before:
        raise RuntimeError("Row count / FK mismatch after migration — rolling back")

    conn.commit()
    print("COMMITTED")
except Exception as e:
    conn.rollback()
    print(f"ROLLED BACK: {e}")
    raise
finally:
    cur.close()
    conn.close()
