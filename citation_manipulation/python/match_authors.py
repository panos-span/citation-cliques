import sqlite3
import pandas as pd
import sys


def greedy_match(rolap_db):
    # Connect directly to the analysis database
    con = sqlite3.connect(rolap_db)

    print(f"Connected to {rolap_db}...")
    print("Loading candidates...")

    # No 'rolap.' prefix needed since we are directly in rolap.db
    try:
        df = pd.read_sql_query(
            """
            SELECT case_orcid, control_orcid, subject, score 
            FROM author_matched_candidates 
            ORDER BY subject, score ASC, case_orcid, control_orcid
        """,
            con,
        )
    except pd.errors.DatabaseError as e:
        print(f"Error reading candidates: {e}")
        print("Did you run 'make prep_candidates'?")
        con.close()
        return

    final_pairs = []
    seen = set()

    print(f"Greedy matching on {len(df)} candidates...")
    # Iterate through candidates sorted by best match score (ascending)
    for row in df.itertuples():
        # Unique constraint is (ORCID, Subject)
        case_key = (row.case_orcid, row.subject)
        ctrl_key = (row.control_orcid, row.subject)

        if case_key not in seen and ctrl_key not in seen:
            final_pairs.append((row.case_orcid, row.control_orcid, row.subject))
            seen.add(case_key)
            seen.add(ctrl_key)

    print(f"Found {len(final_pairs)} pairs. Saving...")

    with con:
        con.execute("DROP TABLE IF EXISTS author_matched_pairs")
        con.execute(
            "CREATE TABLE author_matched_pairs (case_orcid TEXT, control_orcid TEXT, subject TEXT)"
        )
        con.executemany("INSERT INTO author_matched_pairs VALUES (?,?,?)", final_pairs)

        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_amp_case ON author_matched_pairs(case_orcid)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_amp_ctrl ON author_matched_pairs(control_orcid)"
        )

    con.close()
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default fallback if no arg provided
        greedy_match("rolap.db")
    else:
        # We only expect one argument now: rolap.db
        greedy_match(sys.argv[1])
