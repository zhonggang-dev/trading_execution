import contextlib
import io
import sqlite3
import tempfile
import unittest
from pathlib import Path

import strategy_kalshi_account_migration as migration


def seed(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE market_selection (account_id TEXT, strategy_id TEXT, condition_id TEXT,
            PRIMARY KEY (account_id, strategy_id, condition_id));
        CREATE TABLE order_attempts (decision_id TEXT PRIMARY KEY, account_id TEXT, condition_id TEXT);
        CREATE TABLE decision_records (account_id TEXT, cycle_id TEXT);
        CREATE TABLE prediction_selections (account_id TEXT, condition_id TEXT, prediction_id TEXT,
            PRIMARY KEY (account_id, condition_id, prediction_id));
        CREATE TRIGGER ps_no_update BEFORE UPDATE ON prediction_selections BEGIN SELECT RAISE(ABORT, 'immutable'); END;
        CREATE TABLE decision_snapshots (decision_id TEXT PRIMARY KEY, account_id TEXT, condition_id TEXT);
        CREATE TRIGGER ds_no_update BEFORE UPDATE ON decision_snapshots BEGIN SELECT RAISE(ABORT, 'immutable'); END;
        INSERT INTO prediction_selections VALUES ('main','kalshi:KX-A','p1'),('main','0xpoly','p2');
        INSERT INTO decision_snapshots VALUES ('d1','main','kalshi:KX-A');
        INSERT INTO market_selection VALUES ('main','s','kalshi:KX-A'),('main','s','0xpoly'),('wallet-6','s','kalshi:KX-B');
        INSERT INTO order_attempts VALUES ('d1','main','kalshi:KX-A'),('d2','main','0xpoly');
        INSERT INTO decision_records VALUES ('main','main:T');
        """
    )
    conn.commit()
    conn.close()


class StrategyKalshiAccountMigrationTest(unittest.TestCase):
    def run_main(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = migration.main(list(argv))
        return code, out.getvalue()

    def test_dry_run_changes_nothing_and_apply_moves_only_kalshi_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            db, backup = Path(directory) / "s.sqlite3", Path(directory) / "backup.sqlite3"
            seed(db)
            code, out = self.run_main("--db", str(db))
            self.assertEqual(code, 0)
            self.assertIn("market_selection\t1", out)
            self.assertNotIn("decision_records", out)
            conn = sqlite3.connect(str(db))
            self.assertEqual(conn.execute("SELECT count(*) FROM order_attempts WHERE account_id='kalshi:main'").fetchone()[0], 0)
            conn.close()

            code, _ = self.run_main("--db", str(db), "--apply", "--backup", str(backup))
            self.assertEqual(code, 0)
            conn = sqlite3.connect(str(db))
            rows = sorted(conn.execute("SELECT account_id, condition_id FROM market_selection"))
            self.assertEqual(rows, [("kalshi:main", "kalshi:KX-A"), ("main", "0xpoly"), ("wallet-6", "kalshi:KX-B")])
            self.assertEqual(conn.execute("SELECT account_id FROM order_attempts WHERE decision_id='d1'").fetchone()[0], "kalshi:main")
            self.assertEqual(conn.execute("SELECT account_id FROM order_attempts WHERE decision_id='d2'").fetchone()[0], "main")
            # Append-only: the evaluated prediction is copied, the source history kept.
            self.assertEqual(sorted(conn.execute("SELECT account_id, condition_id FROM prediction_selections")),
                             [("kalshi:main", "kalshi:KX-A"), ("main", "0xpoly"), ("main", "kalshi:KX-A")])
            self.assertEqual(conn.execute("SELECT account_id FROM decision_snapshots").fetchone()[0], "main")
            conn.close()
            saved = sqlite3.connect(str(backup))
            self.assertEqual(saved.execute("SELECT account_id FROM order_attempts WHERE decision_id='d1'").fetchone()[0], "main")
            saved.close()

            # A second run refuses: the target account already has Kalshi rows.
            with self.assertRaises(SystemExit):
                self.run_main("--db", str(db), "--apply", "--backup", str(backup))

            # Rollback: after the cutover kalshi:main evaluated one more prediction.
            conn = sqlite3.connect(str(db))
            conn.execute("INSERT INTO prediction_selections VALUES ('kalshi:main','kalshi:KX-A','p9')")
            conn.commit()
            conn.close()
            code, _ = self.run_main("--db", str(db), "--source", "kalshi:main", "--target", "main",
                                    "--apply", "--backup", str(Path(directory) / "rollback.sqlite3"))
            self.assertEqual(code, 0)
            conn = sqlite3.connect(str(db))
            self.assertEqual(conn.execute("SELECT account_id FROM order_attempts WHERE decision_id='d1'").fetchone()[0], "main")
            self.assertEqual(sorted(conn.execute("SELECT prediction_id FROM prediction_selections WHERE account_id='main' AND condition_id LIKE 'kalshi:%'")),
                             [("p1",), ("p9",)])
            conn.close()


if __name__ == "__main__":
    unittest.main()
