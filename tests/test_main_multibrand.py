import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure repository root is importable under pytest's import mode.
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class TestMainMultiBrand(unittest.TestCase):
    def test_brands_file_runs_brand_by_brand_and_resets_seed_and_out_csv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            brands_file = Path(td) / "brands.txt"
            brands_file.write_text(
                """
                # comment
                Lattafa Perfumes
                
                Chanel
                chanel  # should NOT be treated as comment; kept as part of brand
                Lattafa Perfumes
                """.strip()
                + "\n",
                encoding="utf-8",
            )

            import main as main_mod

            calls = []

            def fake_crawl(ns):
                calls.append(ns)
                return 1

            argv = [
                "main.py",
                "--brands-file",
                str(brands_file),
                "--max-pages",
                "1",
                "--delay-seconds",
                "0.0",
                "--session-size",
                "35",
                "--session-break-seconds",
                "600",
            ]

            with (
                patch.object(main_mod, "crawl", side_effect=fake_crawl),
                patch("sys.argv", argv),
            ):
                main_mod.main()

            # Notes:
            # - blank lines and lines starting with # ignored
            # - dedupe is case-insensitive but preserves first occurrence
            # - inline # is not treated specially (only leading # starts a comment)
            self.assertEqual(len(calls), 3)
            self.assertEqual(calls[0].brand, "Lattafa Perfumes")
            self.assertEqual(calls[1].brand, "Chanel")
            self.assertEqual(calls[2].brand, "chanel  # should NOT be treated as comment; kept as part of brand")

            for ns in calls:
                self.assertIsNone(ns.start_url)
                self.assertEqual(ns.out_csv, "perfumes.csv")
                self.assertEqual(ns.max_pages, 1)

    def test_brands_repeatable_dedup_case_insensitive(self) -> None:
        import main as main_mod

        calls = []

        def fake_crawl(ns):
            calls.append(ns)
            return 1

        argv = [
            "main.py",
            "--brands",
            "Chanel",
            "--brands",
            "chanel",
            "--brands",
            "Dior",
            "--max-pages",
            "1",
        ]

        with (
            patch.object(main_mod, "crawl", side_effect=fake_crawl),
            patch("sys.argv", argv),
        ):
            main_mod.main()

        self.assertEqual([c.brand for c in calls], ["Chanel", "Dior"])

    def test_session_size_is_per_brand_and_main_does_not_sleep(self) -> None:
        import main as main_mod

        calls = []

        def fake_crawl(ns):
            calls.append(ns)
            if ns.brand == "Chanel":
                return 3
            if ns.brand == "Dior":
                return 2
            return 0

        argv = [
            "main.py",
            "--brands",
            "Chanel",
            "--brands",
            "Dior",
            "--max-pages",
            "100",
            "--session-size",
            "5",
            "--session-break-seconds",
            "600",
        ]

        with (
            patch.object(main_mod, "crawl", side_effect=fake_crawl),
            patch("fragrantica_scraper.network.session_sleep") as sleep,
            patch("sys.argv", argv),
        ):
            main_mod.main()

        # Each brand run should keep the original max-pages; session-size handling happens inside crawler.
        self.assertEqual(calls[0].brand, "Chanel")
        self.assertEqual(calls[0].max_pages, 100)

        self.assertEqual(calls[1].brand, "Dior")
        self.assertEqual(calls[1].max_pages, 100)

        # Main should not sleep in multi-brand mode; crawler handles session breaks.
        sleep.assert_not_called()

    def test_multi_brand_carries_saved_since_break_between_brands(self) -> None:
        import main as main_mod

        saved_since_break_seen = []

        def fake_crawl(ns):
            saved_since_break_seen.append(getattr(ns, "saved_since_break", None))
            # Simulate saving 3 new fragrances for each brand.
            ns.saved_since_break_end = int(getattr(ns, "saved_since_break", 0) or 0) + 3
            return 3

        argv = [
            "main.py",
            "--brands",
            "Chanel",
            "--brands",
            "Dior",
            "--max-pages",
            "100",
            "--session-size",
            "35",
            "--session-break-seconds",
            "900",
        ]

        with (
            patch.object(main_mod, "crawl", side_effect=fake_crawl),
            patch("sys.argv", argv),
        ):
            main_mod.main()

        # First brand starts with 0; second brand should receive the carried counter.
        self.assertEqual(saved_since_break_seen, [0, 3])


if __name__ == "__main__":
    unittest.main()
