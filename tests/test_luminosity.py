"""Exercise incomplete lumisections, duplicate streams and BRIL CSV integrity."""

import importlib.util
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "multilepton" / "luminosity.py"
SPEC = importlib.util.spec_from_file_location("luminosity_audit", SOURCE)
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


class LuminosityTest(unittest.TestCase):
    def test_explicitly_reused_era_is_counted_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            golden = root / "golden.json"
            golden.write_text("{}")
            first, second, output = (root / name for name in ("first", "second", "output"))
            for path in (first, second, output):
                path.mkdir()
            info = {"is_data": True, "keys": ["/test"], "lfns": [], "n_files": 0}
            manifest = {"golden": {"path": str(golden), "sha256": AUDIT.digest(golden)},
                        "datasets": {"data_e_d": info}, "entries": []}
            AUDIT.save(first / "manifest.json", manifest)
            manifest["reused_datasets"] = {
                "data_e_d": {"directory": str(first), "manifest_sha256": AUDIT.digest(first / "manifest.json")},
            }
            AUDIT.save(second / "manifest.json", manifest)
            result = AUDIT.snapshot([first, second], output)
            self.assertEqual(list(result["datasets"]), ["data_e_d"])
            self.assertEqual(len(result["manifests"]), 2)

    def test_shared_section_requires_every_file(self):
        files = {"ready": {(1, 1), (1, 2), (1, 3)}, "missing": {(1, 2), (1, 4)}}
        usable, partial = AUDIT.complete_sections(files, {"ready"}, {(1, 1), (1, 2), (1, 4)})
        self.assertEqual(usable, {(1, 1)})
        self.assertEqual(partial, {(1, 2)})

    def test_masks_deduplicate_and_preserve_gaps(self):
        pairs = [(1, 1), (1, 2), (1, 2), (1, 4), (2, 1)]
        self.assertEqual(AUDIT.compact(pairs), {"1": [[1, 2], [4, 4]], "2": [[1, 1]]})
        self.assertEqual(AUDIT.expand(AUDIT.compact(pairs)), set(pairs))

    def test_csv_rejects_double_counting_and_wrong_units(self):
        header = "#run:fill,ls,recorded(/pb)\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lumi.csv"
            path.write_text(header + "1:10,2:2,0.123456789\n")
            self.assertEqual(str(AUDIT.read_bril(path)[1, 2]), "0.123456789")
            path.write_text(header + "1:10,2:2,1.0\n1:10,2:2,1.0\n")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                AUDIT.read_bril(path)
            path.write_text(header.replace("/pb", "/fb") + "1:10,2:2,1.0\n")
            with self.assertRaisesRegex(ValueError, "units"):
                AUDIT.read_bril(path)

    def test_incomplete_or_negative_bril_values_are_not_exported_as_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            AUDIT.save(root / "coverage.json", {})
            AUDIT.save(root / "bril_input.json", {"1": [[1, 3]]})
            AUDIT.save(root / "masks" / "complete.json", {"1": [[1, 1]]})
            AUDIT.save(root / "masks" / "problematic.json", {"1": [[1, 3]]})
            normtag = root / "normtag.json"
            normtag.write_text("[]")
            csv_path = root / "bril.csv"
            csv_path.write_text("#run:fill,ls,recorded(/pb)\n1:10,1:1,0.1\n1:10,2:2,-0.001\n")
            args = SimpleNamespace(output=root, csv=csv_path, normtag=normtag, bril_input=None)
            with redirect_stdout(io.StringIO()):
                AUDIT.summarize(args)
            report = json.loads((root / "luminosity.json").read_text())["luminosity"]
            self.assertEqual(report["complete"]["recorded_pb"], 0.1)
            self.assertIsNone(report["problematic"]["recorded_pb"])
            self.assertEqual(report["problematic"]["missing_bril_lumisections"], 1)
            self.assertEqual(report["problematic"]["negative_bril_lumisections"], 1)
            self.assertEqual(report["problematic"]["covered_recorded_pb"], 0.099)
            self.assertEqual(report["problematic"]["usable_recorded_pb"], 0.1)
            self.assertEqual(json.loads(Path(report["problematic"]["usable_mask"]).read_text()), {"1": [[1, 1]]})


if __name__ == "__main__":
    unittest.main()
