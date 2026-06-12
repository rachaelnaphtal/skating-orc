import os
import pickle
import tempfile

import pandas as pd

from element_deviation_ranking_job import (
    load_ranking_result,
    package_element_ranking_result,
    rehydrate_packaged_ranking_result,
)


def test_rehydrate_packaged_ranking_result_inlines_sidecars():
    params = {("Singles", 1, 3): 0.42}
    ctrl = pd.DataFrame({"element_id": [1], "control_score": [2.0]})
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        base = tmp.name
    try:
        packaged = package_element_ranking_result(
            {"marking": pd.DataFrame(), "params": params, "control_by_element": ctrl},
            base,
        )
        assert "params" not in packaged
        assert packaged.get("params_path")

        rehydrated = rehydrate_packaged_ranking_result(packaged)
        assert rehydrated["params"] == params
        assert len(rehydrated["control_by_element"]) == 1

        os.remove(packaged["params_path"])
        os.remove(packaged["control_by_element_path"])
        assert "params" not in rehydrate_packaged_ranking_result(packaged)
    finally:
        for path in (
            base,
            base + ".params.pkl",
            base + ".ctrl.pkl",
        ):
            try:
                os.remove(path)
            except OSError:
                pass


def test_load_ranking_result_rehydrates_before_sidecar_cleanup():
    params = {("Pairs", 2, 0): 0.5}
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        base = tmp.name
    try:
        packaged = package_element_ranking_result(
            {"marking": pd.DataFrame(), "params": params},
            base,
        )
        with open(base, "wb") as f:
            pickle.dump(packaged, f, protocol=pickle.HIGHEST_PROTOCOL)

        loaded = load_ranking_result(base)
        assert loaded["params"] == params

        os.remove(base + ".params.pkl")
        assert loaded.get("params") == params
    finally:
        for path in (base, base + ".params.pkl", base + ".ctrl.pkl"):
            try:
                os.remove(path)
            except OSError:
                pass
