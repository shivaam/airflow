#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import pathlib

import pytest

from airflow.sdk.coordinators.typescript.coordinator import TypescriptCoordinator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


class TestTypescriptCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = TypescriptCoordinator(bundles_root="/airflow/ts-bundles")

        assert coordinator.node_executable == "node"
        assert coordinator.bundles_root == [pathlib.Path("/airflow/ts-bundles")]

    def test_custom_kwargs(self):
        coordinator = TypescriptCoordinator(
            node_executable="/opt/node/bin/node",
            bundles_root=["/airflow/ts-bundles"],
        )

        assert coordinator.node_executable == "/opt/node/bin/node"
        assert coordinator.bundles_root == [pathlib.Path("/airflow/ts-bundles")]


class TestTypescriptCoordinatorBundleSelection:
    def test_find_bundle_returns_single_mjs_file(self, tmp_path):
        bundle = tmp_path / "bundle.mjs"
        bundle.write_text("export {};\n")

        coordinator = TypescriptCoordinator(bundles_root=tmp_path)

        assert coordinator._find_bundle() == bundle

    def test_find_bundle_recurses_into_directories(self, tmp_path):
        nested = tmp_path / "nested"
        nested.mkdir()
        bundle = nested / "bundle.mjs"
        bundle.write_text("export {};\n")

        coordinator = TypescriptCoordinator(bundles_root=tmp_path)

        assert coordinator._find_bundle() == bundle

    def test_find_bundle_ignores_non_mjs_files(self, tmp_path):
        (tmp_path / "bundle.js").write_text("export {};\n")
        coordinator = TypescriptCoordinator(bundles_root=tmp_path)

        with pytest.raises(FileNotFoundError, match="No .mjs bundle found"):
            coordinator._find_bundle()

    def test_find_bundle_rejects_multiple_mjs_files(self, tmp_path):
        (tmp_path / "first.mjs").write_text("export {};\n")
        (tmp_path / "second.mjs").write_text("export {};\n")
        coordinator = TypescriptCoordinator(bundles_root=tmp_path)

        with pytest.raises(ValueError, match="Multiple .mjs bundles found"):
            coordinator._find_bundle()
