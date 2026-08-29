import importlib.util
import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[3]


class SecurityConfigurationTest(unittest.TestCase):
    def test_opensearch_dls_is_read_only_and_has_all_scope_dimensions(self):
        roles = (ROOT / "deploy/security/opensearch/roles.yml").read_text()
        self.assertIn("${attr.proxy.tenant_id}", roles)
        self.assertIn("${attr.proxy.allowed_repos}", roles)
        self.assertIn("${attr.proxy.acl_groups}", roles)
        self.assertNotIn("indices:data/write", roles)
        self.assertNotIn("fallback", roles.lower())

        match = re.search(r"dls: >-\n\s+(\{.*\})", roles)
        self.assertIsNotNone(match)
        # Substitution placeholders are JSON strings/list fragments at runtime.
        materialized = (
            match.group(1)
            .replace("${attr.proxy.tenant_id}", "tenant-a")
            .replace("${attr.proxy.allowed_repos}", '"repo-a","repo-b"')
            .replace("${attr.proxy.acl_groups}", '"dev","ops"')
        )
        parsed = json.loads(materialized)
        self.assertEqual(len(parsed["bool"]["filter"]), 3)

    def test_opensearch_disables_anonymous_and_uses_extended_proxy(self):
        config = (ROOT / "deploy/security/opensearch/config.yml").read_text()
        self.assertIn("anonymous_auth_enabled: false", config)
        self.assertIn("type: extended-proxy", config)
        self.assertIn("attr_header_prefix: x-proxy-ext-", config)

    def test_neo4j_grant_renderer_rejects_cypher_injection(self):
        path = ROOT / "deploy/security/neo4j/provision_reader.py"
        spec = importlib.util.spec_from_file_location("provision_reader", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        with self.assertRaises(ValueError):
            module.render("x' OR true", ["repo"], ["group"], "neo4j")


if __name__ == "__main__":
    unittest.main()
