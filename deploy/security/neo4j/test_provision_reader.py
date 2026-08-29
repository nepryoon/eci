import unittest

from provision_reader import render


class ProvisionReaderTest(unittest.TestCase):
    def test_deterministic_least_privilege_grants(self):
        first = render("tenant-a", ["repo-b", "repo-a"], ["ops", "dev"], "neo4j")
        second = render("tenant-a", ["repo-a", "repo-b"], ["dev", "ops"], "neo4j")
        self.assertEqual(first, second)
        self.assertIn("GRANT MATCH { * }", first)
        self.assertIn("n.tenant_id = 'tenant-a'", first)
        self.assertIn("n.repo IN ['repo-a', 'repo-b']", first)
        self.assertNotIn("GRANT ALL", first)

    def test_rejects_injection_and_empty_scope(self):
        for args in [
            ("tenant-a' OR true", ["repo"], ["group"], "neo4j"),
            ("tenant", [], ["group"], "neo4j"),
            ("tenant", ["repo"], [], "neo4j"),
        ]:
            with self.assertRaises(ValueError):
                render(*args)


if __name__ == "__main__":
    unittest.main()
