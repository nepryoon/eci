# Neo4j Enterprise reader provisioning

`provision_reader.py` renders property-based `MATCH`/`TRAVERSE` grants for an
authenticated tenant/repository/ACL scope. Review the output and apply it with
an Enterprise-authorized administrative identity; never run the Retrieval
Engine as the writer/admin user.

```bash
python3 deploy/security/neo4j/provision_reader.py \
  --tenant tenant-a --repo repo-a --acl-group engineering
```

Neo4j Community does not implement this fine-grained RBAC. The mandatory
application Cypher predicates remain active in every edition, but Community is
dev-only and is not evidence that the native second layer was exercised.
