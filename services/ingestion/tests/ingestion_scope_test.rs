use ingestion::IngestionScope;

#[test]
fn ingestion_scope_requires_non_blank_security_labels() {
    let valid = IngestionScope::new("tenant-a", "repo-a", "engineering")
        .expect("valid scope");
    assert_eq!(valid.tenant_id(), "tenant-a");
    assert_eq!(valid.repo(), "repo-a");
    assert_eq!(valid.acl_group(), "engineering");

    for (tenant, repo, group) in [
        ("", "repo-a", "engineering"),
        ("tenant-a", " ", "engineering"),
        ("tenant-a", "repo-a", "\t"),
        ("tenant-a\r\nforged", "repo-a", "engineering"),
    ] {
        assert!(IngestionScope::new(tenant, repo, group).is_err());
    }
}
